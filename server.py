"""SmartCity-Stream Hub."""

import argparse
import os
import queue
import signal
import threading
import time
from functools import partial
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

import analysis
import calcul
import config
from network import notifier_loop, tcp_loop, udp_loop
from state import State


def ignore_sigint():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def batch_loop(state, inq, notifyq, pool, fn, stop):
    while not stop.is_set():
        batch = []
        deadline = time.time() + config.BATCH_TIMEOUT

        while len(batch) < config.BATCH_SIZE and not stop.is_set():
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                batch.append(inq.get(timeout=min(0.2, remaining)))
            except queue.Empty:
                pass

        if not batch:
            continue

        n = state.workers
        chunks = [batch[i::n] for i in range(n) if batch[i::n]]
        t = time.perf_counter()
        results = pool.map(fn, chunks)
        elapsed = time.perf_counter() - t

        new_alerts = []
        with state.lock:
            for partial_stats, found in results:
                calcul.merge_partial(state.total, partial_stats)
                calcul.merge_partial(state.daily, partial_stats)
                new_alerts.extend(found)

            state.alerts.extend(new_alerts)
            del state.alerts[:-200]
            state.daily_alerts.extend(new_alerts)

            if state.llm_on:
                state.llm_pending.extend(new_alerts)
                del state.llm_pending[:-500]

            state.m["processed"] += len(batch)
            state.m["alerts"] += len(new_alerts)
            state.m["analysis_s"] += elapsed
            state.m["batches"] += 1

        for a in new_alerts:
            state.log(str(a))
            notifyq.put(("alert", a))


def make_report(state, notifyq, llmq, keep_empty):
    with state.lock:
        daily = state.daily
        alerts = state.daily_alerts
        start = state.report_start
        end = time.time()
        state.daily = {}
        state.daily_alerts = []
        state.report_start = end

    if not daily and not keep_empty:
        state.log(
            "rapport ignoré : aucune mesure reçue sur la période "
            "(capteurs lancés ?)"
        )
        return

    text = calcul.format_report(daily, alerts, start, end, state.mode)
    os.makedirs(config.REPORT_DIR, exist_ok=True)
    path = os.path.join(
        config.REPORT_DIR,
        time.strftime("report_%Y%m%d_%H%M%S.txt"),
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(text + "\n")

    with state.lock:
        state.last_report = text
    state.log(f"rapport généré: {path}")
    notifyq.put(("report", text))

    if state.llm_on and alerts:
        try:
            llmq.put_nowait(("report", text))
        except queue.Full:
            state.log("file LLM pleine : analyse ignorée")


def report_loop(state, notifyq, llmq, interval, keep_empty, stop):
    while not stop.wait(interval):
        make_report(state, notifyq, llmq, keep_empty)


def llm_loop(state, llmq, notifyq, interval, stop):
    next_digest = time.time() + interval
    while not stop.is_set():
        try:
            job = llmq.get(timeout=0.5)
        except queue.Empty:
            job = None

        if job is None and time.time() >= next_digest:
            next_digest = time.time() + interval
            with state.lock:
                if len(state.llm_pending) >= config.LLM_MIN_ALERTS:
                    job = ("alerts", state.llm_pending)
                    state.llm_pending = []

        if not job:
            continue

        kind, payload = job
        try:
            if kind == "alerts":
                title = f"Diagnostic de {len(payload)} alertes"
                text = analysis.diagnose_alerts(payload)
            else:
                title = "Résumé du rapport"
                text = analysis.summarize_report(payload)
        except analysis.LLMError as e:
            state.log(f"LLM indisponible : {e}")
            continue

        msg = f"[{time.strftime('%H:%M:%S')}] {title}\n{text}"
        with state.lock:
            state.last_analysis = msg
        state.log(f"analyse IA générée : {title}")
        notifyq.put(("analysis", msg))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["threads", "processes"], default="threads")
    ap.add_argument("--workers", type=int, default=config.WORKERS or os.cpu_count())
    ap.add_argument("--work", type=int, default=config.WORK_FACTOR)
    ap.add_argument("--report-interval", type=float, default=config.REPORT_INTERVAL)
    ap.add_argument("--no-llm", action="store_true")
    ap.add_argument("--llm-interval", type=float, default=None)
    ap.add_argument("--keep-empty", action="store_true")
    a = ap.parse_args()

    if a.workers < 1:
        ap.error("--workers doit être supérieur ou égal à 1")
    if a.work < 0:
        ap.error("--work doit être positif ou nul")
    if a.report_interval <= 0:
        ap.error("--report-interval doit être supérieur à 0")

    llm_on = (not a.no_llm) and analysis.available()
    llm_interval = a.llm_interval or config.LLM_ALERT_INTERVAL

    pool = (
        ThreadPool(a.workers)
        if a.mode == "threads"
        else Pool(a.workers, initializer=ignore_sigint)
    )

    stop = threading.Event()
    inq = queue.Queue()
    notifyq = queue.Queue()
    llmq = queue.Queue(maxsize=3)
    subs = {}
    subs_lock = threading.Lock()
    state = State(a.mode, a.workers, a.report_interval, llm_on)
    fn = partial(calcul.analyze_chunk, work=a.work)

    threads = [
        threading.Thread(target=udp_loop, args=(state, inq, stop)),
        threading.Thread(target=batch_loop, args=(state, inq, notifyq, pool, fn, stop)),
        threading.Thread(target=tcp_loop, args=(state, subs, subs_lock, stop)),
        threading.Thread(target=notifier_loop, args=(notifyq, subs, subs_lock, stop)),
        threading.Thread(
            target=report_loop,
            args=(state, notifyq, llmq, a.report_interval, a.keep_empty, stop),
        ),
    ]
    if llm_on:
        threads.append(
            threading.Thread(
                target=llm_loop, args=(state, llmq, notifyq, llm_interval, stop)
            )
        )

    for t in threads:
        t.start()

    llm_label = (
        analysis.model() if llm_on else "désactivé (pas de GEMINI_API_KEY)"
    )
    state.log(
        f"=== démarrage : mode={a.mode} workers={a.workers} "
        f"rapport toutes les {a.report_interval}s LLM={llm_label} ==="
    )

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        state.log("arrêt du serveur...")
    finally:
        stop.set()
        for t in threads:
            t.join()
        make_report(state, notifyq, llmq, a.keep_empty)
        pool.close()
        pool.join()


if __name__ == "__main__":
    main()