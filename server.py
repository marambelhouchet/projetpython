"""SmartCity-Stream Hub."""

import argparse
import os
import queue
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


# ---------- 1. Calcul par lots ----------
def lire_un_lot(inq, stop):
    """Prend des mesures dans la file jusqu'à BATCH_SIZE ou jusqu'au délai BATCH_TIMEOUT."""
    batch = []
    limite = time.time() + config.BATCH_TIMEOUT

    while len(batch) < config.BATCH_SIZE and not stop.is_set():
        reste = limite - time.time()
        if reste <= 0:
            break
        try:
            batch.append(inq.get(timeout=min(0.2, reste)))
        except queue.Empty:
            pass
    return batch


def decouper(batch, nb_workers):
    """Répartit le lot en morceaux, un par worker."""
    chunks = []
    for i in range(nb_workers):
        morceau = batch[i::nb_workers]
        if morceau:
            chunks.append(morceau)
    return chunks


def fusionner(state, results, nb_mesures, duree):
    """Additionne les résultats des workers dans l'état partagé. Renvoie les nouvelles alertes."""
    new_alerts = []

    with state.lock:
        for stats_partielles, alertes_trouvees in results:
            calcul.merge_partial(state.total, stats_partielles)
            calcul.merge_partial(state.daily, stats_partielles)
            new_alerts.extend(alertes_trouvees)

        state.alerts.extend(new_alerts)
        del state.alerts[:-200]
        state.daily_alerts.extend(new_alerts)

        if state.llm_on:
            state.llm_pending.extend(new_alerts)
            del state.llm_pending[:-500]

        state.m["processed"] += nb_mesures
        state.m["alerts"] += len(new_alerts)
        state.m["analysis_s"] += duree
        state.m["batches"] += 1

    return new_alerts


def batch_loop(state, inq, notifyq, pool, fn, stop):
    while not stop.is_set():
        batch = lire_un_lot(inq, stop)
        if not batch:
            continue

        chunks = decouper(batch, state.workers)

        debut = time.perf_counter()
        results = pool.map(fn, chunks)          # chaque worker calcule un morceau
        duree = time.perf_counter() - debut

        new_alerts = fusionner(state, results, len(batch), duree)

        for alerte in new_alerts:
            state.log(str(alerte))
            notifyq.put(("alert", alerte))


# ---------- 2. Rapport périodique ----------
def ecrire_fichier_rapport(texte):
    os.makedirs(config.REPORT_DIR, exist_ok=True)
    nom = time.strftime("report_%Y%m%d_%H%M%S.txt")
    chemin = os.path.join(config.REPORT_DIR, nom)
    with open(chemin, "w", encoding="utf-8") as f:
        f.write(texte + "\n")
    return chemin


def make_report(state, notifyq, llmq, keep_empty):
    # On prend les données de la période et on remet des listes vides
    with state.lock:
        daily = state.daily
        alerts = state.daily_alerts
        start = state.report_start
        end = time.time()
        state.daily = {}
        state.daily_alerts = []
        state.report_start = end

    if not daily and not keep_empty:
        state.log("rapport ignoré : aucune mesure reçue sur la période (capteurs lancés ?)")
        return

    texte = calcul.format_report(daily, alerts, start, end, state.mode)
    chemin = ecrire_fichier_rapport(texte)

    with state.lock:
        state.last_report = texte
    state.log(f"rapport généré: {chemin}")
    notifyq.put(("report", texte))

    # Gemini seulement s'il y a des alertes
    if state.llm_on and alerts:
        try:
            llmq.put_nowait(("report", texte))
        except queue.Full:
            state.log("file LLM pleine : analyse ignorée")


def report_loop(state, notifyq, llmq, interval, keep_empty, stop):
    while not stop.wait(interval):
        make_report(state, notifyq, llmq, keep_empty)


# ---------- 3. Analyse par le LLM ----------
def llm_loop(state, llmq, notifyq, interval, stop):
    prochain_diagnostic = time.time() + interval

    while not stop.is_set():
        # 1) Y a-t-il un travail dans la file ?
        job = None
        try:
            job = llmq.get(timeout=0.5)
        except queue.Empty:
            pass

        # 2) Sinon, est-ce l'heure du diagnostic des alertes accumulées ?
        if job is None and time.time() >= prochain_diagnostic:
            prochain_diagnostic = time.time() + interval
            with state.lock:
                if len(state.llm_pending) >= config.LLM_MIN_ALERTS:
                    job = ("alerts", state.llm_pending)
                    state.llm_pending = []

        if job is None:
            continue

        # 3) Appel à Gemini
        kind, contenu = job
        try:
            if kind == "alerts":
                titre = f"Diagnostic de {len(contenu)} alertes"
                texte = analysis.diagnose_alerts(contenu)
            else:
                titre = "Résumé du rapport"
                texte = analysis.summarize_report(contenu)
        except analysis.LLMError as e:
            state.log(f"LLM indisponible : {e}")
            continue

        message = f"[{time.strftime('%H:%M:%S')}] {titre}\n{texte}"
        with state.lock:
            state.last_analysis = message
        state.log(f"analyse IA générée : {titre}")
        notifyq.put(("analysis", message))


# ---------- 4. Démarrage ----------
def lire_options():
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
    return a


def main():
    a = lire_options()

    # Le LLM est actif seulement si on ne l'a pas désactivé ET si la clé existe
    llm_on = False
    if not a.no_llm and analysis.available():
        llm_on = True

    llm_interval = config.LLM_ALERT_INTERVAL
    if a.llm_interval:
        llm_interval = a.llm_interval

    # Le Pool : threads OU processus (même façon de l'utiliser : pool.map)
    if a.mode == "threads":
        pool = ThreadPool(a.workers)
    else:
        pool = Pool(a.workers)

    # Objets partagés entre les threads
    stop = threading.Event()
    inq = queue.Queue()                  # mesures reçues
    notifyq = queue.Queue()              # messages à envoyer aux opérateurs
    llmq = queue.Queue(maxsize=3)        # travaux pour Gemini
    subs = {}                            # opérateurs abonnés
    subs_lock = threading.Lock()
    state = State(a.mode, a.workers, a.report_interval, llm_on)

    fn = partial(calcul.analyze_chunk, work=a.work)

    threads = [
        threading.Thread(target=udp_loop, args=(state, inq, stop)),
        threading.Thread(target=batch_loop, args=(state, inq, notifyq, pool, fn, stop)),
        threading.Thread(target=tcp_loop, args=(state, subs, subs_lock, stop)),
        threading.Thread(target=notifier_loop, args=(notifyq, subs, subs_lock, stop)),
        threading.Thread(target=report_loop,
                         args=(state, notifyq, llmq, a.report_interval, a.keep_empty, stop)),
    ]
    if llm_on:
        threads.append(threading.Thread(target=llm_loop,
                                        args=(state, llmq, notifyq, llm_interval, stop)))

    for t in threads:
        t.start()

    if llm_on:
        llm_label = analysis.model()
    else:
        llm_label = "désactivé (pas de GEMINI_API_KEY)"
    state.log(f"=== démarrage : mode={a.mode} workers={a.workers} "
              f"rapport toutes les {a.report_interval}s LLM={llm_label} ===")

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        state.log("arrêt du serveur...")
    finally:
        stop.set()                       # demande à tous les threads de s'arrêter
        for t in threads:
            t.join()                     # attend qu'ils aient fini
        make_report(state, notifyq, llmq, a.keep_empty)    # dernier rapport
        pool.close()
        pool.join()


if __name__ == "__main__":
    main()