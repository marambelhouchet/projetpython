
import argparse
import os
import queue
import socket
import threading
import time
from functools import partial
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

import analysis
import config
import models
from net_utils import recv_msg, safe_loads, send_msg


class Hub:
    def __init__(self, mode, workers, work, report_interval):
        self.mode, self.workers, self.report_interval = mode, workers, report_interval
        self.func = partial(analysis.analyze_chunk, work=work)

 
        self.pool = ThreadPool(workers) if mode == "threads" else Pool(workers)

        self.inq = queue.Queue()       # UDP -> aggregator (producteur / consommateur)
        self.notifyq = queue.Queue()   # résultats -> notifier (alertes / rapports à pousser)
        self.stop = threading.Event()

        self.state_lock = threading.Lock()    # protège stats / alertes / métriques
        self.log_lock = threading.Lock()      # protège le fichier de logs
        self.subs_lock = threading.Lock()     # protège la liste des abonnés
        self.sem = threading.Semaphore(config.MAX_OPERATORS)

        self.total, self.daily = {}, {}
        self.alerts, self.daily_alerts, self.last = [], [], []
        self.last_report = None
        self.report_start = time.time()
        self.subs = {}                        # conn -> verrou d'envoi de cette connexion
        self.m = {"received": 0, "processed": 0, "alerts": 0, "analysis_s": 0.0, "batches": 0}
        self.t0 = time.time()

    # ---------- utilitaires ----------
    def log(self, msg):
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
        print(line, flush=True)
        with self.log_lock:                 # un seul thread écrit à la fois
            with open(config.LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    # ---------- Thread UDP : producteur ----------
    def udp_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        sock.bind((config.HOST, config.UDP_PORT))
        sock.settimeout(1.0)
        self.log(f"UDP en écoute sur {config.HOST}:{config.UDP_PORT}")
        while not self.stop.is_set():
            try:
                data, _ = sock.recvfrom(4096)
            except socket.timeout:
                continue
            try:
                r = safe_loads(data)
                if not isinstance(r, models.RapportCapteur):
                    raise ValueError("pas un RapportCapteur")
            except Exception as e:
                self.log(f"paquet UDP rejeté: {e}")
                continue
            self.inq.put(r)
            with self.state_lock:
                self.m["received"] += 1
                self.last.append(r)
                del self.last[:-100]
        sock.close()

    # ---------- Thread consommateur : lots -> Pool ----------
    def aggregator(self):
        while not self.stop.is_set():
            batch, deadline = [], time.time() + config.BATCH_TIMEOUT
            while len(batch) < config.BATCH_SIZE:
                remaining = deadline - time.time()
                if remaining <= 0 or self.stop.is_set():
                    break
                try:
                    batch.append(self.inq.get(timeout=min(0.2, remaining)))
                except queue.Empty:
                    pass
            if not batch:
                continue
            n = self.workers
            chunks = [batch[i::n] for i in range(n) if batch[i::n]]
            t = time.perf_counter()
            results = self.pool.map(self.func, chunks)      # <- le calcul parallèle
            self.merge(results, len(batch), time.perf_counter() - t)

    def merge(self, results, n_reports, elapsed):
        new_alerts = []
        with self.state_lock:
            for partial_stats, found in results:
                analysis.merge_partial(self.total, partial_stats)
                analysis.merge_partial(self.daily, partial_stats)
                new_alerts.extend(found)
            self.alerts.extend(new_alerts)
            del self.alerts[:-200]
            self.daily_alerts.extend(new_alerts)
            self.m["processed"] += n_reports
            self.m["alerts"] += len(new_alerts)
            self.m["analysis_s"] += elapsed
            self.m["batches"] += 1
        for a in new_alerts:
            self.log(str(a))
            self.notifyq.put(("alert", a))

    # ---------- Thread notifier : pousse alertes et rapports aux abonnés ----------
    def notifier(self):
        while not self.stop.is_set():
            try:
                kind, data = self.notifyq.get(timeout=0.5)
            except queue.Empty:
                continue
            with self.subs_lock:
                targets = list(self.subs.items())
            for conn, lock in targets:
                try:
                    with lock:
                        send_msg(conn, models.Reponse(True, data, kind))
                except OSError:
                    with self.subs_lock:
                        self.subs.pop(conn, None)

    # ---------- Thread rapports périodiques ----------
    def report_loop(self):
        while not self.stop.wait(self.report_interval):
            self.make_report()

    def make_report(self):
        with self.state_lock:
            daily, alerts = self.daily, self.daily_alerts
            start, end = self.report_start, time.time()
            self.daily, self.daily_alerts, self.report_start = {}, [], end
        text = analysis.format_report(daily, alerts, start, end, self.mode)
        os.makedirs(config.REPORT_DIR, exist_ok=True)
        path = os.path.join(config.REPORT_DIR, time.strftime("report_%Y%m%d_%H%M%S.txt"))
        with open(path, "w", encoding="utf-8") as f:
            f.write(text + "\n")
        with self.state_lock:
            self.last_report = text
        self.log(f"rapport généré: {path}")
        self.notifyq.put(("report", text))

    # ---------- TCP : opérateurs ----------
    def answer(self, cmd, arg):
        """Construit la réponse à une commande (sous state_lock). -> (ok, data)"""
        with self.state_lock:
            if cmd == "PING":
                return True, "pong"
            if cmd == "STATS":
                return True, {t: {**s, "avg": s["sum"] / s["count"]} for t, s in self.total.items()}
            if cmd == "ALERTS":
                return True, self.alerts[-arg:]
            if cmd == "LAST":
                return True, self.last[-arg:]
            if cmd == "REPORT":
                return True, self.last_report or "aucun rapport pour l'instant"
            if cmd == "METRICS":
                up, m = time.time() - self.t0, self.m
                return True, {
                    "mode": self.mode, "workers": self.workers,
                    "recus": m["received"], "traites": m["processed"], "alertes": m["alerts"],
                    "uptime_s": round(up, 1),
                    "debit_traite/s": round(m["processed"] / up, 1),
                    "temps_analyse_s": round(m["analysis_s"], 2),
                    "ms_par_mesure": round(1000 * m["analysis_s"] / max(m["processed"], 1), 3),
                }
        return False, f"commande inconnue: {cmd}"

    def handle_operator(self, conn, addr):
        lock = threading.Lock()   # sérialise les envois sur cette connexion (réponses + push)
        self.log(f"opérateur connecté {addr}")
        try:
            while not self.stop.is_set():
                req = recv_msg(conn)
                if not isinstance(req, models.Requete):
                    with lock:
                        send_msg(conn, models.Reponse(False, "requête invalide"))
                    continue
                cmd = req.commande.upper()
                if cmd == "QUIT":
                    with lock:
                        send_msg(conn, models.Reponse(True, "bye"))
                    break
                if cmd == "SUBSCRIBE":
                    with self.subs_lock:
                        self.subs[conn] = lock
                    ok, data = True, "abonné aux alertes et aux rapports"
                else:
                    ok, data = self.answer(cmd, req.arg)
                self.log(f"{addr} -> {cmd}")
                with lock:
                    send_msg(conn, models.Reponse(ok, data))
        except (ConnectionError, OSError):
            pass
        except Exception as e:
            self.log(f"erreur opérateur {addr}: {e}")
        finally:
            with self.subs_lock:
                self.subs.pop(conn, None)
            conn.close()
            self.sem.release()
            self.log(f"opérateur déconnecté {addr}")

    def tcp_listener(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((config.HOST, config.TCP_PORT))
        srv.listen()
        srv.settimeout(1.0)
        self.log(f"TCP en écoute sur {config.HOST}:{config.TCP_PORT} (max {config.MAX_OPERATORS} opérateurs)")
        while not self.stop.is_set():
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            if not self.sem.acquire(blocking=False):
                try:
                    send_msg(conn, models.Reponse(False, "SERVER_FULL"))
                finally:
                    conn.close()
                self.log(f"{addr} refusé (serveur plein)")
                continue
            threading.Thread(target=self.handle_operator, args=(conn, addr), daemon=True).start()
        srv.close()

    # ---------- cycle de vie ----------
    def run(self):
        self.log(f"=== démarrage : mode={self.mode} workers={self.workers} "
                 f"rapport toutes les {self.report_interval}s ===")
        targets = [self.udp_listener, self.aggregator, self.tcp_listener,
                   self.notifier, self.report_loop]
        threads = [threading.Thread(target=t) for t in targets]
        for t in threads:
            t.start()
        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.log("arrêt du serveur...")
        self.stop.set()
        for t in threads:
            t.join()
        self.make_report()          # dernier rapport avant de quitter
        self.pool.close()
        self.pool.join()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["threads", "processes"], default="threads")
    ap.add_argument("--workers", type=int, default=config.WORKERS or os.cpu_count())
    ap.add_argument("--work", type=int, default=config.WORK_FACTOR,
                    help="charge CPU simulée par mesure (0 = aucune)")
    ap.add_argument("--report-interval", type=float, default=config.REPORT_INTERVAL)
    a = ap.parse_args()
    Hub(a.mode, a.workers, a.work, a.report_interval).run()


if __name__ == "__main__":
    main()