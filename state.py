import threading
import time

import analysis
import calcul
import config


class State:
    def __init__(self, mode, workers, report_interval, llm_on):
        self.mode = mode
        self.workers = workers
        self.report_interval = report_interval
        self.llm_on = llm_on

        self.lock = threading.Lock()
        self.log_lock = threading.Lock()

        self.total = {}
        self.daily = {}
        self.alerts = []
        self.daily_alerts = []
        self.llm_pending = []
        self.last = []

        self.last_report = None
        self.last_analysis = None
        self.report_start = time.time()
        self.t0 = time.time()

        self.m = {
            "received": 0,
            "processed": 0,
            "alerts": 0,
            "analysis_s": 0.0,
            "batches": 0,
        }

    def log(self, msg):
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
        print(line, flush=True)
        with self.log_lock, open(config.LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def stats(self):
        return {
            t: {**s, "avg": s["sum"] / s["count"]}
            for t, s in self.total.items()
        }

    def answer(self, cmd, arg):
        n = arg if isinstance(arg, int) and arg > 0 else 10
        with self.lock:
            if cmd == "PING":
                return True, "pong"
            if cmd == "STATS":
                return True, self.stats()
            if cmd == "ALERTS":
                return True, self.alerts[-n:]
            if cmd == "LAST":
                return True, self.last[-n:]
            if cmd == "REPORT": 
                return True, self.last_report or "aucun rapport pour l'instant"
            if cmd == "ANALYSIS":
                return True, self.last_analysis or "aucune analyse IA pour l'instant"
            if cmd == "METRICS":
                up = time.time() - self.t0
                m = self.m
                return True, {
                    "mode": self.mode,
                    "workers": self.workers,
                    "llm": analysis.model() if self.llm_on else "désactivé",
                    "recus": m["received"],
                    "traites": m["processed"],
                    "alertes": m["alerts"],
                    "uptime_s": round(up, 1),
                    "debit_traite/s": round(m["processed"] / up, 1) if up > 0 else 0.0,
                    "temps_analyse_s": round(m["analysis_s"], 2),
                    "ms_par_mesure": round(
                        1000 * m["analysis_s"] / max(m["processed"], 1), 3
                    ),
                }
        return False, f"commande inconnue: {cmd}"