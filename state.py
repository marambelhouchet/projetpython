import threading
import time

import config


class State:
    def __init__(self, mode, workers, llm_on):
        self.mode = mode
        self.workers = workers
        self.llm_on = llm_on
        self.stop = False                # variable partagée d'arrêt

        # Deux verrous : un pour les données, un pour le fichier de logs
        self.lock = threading.Lock()
        self.log_lock = threading.Lock()

        # Données partagées
        self.total = {}
        self.daily = {}
        self.alerts = []
        self.daily_alerts = []
        self.llm_pending = []            # alertes à diagnostiquer
        self.report_pending = None       # rapport à résumer
        self.last = []

        self.last_report = None
        self.last_analysis = None
        self.report_start = time.time()
        self.t0 = time.time()

        # Compteurs (métriques)
        self.m = {"received": 0, "processed": 0, "alerts": 0, "analysis_s": 0.0}

    def log(self, msg):
        line = f"[+{time.time() - self.t0:.1f}s] {msg}"
        print(line, flush=True)
        # Un seul thread écrit dans le fichier à la fois
        with self.log_lock:
            with open(config.LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def stats(self):
        """Ajoute la moyenne à chaque type de capteur."""
        resultat = {}
        for type_capteur, s in self.total.items():
            resultat[type_capteur] = dict(s)
            resultat[type_capteur]["avg"] = s["sum"] / s["count"]
        return resultat

    def metrics(self):
        m = self.m
        uptime = time.time() - self.t0
        traites = max(m["processed"], 1)     # évite la division par zéro
        return {
            "mode": self.mode,
            "workers": self.workers,
            "llm": config.GEMINI_MODEL if self.llm_on else "désactivé",
            "recus": m["received"],
            "traites": m["processed"],
            "alertes": m["alerts"],
            "uptime_s": round(uptime, 1),
            "debit_traite/s": round(m["processed"] / max(uptime, 0.001), 1),
            "temps_analyse_s": round(m["analysis_s"], 2),
            "ms_par_mesure": round(1000 * m["analysis_s"] / traites, 3),
        }

    def answer(self, cmd, arg):
        """Répond à une commande de l'opérateur. Renvoie (ok, données)."""
        n = 10                               # nombre d'éléments demandés
        if isinstance(arg, int) and arg > 0:
            n = arg

        # Le verrou protège toutes les données lues ci-dessous
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
                return True, self.metrics()

        return False, "commande inconnue: " + cmd