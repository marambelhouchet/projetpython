import threading
import time

import analysis
import config


class State:
    def __init__(self, mode, workers, report_interval, llm_on):
        self.mode = mode
        self.workers = workers
        self.report_interval = report_interval
        self.llm_on = llm_on

        # Deux verrous : un pour les données, un pour le fichier de logs
        self.lock = threading.Lock()
        self.log_lock = threading.Lock()

        # Données partagées
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

        # Compteurs (métriques)
        self.m = {
            "received": 0,
            "processed": 0,
            "alerts": 0,
            "analysis_s": 0.0,
            "batches": 0,
        }

    def log(self, msg):
        heure = time.strftime("%Y-%m-%d %H:%M:%S")
        line = heure + " " + msg
        print(line, flush=True)

        # Un seul thread écrit dans le fichier à la fois
        self.log_lock.acquire()
        try:
            f = open(config.LOG_FILE, "a", encoding="utf-8")
            f.write(line + "\n")
            f.close()
        finally:
            self.log_lock.release()

    def stats(self):
        """Ajoute la moyenne à chaque type de capteur."""
        resultat = {}
        for type_capteur in self.total:
            s = self.total[type_capteur]
            resultat[type_capteur] = {
                "count": s["count"],
                "sum": s["sum"],
                "min": s["min"],
                "max": s["max"],
                "avg": s["sum"] / s["count"],
            }
        return resultat

    def metrics(self):
        m = self.m
        uptime = time.time() - self.t0

        debit = 0.0
        if uptime > 0:
            debit = m["processed"] / uptime

        traites = m["processed"]
        if traites == 0:
            traites = 1          # évite la division par zéro
        ms_par_mesure = 1000 * m["analysis_s"] / traites

        if self.llm_on:
            llm = analysis.model()
        else:
            llm = "désactivé"

        return {
            "mode": self.mode,
            "workers": self.workers,
            "llm": llm,
            "recus": m["received"],
            "traites": m["processed"],
            "alertes": m["alerts"],
            "uptime_s": round(uptime, 1),
            "debit_traite/s": round(debit, 1),
            "temps_analyse_s": round(m["analysis_s"], 2),
            "ms_par_mesure": round(ms_par_mesure, 3),
        }

    def answer(self, cmd, arg):
        """Répond à une commande de l'opérateur. Renvoie (ok, données)."""
        # n = nombre d'éléments demandés (10 par défaut)
        n = 10
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
                if self.last_report is None:
                    return True, "aucun rapport pour l'instant"
                return True, self.last_report

            if cmd == "ANALYSIS":
                if self.last_analysis is None:
                    return True, "aucune analyse IA pour l'instant"
                return True, self.last_analysis

            if cmd == "METRICS":
                return True, self.metrics()

        return False, "commande inconnue: " + cmd