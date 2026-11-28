"""Serveur SmartCity-Stream. Lancer : python server.py  (Entrée pour arrêter)."""
import multiprocessing as mp
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import analysis
import calcul
import config
from network import notifier_loop, tcp_loop, udp_loop
from state import State


# ---------- 1. Traitement par lots ----------
def lire_un_lot(inq):
    """Prend jusqu'à BATCH_SIZE mesures. Renvoie (lot, fin) ; None dans la file = signal de fin."""
    lot = []
    attendu = False
    mesure = inq.get()                       # bloque jusqu'à la première mesure
    while mesure is not None:
        lot.append(mesure)
        if len(lot) == config.BATCH_SIZE:
            return lot, False
        if inq.empty() and not attendu:
            time.sleep(config.BATCH_TIMEOUT)  # on laisse le lot se remplir, une seule fois
            attendu = True
        if inq.empty():
            return lot, False
        mesure = inq.get()
    return lot, True


def decouper(lot, nb_workers):
    """Coupe le lot en morceaux, un par worker."""
    chunks = []
    for i in range(nb_workers):
        morceau = lot[i::nb_workers]
        if morceau:
            chunks.append(morceau)
    return chunks


def fusionner(state, results, nb_mesures, duree):
    """Ajoute les résultats des workers dans l'état partagé. Renvoie les nouvelles alertes."""
    new_alerts = []
    with state.lock:
        for stats, alertes in results:
            calcul.merge_partial(state.total, stats)
            calcul.merge_partial(state.daily, stats)
            new_alerts.extend(alertes)

        state.alerts.extend(new_alerts)
        del state.alerts[:-200]              # on garde les 200 dernières
        state.daily_alerts.extend(new_alerts)
        if state.llm_on:
            state.llm_pending.extend(new_alerts)

        state.m["processed"] += nb_mesures
        state.m["alerts"] += len(new_alerts)
        state.m["analysis_s"] += duree
    return new_alerts


def batch_loop(state, inq, notifyq, pool):
    fin = False
    while not fin:
        lot, fin = lire_un_lot(inq)
        if not lot:
            continue

        arguments = [(chunk, config.WORK_FACTOR) for chunk in decouper(lot, state.workers)]

        debut = time.time()                  # chaque worker calcule un morceau
        if state.mode == "processes":
            results = pool.starmap(calcul.analyze_chunk, arguments)
        else:
            results = list(pool.map(calcul.analyze_tuple, arguments))
        duree = time.time() - debut

        for alerte in fusionner(state, results, len(lot), duree):
            state.log(str(alerte))
            notifyq.put(("alert", alerte))


# ---------- 2. Rapport périodique ----------
def make_report(state, notifyq):
    # On récupère les données de la période, puis on remet tout à zéro
    with state.lock:
        daily = state.daily
        alerts = state.daily_alerts
        duree = time.time() - state.report_start
        state.daily = {}
        state.daily_alerts = []
        state.report_start = time.time()

    if not daily:
        state.log("rapport ignoré : aucune mesure reçue")
        return

    texte = calcul.format_report(daily, alerts, duree, state.mode)
    with open(config.REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(texte + "\n\n")

    with state.lock:
        state.last_report = texte
        # On demande un résumé à Gemini seulement s'il y a des alertes
        if state.llm_on and alerts:
            state.report_pending = texte
    state.log(f"rapport ajouté au fichier {config.REPORT_FILE}")
    notifyq.put(("report", texte))


def report_loop(state, notifyq):
    while not state.stop:
        time.sleep(config.REPORT_INTERVAL)
        if not state.stop:
            make_report(state, notifyq)


# ---------- 3. Analyse par le LLM (Gemini) ----------
def llm_loop(state, notifyq):
    prochain_diagnostic = time.time() + config.LLM_ALERT_INTERVAL

    while not state.stop:
        time.sleep(0.5)
        rapport = None
        alertes = None

        # 1) Un rapport à résumer ? Sinon, est-ce l'heure de diagnostiquer les alertes ?
        with state.lock:
            if state.report_pending is not None:
                rapport = state.report_pending
                state.report_pending = None
            elif time.time() >= prochain_diagnostic:
                prochain_diagnostic = time.time() + config.LLM_ALERT_INTERVAL
                if len(state.llm_pending) >= config.LLM_MIN_ALERTS:
                    alertes = state.llm_pending
                    state.llm_pending = []

        if rapport is None and alertes is None:
            continue

        # 2) Appel à Gemini (hors du verrou : il peut être long)
        try:
            if alertes is not None:
                titre = f"Diagnostic de {len(alertes)} alertes"
                texte = analysis.diagnose_alerts(alertes)
            else:
                titre = "Résumé du rapport"
                texte = analysis.summarize_report(rapport)
        except analysis.LLMError as e:
            state.log(f"LLM indisponible : {e}")
            continue

        message = f"{titre}\n{texte}"
        with state.lock:
            state.last_analysis = message
        state.log(f"analyse IA générée : {titre}")
        notifyq.put(("analysis", message))


# ---------- 4. Démarrage ----------
def main():
    mode = input("Mode (threads / processes) [Entrée = " + config.MODE + "] : ").strip() or config.MODE
    if mode not in ("threads", "processes"):
        print("Mode inconnu")
        return
    workers = config.WORKERS or mp.cpu_count()

    # Pool de processus OU pool de threads
    if mode == "processes":
        creer_pool = mp.Pool
    else:
        creer_pool = ThreadPoolExecutor

    with creer_pool(workers) as pool:
        # Objets partagés entre les threads
        state = State(mode, workers, analysis.available())
        inq = queue.Queue()                  # mesures reçues des capteurs
        notifyq = queue.Queue()              # messages à envoyer aux opérateurs
        subs = {}                            # opérateurs abonnés
        subs_lock = threading.Lock()

        batch = threading.Thread(target=batch_loop, args=(state, inq, notifyq, pool), daemon=True)
        threads = [
            batch,
            threading.Thread(target=udp_loop, args=(state, inq), daemon=True),
            threading.Thread(target=tcp_loop, args=(state, subs, subs_lock), daemon=True),
            threading.Thread(target=notifier_loop, args=(notifyq, subs, subs_lock), daemon=True),
            threading.Thread(target=report_loop, args=(state, notifyq), daemon=True),
        ]
        if state.llm_on:
            threads.append(threading.Thread(target=llm_loop, args=(state, notifyq), daemon=True))

        for t in threads:
            t.start()

        llm_label = config.GEMINI_MODEL if state.llm_on else "désactivé"
        state.log(f"=== démarrage : mode={mode} workers={workers} "
                  f"rapport toutes les {config.REPORT_INTERVAL}s LLM={llm_label} ===")

        input("Appuyez sur Entrée pour arrêter le serveur.\n")
        state.log("arrêt du serveur...")
        state.stop = True                    # dit aux threads de s'arrêter
        inq.put(None)                        # signal de fin pour le traitement par lots
        batch.join()                         # attend le dernier lot
        make_report(state, notifyq)          # dernier rapport


if __name__ == "__main__":
    main()