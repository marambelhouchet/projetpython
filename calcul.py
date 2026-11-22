"""Fonctions d'analyse. Elles sont au premier niveau du module pour être picklables
(obligatoire pour les envoyer à des processus)."""
import math
import time
from collections import Counter

import config
import models


def heavy_compute(x, work):
    """Simule un calcul coûteux par mesure (score d'anomalie, FFT, modèle ML...).
    Pur CPU : c'est ce type de charge que le GIL empêche de paralléliser avec des threads."""
    acc = float(x)
    for i in range(work):
        acc = math.sin(acc) * math.cos(acc + i) + math.sqrt(abs(acc) + 1.0)
    return acc


def niveau_alerte(valeur, seuil):
    return "CRITIQUE" if valeur > seuil * config.CRITICAL_RATIO else "ATTENTION"


def analyze_chunk(chunk, work=0):
    """Analyse un morceau de mesures -> (stats partielles par type, alertes détectées)."""
    partial, found, sink = {}, [], 0.0
    for r in chunk:
        if work:
            sink += heavy_compute(r.valeur, work)
        s = partial.get(r.type)
        if s is None:
            s = partial[r.type] = {"count": 0, "sum": 0.0, "min": r.valeur, "max": r.valeur}
        s["count"] += 1
        s["sum"] += r.valeur
        s["min"] = min(s["min"], r.valeur)
        s["max"] = max(s["max"], r.valeur)
        seuil = config.THRESHOLDS.get(r.type)
        if seuil is not None and r.valeur > seuil:
            found.append(models.AlerteUrbaine(
                sensor_id=r.sensor_id, type=r.type, valeur=r.valeur, seuil=seuil,
                niveau=niveau_alerte(r.valeur, seuil), timestamp=r.timestamp))
    return partial, found


def io_chunk(chunk, delay=0.02):
    """Tâche liée aux E/S (écriture en base, appel réseau...) : simulée par un sleep."""
    time.sleep(delay)
    return len(chunk)


def merge_partial(target, partial):
    """Fusionne des stats partielles dans un dictionnaire cumulé."""
    for t, p in partial.items():
        s = target.get(t)
        if s is None:
            target[t] = dict(p)
        else:
            s["count"] += p["count"]
            s["sum"] += p["sum"]
            s["min"] = min(s["min"], p["min"])
            s["max"] = max(s["max"], p["max"])


def format_report(daily, alerts, start, end, mode):
    fmt = "%Y-%m-%d %H:%M:%S"
    out = ["=" * 66,
           "RAPPORT SMARTCITY-STREAM",
           f"Période : {time.strftime(fmt, time.localtime(start))}  ->  {time.strftime(fmt, time.localtime(end))}",
           f"Mode d'analyse : {mode}",
           "=" * 66]
    if not daily:
        out.append("Aucune mesure reçue sur la période.")
    for t, s in sorted(daily.items()):
        out.append(f"{t:12} n={s['count']:7}  moy={s['sum'] / s['count']:9.2f}  "
                   f"min={s['min']:9.2f}  max={s['max']:9.2f}")
    crit = sum(1 for a in alerts if a.niveau == "CRITIQUE")
    out.append("-" * 66)
    out.append(f"Alertes : {len(alerts)} (dont CRITIQUE : {crit})")
    for t, n in Counter(a.type for a in alerts).most_common():
        out.append(f"  - {t:12} {n}")
    top = Counter(a.sensor_id for a in alerts).most_common(5)
    if top:
        out.append("Capteurs les plus en alerte : " + ", ".join(f"{s} ({n})" for s, n in top))
    return "\n".join(out)