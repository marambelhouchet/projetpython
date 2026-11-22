"""Calcul rapide (sans LLM) : statistiques, détection des seuils, rapports.
Fonctions au premier niveau du module pour être picklables (obligatoire pour les envoyer à des processus)."""
import math
import re
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


_SENSOR_ID = re.compile(r"[A-Za-z0-9_-]{1,16}")


def valid_report(r):
    """Rejette les paquets UDP mal formés ou hostiles. Indispensable : les identifiants de capteurs
    finissent dans les rapports et dans les prompts du LLM, ils ne doivent contenir aucun texte libre."""
    return (isinstance(r, models.RapportCapteur)
            and isinstance(r.type, str) and r.type in config.THRESHOLDS
            and isinstance(r.sensor_id, str) and _SENSOR_ID.fullmatch(r.sensor_id) is not None
            and isinstance(r.valeur, (int, float)) and not isinstance(r.valeur, bool)
            and math.isfinite(r.valeur)
            and isinstance(r.timestamp, (int, float)) and not isinstance(r.timestamp, bool))


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


# ---------- rapport lisible ----------
ICONES = {"temperature": "🌡️", "co2": "🌫️", "pm25": "😷", "noise": "🔊",
          "traffic": "🚗", "water_level": "💧"}
NOMS = {"temperature": "Température", "co2": "CO₂", "pm25": "Particules PM2.5",
        "noise": "Bruit", "traffic": "Trafic", "water_level": "Niveau d'eau"}


def _pluriel(n, mot):
    return f"{n} {mot}{'s' if n > 1 else ''}"


def format_report(daily, alerts, start, end, mode):
    """Rapport lisible. daily = {type: {count, sum, min, max}}, alerts = liste d'AlerteUrbaine."""
    hm = lambda ts: time.strftime("%Hh%M", time.localtime(ts))
    jour = time.strftime("%d/%m/%Y", time.localtime(start))
    minutes = max(1, round((end - start) / 60))
    out = ["📊 Rapport SmartCity-Stream",
           f"🕐 {jour}, de {hm(start)} à {hm(end)} ({minutes} min) · analyse en mode {mode}", ""]

    total = sum(s["count"] for s in daily.values())
    if total == 0:
        out.append("😴 Rien à signaler : aucune mesure reçue sur cette période.")
        out.append("   Vérifie que les capteurs (sensor_simulator.py) sont bien lancés.")
        return "\n".join(out)

    crit = [a for a in alerts if a.niveau == "CRITIQUE"]
    if not alerts:
        out.append(f"✅ Tout est calme : {_pluriel(total, 'mesure')} reçue{'s' if total > 1 else ''}, aucune alerte.")
    else:
        out.append(f"⚠️ {_pluriel(total, 'mesure')} reçue{'s' if total > 1 else ''}, "
                   f"{_pluriel(len(alerts), 'alerte')} dont {_pluriel(len(crit), 'critique')}.")
    out.append("")

    for t, s in sorted(daily.items()):
        out.append(f"{ICONES.get(t, '•')} {NOMS.get(t, t):<17} {s['count']:>6} mesures · "
                   f"moyenne {s['sum'] / s['count']:.1f} · de {s['min']:.1f} à {s['max']:.1f}")

    if alerts:
        out += ["", "🔴 Détail des alertes :"]
        for t, n in Counter(a.type for a in alerts).most_common():
            groupe = [a for a in alerts if a.type == t]
            pic = max(a.valeur for a in groupe)
            out.append(f"   • {NOMS.get(t, t)} : {_pluriel(n, 'alerte')} "
                       f"(pic {pic:.1f}, seuil {groupe[0].seuil})")
        top = Counter(a.sensor_id for a in alerts).most_common(3)
        out.append("   Capteurs les plus en alerte : " + ", ".join(f"{s} ({n})" for s, n in top))
    return "\n".join(out)