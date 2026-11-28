"""Calcul rapide (sans LLM) : statistiques, détection des seuils, rapports.
Fonctions au premier niveau du module pour être sérialisables (envoi aux processus)."""
import config
import models


def heavy_compute(x, work):
    """Simule un calcul coûteux par mesure."""
    acc = x
    for i in range(work):
        acc = (acc * acc + i) ** 0.5
    return acc


def niveau_alerte(valeur, seuil):
    if valeur > seuil * config.CRITICAL_RATIO:
        return "CRITIQUE"
    return "ATTENTION"


def analyze_chunk(chunk, work):
    """Analyse un morceau de mesures -> (stats partielles par type, alertes détectées)."""
    partial = {}
    found = []
    for r in chunk:
        heavy_compute(r.valeur, work)
        if r.type not in partial:
            partial[r.type] = {"count": 0, "sum": 0.0, "min": r.valeur, "max": r.valeur}
        s = partial[r.type]
        s["count"] += 1
        s["sum"] += r.valeur
        s["min"] = min(s["min"], r.valeur)
        s["max"] = max(s["max"], r.valeur)
        seuil = config.THRESHOLDS.get(r.type)
        if seuil is not None and r.valeur > seuil:
            niveau = niveau_alerte(r.valeur, seuil)
            found.append(models.AlerteUrbaine(r.sensor_id, r.type, r.valeur, seuil, niveau))
    return partial, found


def analyze_tuple(data):
    """Même analyse avec un seul argument (pour ThreadPoolExecutor.map)."""
    chunk, work = data
    return analyze_chunk(chunk, work)


def merge_partial(target, partial):
    """Fusionne des stats partielles dans un dictionnaire cumulé."""
    for t, p in partial.items():
        if t not in target:
            target[t] = dict(p)
        else:
            s = target[t]
            s["count"] += p["count"]
            s["sum"] += p["sum"]
            s["min"] = min(s["min"], p["min"])
            s["max"] = max(s["max"], p["max"])


def les_plus_frequents(valeurs):
    """Renvoie la liste des (valeur, nombre), de la plus fréquente à la moins fréquente."""
    comptes = {}
    for v in valeurs:
        comptes[v] = comptes.get(v, 0) + 1
    ordre = sorted(comptes, key=comptes.get, reverse=True)
    return [(v, comptes[v]) for v in ordre]


# ---------- rapport lisible ----------
ICONES = {"temperature": "🌡️", "co2": "🌫️", "pm25": "😷", "noise": "🔊",
          "traffic": "🚗", "water_level": "💧"}
NOMS = {"temperature": "Température", "co2": "CO₂", "pm25": "Particules PM2.5",
        "noise": "Bruit", "traffic": "Trafic", "water_level": "Niveau d'eau"}


def _pluriel(n, mot):
    return f"{n} {mot}{'s' if n > 1 else ''}"


def format_report(daily, alerts, duree, mode):
    """Rapport lisible. daily = {type: {count, sum, min, max}}, alerts = liste d'AlerteUrbaine."""
    out = ["📊 Rapport SmartCity-Stream",
           f"🕐 Période de {duree:.0f} s · analyse en mode {mode}", ""]

    total = 0
    for s in daily.values():
        total += s["count"]
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
        for t, n in les_plus_frequents([a.type for a in alerts]):
            groupe = [a for a in alerts if a.type == t]
            pic = max(a.valeur for a in groupe)
            out.append(f"   • {NOMS.get(t, t)} : {_pluriel(n, 'alerte')} "
                       f"(pic {pic:.1f}, seuil {groupe[0].seuil})")
        top = les_plus_frequents([a.sensor_id for a in alerts])[:3]
        out.append("   Capteurs les plus en alerte : " + ", ".join(f"{s} ({n})" for s, n in top))
    return "\n".join(out)