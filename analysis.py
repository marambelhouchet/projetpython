"""Analyse par LLM (Gemini). Rien à installer : on appelle l'API REST avec la bibliothèque standard.

Le LLM reçoit des SYNTHÈSES (pas chaque mesure) et les interprète.
Clé : variable GEMINI_API_KEY (fichier .env, lu par config.py).
Test de la clé et liste des modèles :   python analysis.py
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter

import config

URL = "https://generativelanguage.googleapis.com/v1beta"
CONSIGNE = "Tu es un analyste d'une ville intelligente. Réponds en français, de façon concise. N'invente rien."


class LLMError(Exception):
    pass


def cle():
    """Clé API : celle de config.py, sinon la variable d'environnement GEMINI_API_KEY."""
    return config.GEMINI_API_KEY.strip() or os.environ.get("GEMINI_API_KEY", "").strip()


def available():
    return bool(cle())


def model():
    return os.environ.get("GEMINI_MODEL", config.GEMINI_MODEL)


# ---------- appel à Gemini ----------
_lock = threading.Lock()
_dernier_appel = 0.0


def _attendre():
    """Espace les appels (le palier gratuit limite le nombre de requêtes par minute)."""
    global _dernier_appel
    delai = float(os.environ.get("LLM_MIN_INTERVAL", config.LLM_MIN_INTERVAL))
    with _lock:
        pause = _dernier_appel + delai - time.monotonic()
        if pause > 0:
            time.sleep(pause)
        _dernier_appel = time.monotonic()


def _appel(url, corps=None):
    if not available():
        raise LLMError("clé absente : renseignez GEMINI_API_KEY dans le fichier .env")
    requete = urllib.request.Request(
        url, data=json.dumps(corps).encode() if corps else None,
        headers={"Content-Type": "application/json", "x-goog-api-key": cle()})
    try:
        with urllib.request.urlopen(requete, timeout=config.LLM_TIMEOUT) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise LLMError(f"erreur HTTP {e.code} (modèle ou clé invalide ? lancez : python analysis.py)") from None
    except OSError as e:
        raise LLMError(f"réseau : {e}") from None


def generate(prompt, consigne=CONSIGNE):
    _attendre()
    corps = {"contents": [{"role": "user", "parts": [{"text": prompt}]}],
             "generationConfig": {"maxOutputTokens": config.LLM_MAX_TOKENS}}
    if consigne:
        corps["systemInstruction"] = {"parts": [{"text": consigne}]}
    base = os.environ.get("GEMINI_BASE_URL", URL)
    reponse = _appel(f"{base}/models/{model()}:generateContent", corps)
    try:
        return "".join(p.get("text", "") for p in reponse["candidates"][0]["content"]["parts"]).strip()
    except (KeyError, IndexError):
        raise LLMError("réponse vide ou bloquée") from None


# ---------- les 2 analyses ----------
def _heure(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts))


def diagnose_alerts(alertes):
    """Diagnostic d'un groupe d'alertes (une ligne de synthèse par type de capteur)."""
    lignes = []
    for t in sorted({a.type for a in alertes}):
        groupe = [a for a in alertes if a.type == t]
        top = Counter(a.sensor_id for a in groupe).most_common(3)
        lignes.append(f"- {t} : {len(groupe)} alertes, pic {max(a.valeur for a in groupe):.1f} "
                      f"(seuil {groupe[0].seuil}), capteurs : " + ", ".join(f"{s} x{n}" for s, n in top))
    return generate(
        f"Alertes de {_heure(min(a.timestamp for a in alertes))} à {_heure(max(a.timestamp for a in alertes))} :\n"
        + "\n".join(lignes) + "\n"
        "Donne : 1) la situation en une phrase ; 2) les causes probables ; 3) 3 actions maximum, par priorité.")


def summarize_report(texte):
    """Résumé en langage naturel du rapport périodique."""
    return generate(texte + "\n\nRésume ce rapport en 5 lignes maximum, sur un ton naturel et humain : "
                    "tendances, risques, conseil. Pas de titres, pas de markdown, pas de listes à puces.")


if __name__ == "__main__":
    if not available():
        sys.exit('Aucune clé : renseignez GEMINI_API_KEY dans le fichier .env')
    try:
        base = os.environ.get("GEMINI_BASE_URL", URL)
        noms = [m["name"][7:] for m in _appel(f"{base}/models?pageSize=200").get("models", [])
                if "generateContent" in m.get("supportedGenerationMethods", []) and "gemini" in m["name"]]
        print("Modèles utilisables avec votre clé :", *sorted(noms), sep="\n  ")
        print(f"\nModèle utilisé : {model()}   (changer : set GEMINI_MODEL=nom)")
        print("Test :", generate("Réponds uniquement : OK", consigne=None))
    except LLMError as e:
        sys.exit(f"Erreur : {e}")