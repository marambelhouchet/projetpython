HOST = "127.0.0.1"
UDP_PORT = 9999            # capteurs -> serveur (UDP)
TCP_PORT = 8888            # opérateurs <-> serveur (TCP)

MODE = "threads"           # "threads" ou "processes"
WORKERS = None             # None = nombre de coeurs (mp.cpu_count())
MAX_OPERATORS = 5          # Semaphore : nb max d'opérateurs servis en même temps
BATCH_SIZE = 200           # taille max d'un lot envoyé au pool
BATCH_TIMEOUT = 1.0        # secondes d'attente pour remplir un lot
WORK_FACTOR = 300          # simule un calcul lourd par mesure. 0 = aucun

REPORT_INTERVAL = 60       # secondes entre deux rapports ("1 jour" simulé)
REPORT_FILE = "rapports.txt"
LOG_FILE = "server.log"

# --- Capteurs simulés ---
NB_SENSORS = 12
SENSOR_INTERVAL = 1.0      # secondes entre deux mesures d'un capteur
SPIKE_PERCENT = 5          # % de mesures anormalement hautes (=> alerte)

# --- Analyse par LLM (Gemini) ---
LLM_ON = True
GEMINI_MODEL = "gemini-3.5-flash-lite"   # voir la liste : python analysis.py
LLM_TIMEOUT = 30            # secondes max par appel
LLM_MAX_TOKENS = 2048
LLM_MIN_INTERVAL = 6.0      # secondes entre 2 appels (quota gratuit)
LLM_ALERT_INTERVAL = 30     # secondes entre deux diagnostics d'alertes
LLM_MIN_ALERTS = 5          # pas de diagnostic en dessous de ce nombre


def lire_cle():
    """Lit la ligne GEMINI_API_KEY=... du fichier .env."""
    try:
        with open(".env", "r", encoding="utf-8") as f:
            for ligne in f:
                parties = ligne.split("=", 1)
                if len(parties) == 2 and parties[0].strip() == "GEMINI_API_KEY":
                    return parties[1].strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return ""


GEMINI_API_KEY = lire_cle()

# Seuil d'alerte par type de capteur
THRESHOLDS = {
    "temperature": 40.0,   # °C
    "co2": 1000.0,         # ppm
    "pm25": 50.0,          # µg/m3
    "noise": 85.0,         # dB
    "traffic": 120.0,      # véhicules/min
    "water_level": 4.0,    # m
}
CRITICAL_RATIO = 1.25      # valeur > seuil * 1.25 => CRITIQUE, sinon ATTENTION

# Profil des capteurs simulés : (moyenne, variation)
PROFILES = {
    "temperature": (28, 5),
    "co2": (700, 120),
    "pm25": (30, 8),
    "noise": (65, 7),
    "traffic": (70, 20),
    "water_level": (2.5, 0.5),
}