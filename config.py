HOST = "127.0.0.1"
UDP_PORT = 9999            # capteurs -> serveur (UDP)
TCP_PORT = 8888            # opérateurs <-> serveur (TCP)

MAX_OPERATORS = 5          # Semaphore : nb max d'opérateurs simultanés
WORKERS = None             # None = os.cpu_count()
BATCH_SIZE = 200           # taille max d'un lot envoyé au Pool
BATCH_TIMEOUT = 1.0        # secondes avant de traiter un lot incomplet
WORK_FACTOR = 300          # simule un calcul lourd par mesure (ML, FFT...). 0 = aucun

REPORT_INTERVAL = 60       # secondes entre deux rapports ("1 jour" simulé)
REPORT_DIR = "reports"
LOG_FILE = "server.log"

# Seuil d'alerte par type de capteur (éléments de la smart city)
THRESHOLDS = {
    "temperature": 40.0,   # °C
    "co2": 1000.0,         # ppm
    "pm25": 50.0,          # µg/m3
    "noise": 85.0,         # dB
    "traffic": 120.0,      # véhicules/min
    "water_level": 4.0,    # m
}
CRITICAL_RATIO = 1.25      # valeur > seuil * 1.25 => CRITIQUE, sinon ATTENTION

# Profil des capteurs simulés : (moyenne, écart-type)
PROFILES = {
    "temperature": (28, 5),
    "co2": (700, 120),
    "pm25": (30, 8),
    "noise": (65, 7),
    "traffic": (70, 20),
    "water_level": (2.5, 0.5),
}