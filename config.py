HOST = "127.0.0.1"
UDP_PORT = 9999          # capteurs -> serveur (UDP)
TCP_PORT = 8888          # opérateurs <-> serveur (TCP)

MAX_OPERATORS = 3        # Semaphore : nb max d'opérateurs simultanés
POOL_SIZE = None         # None = tous les coeurs CPU
BATCH_SIZE = 50          # taille max d'un lot envoyé au Pool
BATCH_TIMEOUT = 2.0      # secondes avant de traiter un lot incomplet
LOG_FILE = "server.log"

# Seuils d'alerte par type de capteur
THRESHOLDS = {"temperature": 40.0, "co2": 1000.0, "pm25": 50.0}