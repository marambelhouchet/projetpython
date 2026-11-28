"""Simule des capteurs (un thread par capteur) qui envoient en UDP.
Réglages par défaut dans config.py ; les options ci-dessous les écrasent pour cette exécution :
  --sensors N   --interval S   --spike P   --duration S (0 = jusqu'à Entrée)"""
import argparse
import pickle
import random
import socket
import threading
import time

import config
from models import RapportCapteur


class Capteur(threading.Thread):
    def __init__(self, sid, typ, sock):
        threading.Thread.__init__(self, daemon=True)
        self.sid = sid
        self.typ = typ
        self.sock = sock

    def run(self):
        moyenne, variation = config.PROFILES[self.typ]
        addr = (config.HOST, config.UDP_PORT)
        while True:
            v = moyenne + variation * random.randrange(-200, 201) / 100
            if random.randrange(100) < config.SPIKE_PERCENT:
                v = v * random.randrange(140, 201) / 100       # pic anormal
            r = RapportCapteur(self.sid, self.typ, round(max(v, 0.0), 2))
            self.sock.sendto(pickle.dumps(r), addr)             # UDP : pas de connexion, pas de garantie
            time.sleep(config.SENSOR_INTERVAL * random.randrange(80, 121) / 100)


def lire_options():
    p = argparse.ArgumentParser(description="Simulateur de capteurs")
    p.add_argument("--sensors", type=int, help="nombre de capteurs (un thread chacun)")
    p.add_argument("--interval", type=float, help="secondes entre deux mesures d'un capteur")
    p.add_argument("--spike", type=int, help="%% de mesures anormalement hautes")
    p.add_argument("--duration", type=int, default=0,
                   help="durée en secondes avant arrêt automatique (0 = jusqu'à Entrée)")
    a = p.parse_args()

    if a.sensors is not None:
        config.NB_SENSORS = a.sensors
    if a.interval is not None:
        config.SENSOR_INTERVAL = a.interval
    if a.spike is not None:
        config.SPIKE_PERCENT = a.spike
    return a


def main():
    options = lire_options()
    types = list(config.PROFILES)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for i in range(config.NB_SENSORS):
        typ = types[i % len(types)]
        Capteur(f"{typ[:4].upper()}-{i:04d}", typ, sock).start()
    print(f"{config.NB_SENSORS} capteurs -> UDP {config.HOST}:{config.UDP_PORT} "
          f"(intervalle {config.SENSOR_INTERVAL}s, pics {config.SPIKE_PERCENT}%)")

    if options.duration > 0:
        print(f"Arrêt automatique dans {options.duration} s (Ctrl+C pour arrêter avant).")
        try:
            time.sleep(options.duration)
        except KeyboardInterrupt:
            pass
        print("Capteurs arrêtés.")
    else:
        input("Appuyez sur Entrée pour arrêter.\n")


if __name__ == "__main__":
    main()