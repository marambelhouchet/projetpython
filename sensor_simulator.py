"""Simule des capteurs (un thread par capteur) qui envoient en UDP.
Réglages dans config.py : NB_SENSORS, SENSOR_INTERVAL, SPIKE_PERCENT."""
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


def main():
    types = list(config.PROFILES)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for i in range(config.NB_SENSORS):
        typ = types[i % len(types)]
        Capteur(f"{typ[:4].upper()}-{i:04d}", typ, sock).start()
    print(f"{config.NB_SENSORS} capteurs -> UDP {config.HOST}:{config.UDP_PORT}")
    input("Appuyez sur Entrée pour arrêter.\n")


if __name__ == "__main__":
    main()