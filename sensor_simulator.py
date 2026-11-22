"""Simule N capteurs (un thread par capteur) qui envoient en UDP.

Usage : python sensor_simulator.py [--sensors 12] [--interval 1.0] [--spike 0.05] [--duration 0]
Chaque capteur a un type (temperature, co2, pm25, noise, traffic, water_level).
--spike = probabilité qu'une mesure soit anormalement haute (=> alerte)
"""
import argparse
import pickle
import random
import socket
import threading

import config
from models import RapportCapteur


def sensor_loop(sid, typ, sock, interval, spike, stop):
    mean, std = config.PROFILES[typ]
    addr = (config.HOST, config.UDP_PORT)
    while not stop.is_set():
        v = random.gauss(mean, std)
        if random.random() < spike:
            v *= random.uniform(1.4, 2.0)                   # pic anormal
        r = RapportCapteur(sid, typ, round(max(v, 0.0), 2))
        sock.sendto(pickle.dumps(r), addr)                  # UDP : pas de connexion, pas de garantie
        stop.wait(interval * random.uniform(0.8, 1.2))      # un peu de gigue


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sensors", type=int, default=12)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--spike", type=float, default=0.05)
    ap.add_argument("--duration", type=float, default=0, help="0 = jusqu'à Ctrl+C")
    a = ap.parse_args()

    types = list(config.PROFILES)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    stop = threading.Event()
    for i in range(a.sensors):
        typ = types[i % len(types)]
        threading.Thread(target=sensor_loop, daemon=True,
                         args=(f"{typ[:4].upper()}-{i:04d}", typ, sock, a.interval, a.spike, stop)).start()
    print(f"{a.sensors} capteurs -> UDP {config.HOST}:{config.UDP_PORT}, ~1 mesure/{a.interval}s chacun "
          f"(~{a.sensors / a.interval:.0f} mesures/s). Ctrl+C pour arrêter.")
    try:
        stop.wait(a.duration or None)
    except KeyboardInterrupt:
        pass
    stop.set()


if __name__ == "__main__":
    main()