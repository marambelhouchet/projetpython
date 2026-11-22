"""Console opérateur (TCP). S'abonne automatiquement : les ALERTES et RAPPORTS arrivent en direct.
Commandes : stats | alerts [n] | last [n] | report | metrics | ping | quit
(Ne pas nommer ce fichier operator.py : il masquerait le module standard 'operator'.)"""
import socket
import threading

import config
from models import Requete
from net_utils import recv_msg, send_msg


def show(rep):
    d = rep.data
    if rep.kind == "alert":
        print(f"\n>>> ATTENTION ALERTE : {d}")
    elif rep.kind == "report":
        print(f"\n>>> RAPPORT PÉRIODIQUE\n{d}")
    elif not rep.ok:
        print(f"\nErreur : {d}")
    elif isinstance(d, dict):
        print()
        for k, v in d.items():
            if isinstance(v, dict):
                print(f"  {k:12} n={v['count']:6} moy={v['avg']:9.2f} min={v['min']:9.2f} max={v['max']:9.2f}")
            else:
                print(f"  {k:16} {v}")
    elif isinstance(d, list):
        print()
        for x in d:
            print(" ", x)
        if not d:
            print("  (vide)")
    else:
        print(f"\n{d}")
    print("> ", end="", flush=True)


def receiver(sock, stop):
    """Thread qui affiche tout ce que le serveur envoie (réponses ET alertes poussées)."""
    try:
        while True:
            show(recv_msg(sock))
    except (ConnectionError, OSError):
        pass
    finally:
        stop.set()


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((config.HOST, config.TCP_PORT))
    stop = threading.Event()
    threading.Thread(target=receiver, args=(sock, stop), daemon=True).start()
    send_msg(sock, Requete("SUBSCRIBE"))
    print("Commandes : stats | alerts [n] | last [n] | report | metrics | ping | quit")
    try:
        while not stop.is_set():
            parts = input("> ").strip().split()
            if not parts:
                continue
            arg = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 10
            send_msg(sock, Requete(parts[0], arg))
            if parts[0].lower() == "quit":
                stop.wait(2)
                break
    except (EOFError, KeyboardInterrupt, OSError):
        pass
    finally:
        sock.close()


if __name__ == "__main__":
    main()