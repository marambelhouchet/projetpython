"""Console opérateur (TCP). S'abonne automatiquement : les ALERTES, RAPPORTS et ANALYSES arrivent en direct.
Commandes : stats | alerts [n] | last [n] | report | analysis | metrics | ping | quit
(Ne pas nommer ce fichier operator.py : il masquerait le module standard 'operator'.)"""
import socket
import threading

import config
from models import Requete
from net_utils import recv_msg, send_msg

stop = False


def show(rep):
    d = rep.data
    if rep.kind == "alert":
        print(f"\n>>> ATTENTION ALERTE : {d}")
    elif rep.kind == "report":
        print(f"\n>>> RAPPORT PÉRIODIQUE\n{d}")
    elif rep.kind == "analysis":
        print(f"\n>>> ANALYSE IA\n{d}")
    elif not rep.ok:
        print(f"\nErreur : {d}")
    elif isinstance(d, dict):
        print()
        for k, v in d.items():
            if isinstance(v, dict):
                print(f"  {k:12} n={v['count']:6} moy={v['avg']:9.2f} "
                      f"min={v['min']:9.2f} max={v['max']:9.2f}")
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


def receiver(sock):
    """Thread qui affiche tout ce que le serveur envoie (réponses ET alertes poussées)."""
    global stop
    try:
        while True:
            show(recv_msg(sock))
    except OSError:
        pass
    finally:
        stop = True


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((config.HOST, config.TCP_PORT))
    t = threading.Thread(target=receiver, args=(sock,), daemon=True)
    t.start()
    send_msg(sock, Requete("SUBSCRIBE"))
    print("Commandes : stats | alerts [n] | last [n] | report | analysis | metrics | ping | quit")
    print(f"(Si {config.MAX_OPERATORS} opérateurs sont déjà connectés, patientez.)")
    try:
        while not stop:
            parts = input("> ").strip().split()
            if not parts:
                continue
            arg = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 10
            send_msg(sock, Requete(parts[0], arg))
            if parts[0].lower() == "quit":
                t.join()                     # le serveur répond "bye" puis ferme
                break
    except (EOFError, KeyboardInterrupt, OSError):
        pass
    finally:
        sock.close()


if __name__ == "__main__":
    main()