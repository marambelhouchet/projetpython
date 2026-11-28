import pickle
import socket
import threading

import config
import models
from net_utils import recv_msg, send_msg


def udp_loop(state, inq):
    """Reçoit les mesures des capteurs et les met dans la file inq."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((config.HOST, config.UDP_PORT))
    state.log(f"UDP en écoute sur {config.HOST}:{config.UDP_PORT}")

    while not state.stop:
        data, addr = sock.recvfrom(4096)
        try:
            mesure = pickle.loads(data)
        except Exception as e:
            state.log(f"paquet UDP illisible de {addr}: {e}")
            continue

        inq.put(mesure)
        with state.lock:
            state.m["received"] += 1
            state.last.append(mesure)
            del state.last[:-100]

    sock.close()


def notifier_loop(notifyq, subs, subs_lock):
    """Envoie alertes, rapports et analyses à tous les opérateurs abonnés."""
    while True:
        kind, data = notifyq.get()

        # On copie la liste des abonnés vite, puis on envoie hors du verrou
        with subs_lock:
            cibles = list(subs.items())

        for conn, lock in cibles:
            try:
                with lock:
                    send_msg(conn, models.Reponse(True, data, kind))
            except OSError:
                with subs_lock:
                    subs.pop(conn, None)


def dialoguer(conn, addr, state, subs, subs_lock, lock):
    """Lit les commandes d'un opérateur et lui répond."""
    while not state.stop:
        req = recv_msg(conn)
        if not isinstance(req, models.Requete):
            with lock:
                send_msg(conn, models.Reponse(False, "requête invalide"))
            continue

        cmd = req.commande.upper()
        if cmd == "QUIT":
            with lock:
                send_msg(conn, models.Reponse(True, "bye"))
            break

        if cmd == "SUBSCRIBE":
            with subs_lock:
                subs[conn] = lock
            ok, data = True, "abonné aux alertes, rapports et analyses IA"
        else:
            ok, data = state.answer(cmd, req.arg)

        state.log(f"{addr} -> {cmd}")
        with lock:
            send_msg(conn, models.Reponse(ok, data))


def handle_operator(conn, addr, state, subs, subs_lock, sem):
    """Un thread par opérateur. Le sémaphore limite le nombre d'opérateurs servis."""
    lock = threading.Lock()          # un seul envoi à la fois sur cette connexion
    with sem:                        # attend ici s'il n'y a plus de place
        state.log(f"opérateur connecté {addr}")
        try:
            dialoguer(conn, addr, state, subs, subs_lock, lock)
        except OSError:
            pass                     # connexion coupée
        except Exception as e:
            state.log(f"erreur opérateur {addr}: {e}")
        finally:
            with subs_lock:
                subs.pop(conn, None)
            conn.close()
            state.log(f"opérateur déconnecté {addr}")


def tcp_loop(state, subs, subs_lock):
    """Thread veilleur : accepte les opérateurs et crée un thread par connexion."""
    sem = threading.Semaphore(config.MAX_OPERATORS)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((config.HOST, config.TCP_PORT))
    srv.listen(5)
    state.log(f"TCP en écoute sur {config.HOST}:{config.TCP_PORT} (max {config.MAX_OPERATORS} opérateurs)")

    while not state.stop:
        conn, addr = srv.accept()
        threading.Thread(target=handle_operator,
                         args=(conn, addr, state, subs, subs_lock, sem),
                         daemon=True).start()

    srv.close()