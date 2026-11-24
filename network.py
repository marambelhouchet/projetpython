import pickle
import queue
import socket
import threading

import config
import models
from net_utils import recv_msg, send_msg


def udp_loop(state, inq, stop):
    """Reçoit les mesures des capteurs et les met dans la file inq."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    sock.bind((config.HOST, config.UDP_PORT))
    sock.settimeout(1.0)
    state.log(f"UDP en écoute sur {config.HOST}:{config.UDP_PORT}")

    while not stop.is_set():
        try:
            data, _ = sock.recvfrom(4096)
        except socket.timeout:
            continue

        try:
            mesure = pickle.loads(data)
        except Exception as e:
            state.log(f"paquet UDP illisible: {e}")
            continue

        inq.put(mesure)
        with state.lock:
            state.m["received"] += 1
            state.last.append(mesure)
            del state.last[:-100]

    sock.close()


def notifier_loop(notifyq, subs, subs_lock, stop):
    """Envoie alertes, rapports et analyses à tous les opérateurs abonnés."""
    while not stop.is_set():
        try:
            kind, data = notifyq.get(timeout=0.5)
        except queue.Empty:
            continue

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


def handle_operator(conn, addr, state, subs, subs_lock, sem, stop):
    """Un thread par opérateur : lit ses commandes et répond."""
    lock = threading.Lock()          # un seul envoi à la fois sur cette connexion
    state.log(f"opérateur connecté {addr}")
    try:
        while not stop.is_set():
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
    except (ConnectionError, OSError):
        pass
    except Exception as e:
        state.log(f"erreur opérateur {addr}: {e}")
    finally:
        with subs_lock:
            subs.pop(conn, None)
        conn.close()
        sem.release()                # on rend la place
        state.log(f"opérateur déconnecté {addr}")


def tcp_loop(state, subs, subs_lock, stop):
    """Accepte les opérateurs (maximum MAX_OPERATORS grâce au sémaphore)."""
    sem = threading.Semaphore(config.MAX_OPERATORS)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((config.HOST, config.TCP_PORT))
    srv.listen()
    srv.settimeout(1.0)
    state.log(f"TCP en écoute sur {config.HOST}:{config.TCP_PORT} (max {config.MAX_OPERATORS} opérateurs)")

    while not stop.is_set():
        try:
            conn, addr = srv.accept()
        except socket.timeout:
            continue

        if not sem.acquire(blocking=False):      # plus de place
            try:
                send_msg(conn, models.Reponse(False, "SERVER_FULL"))
            finally:
                conn.close()
            state.log(f"{addr} refusé (serveur plein)")
            continue

        threading.Thread(target=handle_operator,
                         args=(conn, addr, state, subs, subs_lock, sem, stop),
                         daemon=True).start()

    srv.close()