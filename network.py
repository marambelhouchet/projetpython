import queue
import socket
import threading

import calcul
import config
import models
from net_utils import recv_msg, safe_loads, send_msg


def udp_loop(state, inq, stop):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    sock.bind((config.HOST, config.UDP_PORT))
    sock.settimeout(1.0)
    state.log(f"UDP en écoute sur {config.HOST}:{config.UDP_PORT}")

    while not stop.is_set():
        try:
            data, _ = sock.recvfrom(4096)
            r = safe_loads(data)
            if not calcul.valid_report(r):
                raise ValueError("mesure invalide")
        except socket.timeout:
            continue
        except Exception as e:
            state.log(f"paquet UDP rejeté: {e}")
            continue

        inq.put(r)
        with state.lock:
            state.m["received"] += 1
            state.last.append(r)
            del state.last[:-100]

    sock.close()


def notifier_loop(notifyq, subs, subs_lock, stop):
    while not stop.is_set():
        try:
            kind, data = notifyq.get(timeout=0.5)
        except queue.Empty:
            continue

        with subs_lock:
            targets = list(subs.items())

        for conn, lock in targets:
            try:
                with lock:
                    send_msg(conn, models.Reponse(True, data, kind))
            except OSError:
                with subs_lock:
                    subs.pop(conn, None)


def handle_operator(conn, addr, state, subs, subs_lock, sem, stop):
    lock = threading.Lock()
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
        sem.release()
        state.log(f"opérateur déconnecté {addr}")


def tcp_loop(state, subs, subs_lock, stop):
    sem = threading.Semaphore(config.MAX_OPERATORS)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((config.HOST, config.TCP_PORT))
    srv.listen()
    srv.settimeout(1.0)
    state.log(
        f"TCP en écoute sur {config.HOST}:{config.TCP_PORT} "
        f"(max {config.MAX_OPERATORS} opérateurs)"
    )

    while not stop.is_set():
        try:
            conn, addr = srv.accept()
        except socket.timeout:
            continue

        if not sem.acquire(blocking=False):
            try:
                send_msg(conn, models.Reponse(False, "SERVER_FULL"))
            finally:
                conn.close()
            state.log(f"{addr} refusé (serveur plein)")
            continue

        threading.Thread(
            target=handle_operator,
            args=(conn, addr, state, subs, subs_lock, sem, stop),
            daemon=True,
        ).start()

    srv.close()