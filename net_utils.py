import pickle
import struct


def send_msg(sock, obj):
    """TCP = flux d'octets : on envoie d'abord la taille (4 octets), puis le message."""
    data = pickle.dumps(obj)
    sock.sendall(struct.pack("!I", len(data)) + data)


def recevoir_exactement(sock, n):
    """Lit exactement n octets."""
    donnees = b""
    while len(donnees) < n:
        morceau = sock.recv(n - len(donnees))
        if not morceau:
            raise ConnectionError("connexion fermée")
        donnees += morceau
    return donnees


def recv_msg(sock):
    taille = struct.unpack("!I", recevoir_exactement(sock, 4))[0]
    return pickle.loads(recevoir_exactement(sock, taille))