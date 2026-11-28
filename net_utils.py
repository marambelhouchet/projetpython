import pickle

TAILLE_ENTETE = 10      # l'en-tête contient la taille du message, écrite sur 10 caractères


def send_msg(sock, obj):
    """TCP = flux d'octets : on envoie la taille du message, puis le message."""
    data = pickle.dumps(obj)
    msg = "{:10d}".format(len(data)).encode() + data
    totalsent = 0
    while totalsent < len(msg):              # boucle d'envoi
        sent = sock.send(msg[totalsent:])
        totalsent += sent


def recevoir_exactement(sock, n):
    """Boucle de réception : lit exactement n octets."""
    donnees = b""
    while len(donnees) < n:
        morceau = sock.recv(min(n - len(donnees), 4096))
        if morceau == b"":
            raise ConnectionError("connexion fermée")
        donnees += morceau
    return donnees


def recv_msg(sock):
    taille = int(recevoir_exactement(sock, TAILLE_ENTETE).decode())
    return pickle.loads(recevoir_exactement(sock, taille))