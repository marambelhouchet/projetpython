import builtins
import io
import pickle
import struct

import models

_ALLOWED_MODELS = {"RapportCapteur", "AlerteUrbaine", "Requete", "Reponse"}
_ALLOWED_BUILTINS = {"dict", "list", "set", "tuple", "float", "int", "str"}


class SafeUnpickler(pickle.Unpickler):
    """Refuse toute classe hors models.py (pickle brut = exécution de code à distance)."""

    def find_class(self, module, name):
        if module == "models" and name in _ALLOWED_MODELS:
            return getattr(models, name)
        if module == "builtins" and name in _ALLOWED_BUILTINS:
            return getattr(builtins, name)
        raise pickle.UnpicklingError(f"classe interdite: {module}.{name}")


def safe_loads(data: bytes):
    return SafeUnpickler(io.BytesIO(data)).load()


def send_msg(sock, obj):
    """TCP = flux d'octets : on préfixe chaque message par sa taille (4 octets)."""
    payload = pickle.dumps(obj)
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connexion fermée")
        buf += chunk
    return buf


def recv_msg(sock):
    (length,) = struct.unpack("!I", _recv_exact(sock, 4))
    return safe_loads(_recv_exact(sock, length))