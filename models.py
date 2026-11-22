import time


class RapportCapteur:
    def __init__(self, sensor_id, type, valeur, timestamp=None):
        self.sensor_id = sensor_id
        self.type = type             # "temperature", "co2", "pm25", "noise", "traffic", "water_level"
        self.valeur = valeur
        self.timestamp = time.time() if timestamp is None else timestamp

    def __repr__(self):
        return f"RapportCapteur({self.sensor_id}, {self.type}, {self.valeur})"


class AlerteUrbaine:
    def __init__(self, sensor_id, type, valeur, seuil, niveau="ATTENTION", timestamp=None):
        self.sensor_id = sensor_id
        self.type = type
        self.valeur = valeur
        self.seuil = seuil
        self.niveau = niveau         # ATTENTION | CRITIQUE
        self.timestamp = time.time() if timestamp is None else timestamp

    def __str__(self):
        t = time.strftime("%H:%M:%S", time.localtime(self.timestamp))
        return (f"[{t}] {self.niveau} {self.type} capteur={self.sensor_id} "
                f"valeur={self.valeur:.1f} (seuil {self.seuil})")

    __repr__ = __str__


class Requete:
    def __init__(self, commande, arg=10):
        self.commande = commande     # SUBSCRIBE | STATS | ALERTS | LAST | REPORT | METRICS | PING | QUIT
        self.arg = arg

    def __repr__(self):
        return f"Requete({self.commande}, {self.arg})"


class Reponse:
    def __init__(self, ok, data=None, kind="reply"):
        self.ok = ok
        self.data = data
        self.kind = kind             # reply = réponse à une requête | alert / report = poussé par le serveur

    def __repr__(self):
        return f"Reponse(ok={self.ok}, kind={self.kind}, data={self.data})"