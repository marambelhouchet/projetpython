import time


class RapportCapteur:
    def __init__(self, sensor_id, type, valeur):
        self.sensor_id = sensor_id
        self.type = type             # "temperature", "co2", "pm25", "noise", "traffic", "water_level"
        self.valeur = valeur
        self.timestamp = time.time()

    def __str__(self):
        return f"{self.sensor_id} {self.type} = {self.valeur}"


class AlerteUrbaine:
    def __init__(self, sensor_id, type, valeur, seuil, niveau):
        self.sensor_id = sensor_id
        self.type = type
        self.valeur = valeur
        self.seuil = seuil
        self.niveau = niveau         # ATTENTION | CRITIQUE

    def __str__(self):
        return (f"{self.niveau} {self.type} capteur={self.sensor_id} "
                f"valeur={self.valeur:.1f} (seuil {self.seuil})")


class Requete:
    def __init__(self, commande, arg=10):
        self.commande = commande     # SUBSCRIBE | STATS | ALERTS | LAST | REPORT | ANALYSIS | METRICS | PING | QUIT
        self.arg = arg


class Reponse:
    def __init__(self, ok, data=None, kind="reply"):
        self.ok = ok
        self.data = data
        self.kind = kind             # reply = réponse à une requête | alert / report / analysis = poussé par le serveur