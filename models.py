import time
from dataclasses import dataclass, field


@dataclass
class RapportCapteur:
    sensor_id: str
    type: str            # "temperature", "co2", "pm25", "noise", "traffic", "water_level"
    valeur: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class AlerteUrbaine:
    sensor_id: str
    type: str
    valeur: float
    seuil: float
    niveau: str = "ATTENTION"      # ATTENTION | CRITIQUE
    timestamp: float = field(default_factory=time.time)

    def __str__(self):
        t = time.strftime("%H:%M:%S", time.localtime(self.timestamp))
        return (f"[{t}] {self.niveau} {self.type} capteur={self.sensor_id} "
                f"valeur={self.valeur:.1f} (seuil {self.seuil})")


@dataclass
class Requete:
    commande: str        # SUBSCRIBE | STATS | ALERTS | LAST | REPORT | METRICS | PING | QUIT
    arg: int = 10


@dataclass
class Reponse:
    ok: bool
    data: object = None
    kind: str = "reply"  # reply = réponse à une requête | alert / report = poussé par le serveur