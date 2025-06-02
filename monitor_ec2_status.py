
import os
import psutil
import boto3
import logging
from datetime import datetime
import time
import watchtower
import requests

# === CONFIGURACION ===
THRESHOLDS = {
    "cpu": 90,
    "memoria": 65,
    "disco": 80,
    "procesos": 200,
    "uptime": 2000  # en minutos
}
LOG_FILE = "/home/ubuntu/tr/logs/monitor/monitor_status.log"
LOG_GROUP = "EC2MonitorLogs"
LOG_STREAM = "vm01-prod"

# === LOGGING ===
logger = logging.getLogger("EC2-status")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s,%(name)s,%(levelname)s,%(message)s", datefmt="%Y-%m-%d %H:%M:%S")

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(formatter)

cw_handler = watchtower.CloudWatchLogHandler(
    log_group=LOG_GROUP,
    stream_name=LOG_STREAM
)
cw_handler.setFormatter(formatter)

logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(cw_handler)

# === METRICAS ===
def obtener_uptime():
    try:
        with open("/proc/uptime", "r") as f:
            segundos = float(f.readline().split()[0])
            return round(segundos / 60, 2)
    except Exception as e:
        logger.error(f"Error obteniendo uptime: {e}")
        return 0

def obtener_instance_id():
    try:
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=2
        ).text
        instance_id = requests.get(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=2
        ).text.strip()
        return instance_id
    except Exception as e:
        logger.error(f"Error obteniendo instance-id: {e}")
        return None

def evaluar_y_loguear(nombre, valor, umbral):
    estado = "INFO"
    alerta = ""
    if nombre == "uptime" and valor > THRESHOLDS["uptime"]:
        estado = "ERROR"
        alerta = "ALERTA_UPTIME_SHUTDOWN"
        instance_id = obtener_instance_id()
        if instance_id:
            try:
                ec2 = boto3.client("ec2", region_name="eu-central-1")
                ec2.stop_instances(InstanceIds=[instance_id])
                logger.error(f"Instancia {instance_id} apagada por uptime > {THRESHOLDS['uptime']} min")
            except Exception as e:
                logger.error(f"No se pudo apagar la instancia {instance_id}: {e}")
    elif nombre == "disco":
        if valor >= 90:
            estado = "ERROR"
        elif valor >= 85:
            estado = "WARNING"
    elif valor > umbral:
        estado = "WARNING"

    logger.log(getattr(logging, estado), f"{nombre.upper()} = valor={valor:.2f} - status={estado} - umbral={umbral}" + (f" - {alerta}" if alerta else ""))

# === FUNCION PRINCIPAL ===
def monitorear():
    cpu = psutil.cpu_percent(interval=1)
    memoria = psutil.virtual_memory().percent
    disco = psutil.disk_usage('/').percent
    procesos = len(psutil.pids())
    uptime = obtener_uptime()

    evaluar_y_loguear("cpu", cpu, THRESHOLDS["cpu"])
    evaluar_y_loguear("ram", memoria, THRESHOLDS["memoria"])
    evaluar_y_loguear("hdd", disco, THRESHOLDS["disco"])
    evaluar_y_loguear("procesos", procesos, THRESHOLDS["procesos"])
    evaluar_y_loguear("uptime", uptime, THRESHOLDS["uptime"])

    logger.info("OK - status saved.")

if __name__ == "__main__":
    monitorear()
