import logging
import os
from datetime import datetime
from pathlib import Path

def configurar_logger(nombre_estrategia, log_dir_base=None):
    if log_dir_base is None:
        log_dir_base = str(Path.home() / "tr" / "logs" / "estrategias")

    logger = logging.getLogger(nombre_estrategia)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger  # evitar duplicados si ya está configurado

    hoy = datetime.utcnow().strftime("%Y-%m-%d")
    dir_estrategia = os.path.join(log_dir_base, nombre_estrategia)
    os.makedirs(dir_estrategia, exist_ok=True)

    # Log persistente
    log_file_persistente = os.path.join(dir_estrategia, f"{nombre_estrategia}.log")
    fh = logging.FileHandler(log_file_persistente)
    fh.setFormatter(logging.Formatter("%(asctime)s,%(levelname)s,%(message)s"))

    # Log diario
    log_file_diario = os.path.join(dir_estrategia, f"{nombre_estrategia}_{hoy}.csv")
    dh = logging.FileHandler(log_file_diario)
    dh.setFormatter(logging.Formatter("%(asctime)s,%(levelname)s,%(message)s"))

    logger.addHandler(fh)
    logger.addHandler(dh)

    return logger
