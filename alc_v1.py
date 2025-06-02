#!/usr/bin/env python3
# /home/ubuntu/tr/scripts/utils/alc_v1.py

import os
import sys
import json
import pandas as pd
import logging
import watchtower
from datetime import datetime
from collections import defaultdict

# === PATH DEL PROYECTO ===
sys.path.append("/home/ubuntu/tr")
BASE_DIR = "/home/ubuntu/tr"
from my_modules.email_sender import enviar_email

# === RUTAS ===
SENALES_DIR = f"{BASE_DIR}/reports/senales_heuristicas/diarias"
HISTORIC_DIR = f"{BASE_DIR}/data/historic"
LOG_DIR = f"{BASE_DIR}/logs/alerts"
SUMMARY_PATH = f"{BASE_DIR}/reports/summary/system_status.json"
DESTINATARIO = os.getenv("EMAIL_TRADING")
LOG_GROUP = "EC2AlertasSenales"
fecha_hoy = datetime.utcnow().strftime("%Y-%m-%d")

# === LOGGING ===
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"alertas_{fecha_hoy}.csv")
log_persistente = os.path.join(LOG_DIR, "alertas.log")

logger = logging.getLogger("AlertasSenales")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s,alertas,%(levelname)s,%(message)s", datefmt="%Y-%m-%d %H:%M:%S")

for handler_path in [log_file, log_persistente]:
    fh = logging.FileHandler(handler_path)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

cw_handler = watchtower.CloudWatchLogHandler(log_group=LOG_GROUP)
cw_handler.setFormatter(formatter)
logger.addHandler(cw_handler)

# === FUNCION DE ESTADO ===
def guardar_estado(modulo, status, mensaje):
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    status_obj = {}
    if os.path.exists(SUMMARY_PATH):
        with open(SUMMARY_PATH, "r") as f:
            status_obj = json.load(f)
    status_obj[modulo] = {
        "fecha": fecha_hoy,
        "ultima_ejecucion": now_str,
        "status": status,
        "mensaje": mensaje
    }
    with open(SUMMARY_PATH, "w") as f:
        json.dump(status_obj, f, indent=2)

# === PROCESAR Y AGRUPAR SENALES ===
senales_dict = defaultdict(lambda: {"buy": [], "sell": [], "close": "N/D"})

for archivo in os.listdir(SENALES_DIR):
    if not archivo.endswith(".csv"):
        continue
    try:
        ruta = os.path.join(SENALES_DIR, archivo)
        df = pd.read_csv(ruta)

        if "fecha" in df.columns and "signal" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
            fila = df[df["fecha"] == df["fecha"].max()]
            if fila.empty:
                continue
            for _, row in fila.iterrows():
                signal = row["signal"].lower()
                if signal not in ["buy", "sell"]:
                    continue

                symbol = row.get("simbolo", "UNKNOWN")
                estrategia = row.get("estrategia", "N/A")
                fecha = row["fecha"]

                if senales_dict[symbol]["close"] == "N/D":
                    ruta_hist = os.path.join(HISTORIC_DIR, f"{symbol}.parquet")
                    if os.path.exists(ruta_hist):
                        df_hist = pd.read_parquet(ruta_hist)
                        if "fecha" in df_hist.columns:
                            df_hist["fecha"] = pd.to_datetime(df_hist["fecha"]).dt.date
                            match = df_hist[df_hist["fecha"] == fecha]
                            if not match.empty and "close" in match.columns:
                                senales_dict[symbol]["close"] = round(match["close"].iloc[-1], 2)

                senales_dict[symbol][signal].append(estrategia)

    except Exception as e:
        logger.error(f"Error procesando {archivo}: {str(e)}")

# === FORMAR TABLA FINAL AGRUPADA ===
if senales_dict:
    df_final = pd.DataFrame([
        {
            "Simbolo": simbolo,
            "Cierre": data["close"],
            "Estrategias BUY": ", ".join(sorted(set(data["buy"]))),
            "Estrategias SELL": ", ".join(sorted(set(data["sell"])))
        }
        for simbolo, data in sorted(senales_dict.items())
        if data["buy"] or data["sell"]
    ])

    if not df_final.empty:
        conteo_total = df_final["Estrategias BUY"].apply(lambda x: len(x.split(",")) if x else 0).sum(), \
                       df_final["Estrategias SELL"].apply(lambda x: len(x.split(",")) if x else 0).sum()
        logger.info(f"Resumen de señales enviadas: BUY: {conteo_total[0]}, SELL: {conteo_total[1]}")

        tabla = df_final.to_html(index=False, border=0, justify="center", classes="tabla")
        html = f"""<html>
<head>
<style>
.tabla {{
    border-collapse: collapse;
    width: 100%;
    font-family: Arial, sans-serif;
}}
.tabla th, .tabla td {{
    border: 1px solid #cccccc;
    padding: 8px;
    text-align: center;
}}
.tabla th {{
    background-color: #f5f5f5;
    font-weight: bold;
    color: #333333;
}}
</style>
</head>
<body>
<h3 style="font-family:Arial;">{df_final.shape[0]} símbolos con señales heurísticas BUY/SELL ({fecha_hoy})</h3>
{tabla}
</body>
</html>
"""
        asunto = f"Senales heuristicas del dia - {fecha_hoy}"
        if DESTINATARIO:
            exito = enviar_email(asunto=asunto, cuerpo=html, destinatario=DESTINATARIO, html=True)
            if exito is True:
                logger.info("Correo enviado exitosamente.")
                guardar_estado("alertas", "OK", f"{df_final.shape[0]} simbolos enviados")
            else:
                logger.error(f"Fallo el envio del correo: {exito}")
                guardar_estado("alertas", "ERROR", "Fallo envio de correo")
        else:
            logger.error("EMAIL_TRADING no esta definido.")
            guardar_estado("alertas", "ERROR", "EMAIL_TRADING no definido")
    else:
        logger.info("No se encontraron señales BUY/SELL.")
        guardar_estado("alertas", "OK", "0 senales agrupadas")
else:
    logger.info("No se encontraron señales heuristicas.")
    guardar_estado("alertas", "OK", "0 senales encontradas")
