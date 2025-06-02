import os
import json
import boto3
import requests
from datetime import datetime
from io import StringIO

API_KEY = os.getenv("TWELVE_API_KEY")
EMAIL_TRADING = os.getenv("EMAIL_TRADING")
BUCKET_NAME = os.getenv("BUCKET_NAME")
REGION = os.getenv("REGION")
LOG_FOLDER = "logs/ingestion"
CONFIG_GROUPS_KEY = "config/symbol_groups.json"
CURRENT_GROUP_KEY = "config/grupo_actual.json"

s3 = boto3.client("s3", region_name=REGION)
ses = boto3.client("ses", region_name=REGION)

def cargar_json_s3(key):
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def guardar_json_s3(data, key):
    body = json.dumps(data, indent=2)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body.encode("utf-8"))

def fetch_data(symbol):
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": 5,
        "apikey": API_KEY
    }
    response = requests.get("https://api.twelvedata.com/time_series", params=params)
    data = response.json()
    if "values" not in data:
        raise ValueError(f"Respuesta invalida para {symbol}: {data}")
    return data["values"]

def guardar_en_s3(symbol, values):
    csv_buffer = StringIO()
    headers = ["datetime", "open", "high", "low", "close", "volume"]
    csv_buffer.write(",".join(headers) + "\n")
    for entry in values:
        row = [entry.get(col, "") for col in headers]
        csv_buffer.write(",".join(row) + "\n")
    s3_key = f"data/historic/{symbol}.csv"
    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
    return s3_key

def escribir_log_s3(lineas):
    hoy = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    log_key = f"{LOG_FOLDER}/{hoy}.csv"
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=log_key)
        contenido = obj["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        contenido = "timestamp,proceso,estatus,mensaje\n"
    for linea in lineas:
        log_entry = f"{timestamp},ingest_TwelveData,{linea[0]},{linea[1]}"
        contenido += log_entry + "\n"
        print(log_entry)
    s3.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=contenido.encode("utf-8"))

def enviar_email(asunto, cuerpo):
    if not EMAIL_TRADING:
        return
    ses.send_email(
        Source=EMAIL_TRADING,
        Destination={"ToAddresses": [EMAIL_TRADING]},
        Message={
            "Subject": {"Data": asunto},
            "Body": {"Text": {"Data": cuerpo}}
        }
    )

def avanzar_grupo(grupo_actual, lista_grupos):
    idx = lista_grupos.index(grupo_actual)
    siguiente = lista_grupos[(idx + 1) % len(lista_grupos)]
    return siguiente

def lambda_handler(event, context):
    errores = []
    logs = []

    try:
        symbol_groups = cargar_json_s3(CONFIG_GROUPS_KEY)
        estado_grupo = cargar_json_s3(CURRENT_GROUP_KEY)
        grupo_actual = estado_grupo.get("grupo_actual")
        symbols = symbol_groups.get(grupo_actual)

        if not symbols:
            raise ValueError(f"Grupo '{grupo_actual}' no encontrado en symbol_groups.json")

        logs.append(("INFO", f"Inicio de ingesta para {grupo_actual} ({len(symbols)} simbolos)"))

        for symbol in symbols:
            try:
                values = fetch_data(symbol)
                fecha_max = values[0]["datetime"]
                guardar_en_s3(symbol, values)
                logs.append(("OK", f"{symbol} guardado - ultima fecha {fecha_max}"))
            except Exception as e:
                msg = f"{symbol} error: {str(e)}"
                logs.append(("ERROR", msg))
                errores.append(msg)

        # Avanza de grupo incluso si hubo errores
        nuevo_grupo = avanzar_grupo(grupo_actual, list(symbol_groups.keys()))
        estado_grupo["grupo_actual"] = nuevo_grupo
        estado_grupo["ultimo_update"] = datetime.utcnow().isoformat() + "Z"
        guardar_json_s3(estado_grupo, CURRENT_GROUP_KEY)
        logs.append(("INFO", f".json config actualizado a: {nuevo_grupo}"))

    except Exception as e:
        errores.append(f"Fallo global: {str(e)}")
        logs.append(("ERROR", str(e)))
        print(f"[ERROR] {str(e)}")

    escribir_log_s3(logs)

    if errores:
        asunto = f"[TRADING] Error en ingesta {grupo_actual}"
        cuerpo = "\n".join(errores)
        enviar_email(asunto, cuerpo)

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda ejecutada correctamente')
    }
