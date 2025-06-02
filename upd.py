import os
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
from pathlib import Path

# === CONFIGURACION ===
BUCKET_NAME = "bucket-name"
S3_CONFIG_PATH = "config/symbol_groups.json"
LOCAL_CONFIG_PATH = "/home/ubuntu/tr/config/symbol_groups.json"
S3_CSV_PATH = "data/historic"
LOCAL_PARQUET_PATH = "/home/ubuntu/tr/data/historic"
RECORTE_PARQUET_PATH = "/home/ubuntu/tr/data/historic_reciente"
LOG_DIR = "/home/ubuntu/tr/logs/ing"
LOG_FILE = f"{LOG_DIR}/upd_{datetime.now().date()}.csv"
NUM_DIAS = 60

# === CLIENTES AWS ===
s3 = boto3.client("s3")

# === LOGGING ===
def log_event(simbolo, status, mensaje, filas_agregadas):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"{ts},{simbolo},{status},{mensaje},{filas_agregadas}\n"
    with open(LOG_FILE, "a") as f:
        f.write(linea)
    print(linea.strip())

# === UTILIDADES ===
def convertir_fecha(df):
    if "datetime" in df.columns:
        df["fecha"] = pd.to_datetime(df["datetime"]).dt.date
        df.drop(columns=["datetime"], inplace=True)
    return df

def cargar_parquet_local(simbolo):
    path = Path(f"{LOCAL_PARQUET_PATH}/{simbolo}.parquet")
    if path.exists():
        return pd.read_parquet(path)
    else:
        return pd.DataFrame()

def guardar_parquet_local(simbolo, df):
    df = df.sort_values("fecha").drop_duplicates("fecha")
    df.to_parquet(f"{LOCAL_PARQUET_PATH}/{simbolo}.parquet", index=False)

def guardar_recorte(simbolo, df):
    df = df.sort_values("fecha").drop_duplicates("fecha").tail(NUM_DIAS)
    os.makedirs(RECORTE_PARQUET_PATH, exist_ok=True)
    df.to_parquet(f"{RECORTE_PARQUET_PATH}/{simbolo}.parquet", index=False)

# === PROCESAR SIMBOLO ===
def procesar_simbolo(simbolo):
    try:
        # Descargar .csv reciente desde S3
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=f"{S3_CSV_PATH}/{simbolo}.csv")
        df_csv = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
        df_csv = convertir_fecha(df_csv)

        if "fecha" not in df_csv.columns or df_csv.empty:
            log_event(simbolo, "ERROR", "CSV sin columna 'fecha' o vacio", 0)
            return

        df_parquet = cargar_parquet_local(simbolo)
        fechas_existentes = set(df_parquet["fecha"]) if not df_parquet.empty else set()

        # Filtrar solo fechas nuevas
        df_nuevo = df_csv[~df_csv["fecha"].isin(fechas_existentes)]

        if df_nuevo.empty:
            log_event(simbolo, "SKIP", "Sin fechas nuevas", 0)
            return

        # Merge y guardar historico completo
        df_combined = pd.concat([df_parquet, df_nuevo], ignore_index=True)
        df_combined = df_combined.sort_values("fecha").drop_duplicates("fecha")

        guardar_parquet_local(simbolo, df_combined)
        guardar_recorte(simbolo, df_combined)

        log_event(simbolo, "OK", "Actualizacion exitosa", len(df_nuevo))

    except Exception as e:
        log_event(simbolo, "ERROR", str(e), 0)

# === MAIN ===
def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=S3_CONFIG_PATH)
        simbolos_json = obj["Body"].read().decode("utf-8")

        with open(LOCAL_CONFIG_PATH, "w") as f:
            f.write(simbolos_json)

        grupos = pd.read_json(StringIO(simbolos_json))
        simbolos = sorted(set(sum(grupos.values.tolist(), [])))

        for simbolo in simbolos:
            procesar_simbolo(simbolo)

    except Exception as e:
        log_event("GLOBAL", "ERROR", f"No se pudo iniciar: {e}", 0)

if __name__ == "__main__":
    main()
