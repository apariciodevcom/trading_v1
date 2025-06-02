import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# === CONFIG ===
HIST_DIR = "/home/ubuntu/tr/data/historic_reciente"
OUTPUT_PATH = "/home/ubuntu/tr/data/features/features_dia.parquet"
LOG_PATH = f"/home/ubuntu/tr/logs/utils/fea_{datetime.now().date()}.log"
N_FEATURES = 8

# === FUNCIONES DE FEATURES ===
def calcular_rsi(series, window=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.rolling(window).mean()
    ma_down = down.rolling(window).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"{ts} | {msg}"
    print(linea)
    with open(LOG_PATH, "a") as f:
        f.write(linea + "\n")

# === MAIN ===
def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

    archivos = sorted(Path(HIST_DIR).glob("*.parquet"))
    filas = []

    for archivo in archivos:
        simbolo = archivo.stem.upper()
        try:
            df = pd.read_parquet(archivo)
            df = df[df["fecha"].notna()]
            df["fecha"] = pd.to_datetime(df["fecha"])
            df = df.sort_values("fecha")

            if len(df) < 60:
                log(f"SKIP {simbolo}: menos de 60 filas")
                continue

            df["ma_5"] = df["close"].rolling(5).mean()
            df["ma_20"] = df["close"].rolling(20).mean()
            df["rsi_14"] = calcular_rsi(df["close"], 14)
            df["pos_rango_60"] = (df["close"] - df["low"].rolling(60).min()) / (df["high"].rolling(60).max() - df["low"].rolling(60).min())
            df["volatilidad_20"] = df["close"].rolling(20).std()
            df["cambio_1d"] = df["close"].pct_change(1)
            df["cambio_3d"] = df["close"].pct_change(3)

            ultima = df.iloc[-1]

            fila = {
                "simbolo": simbolo,
                "fecha": ultima["fecha"].date(),
                "ma_5": ultima["ma_5"],
                "ma_20": ultima["ma_20"],
                "rsi_14": ultima["rsi_14"],
                "pos_rango_60": ultima["pos_rango_60"],
                "volatilidad_20": ultima["volatilidad_20"],
                "cambio_1d": ultima["cambio_1d"],
                "cambio_3d": ultima["cambio_3d"],
                "volume": ultima["volume"]
            }

            filas.append(fila)
            log(f"OK {simbolo}")

        except Exception as e:
            log(f"ERROR {simbolo}: {e}")

    if filas:
        df_final = pd.DataFrame(filas)
        df_final.to_parquet(OUTPUT_PATH, index=False)
        log(f"Archivo generado con {len(df_final)} simbolos y {N_FEATURES} features.")
    else:
        log("No se generaron datos.")

if __name__ == "__main__":
    main()
