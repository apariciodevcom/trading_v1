"""
===========================================================================
Estrategia: Gap Open Strategy - v5 (configuración óptima fija) - LeanTech Trading
===========================================================================

Descripción:
------------
Versión final con parámetros de configuración optimizados mediante grid search.

Mejor combinación validada:
---------------------------
- umbral_gap = 0.04
- gap_min_abs_pct = 0.015
- usar_confirmacion_cuerpo = False

Requiere:
---------
- Columnas: ['fecha', 'open', 'high', 'low', 'close']

Salida:
--------
DataFrame con ['fecha', 'signal', 'estrategia']
"""

import pandas as pd
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("gap_open_strategy_v5")

def generar_senales(df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        columnas_req = {"fecha", "open", "close", "high", "low"}
        if not columnas_req.issubset(df.columns):
            logger.warning(f"Faltan columnas: {columnas_req - set(df.columns)}")
            return df_as_hold(df, razon="faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < 10:
            return df_as_hold(df, razon="datos insuficientes")

        # Configuración fija óptima
        umbral_gap = 0.04
        gap_min_abs_pct = 0.015
        usar_confirmacion_cuerpo = False

        # Cálculo del gap
        df["close_prev"] = df["close"].shift(1)
        df["gap"] = (df["open"] - df["close_prev"]) / df["close_prev"]
        df["gap_abs"] = df["gap"].abs()

        # Gaps
        df["gap_alcista"] = df["gap"] > umbral_gap
        df["gap_bajista"] = df["gap"] < -umbral_gap
        df["gap_suficiente"] = df["gap_abs"] >= gap_min_abs_pct

        # Confirmación por cuerpo (desactivado en v5)
        df["cuerpo_negativo"] = True
        df["cuerpo_positivo"] = True

        # Condiciones
        df["cond_sell"] = df["gap_alcista"] & df["gap_suficiente"] & df["cuerpo_negativo"]
        df["cond_buy"] = df["gap_bajista"] & df["gap_suficiente"] & df["cuerpo_positivo"]

        # Señales
        df["signal"] = "hold"
        df.loc[df["cond_buy"], "signal"] = "buy"
        df.loc[df["cond_sell"], "signal"] = "sell"
        df["estrategia"] = "gap_open_strategy_v5"

        logger.info(f"GapOpen v5 | BUY={df['signal'].eq('buy').sum()} | SELL={df['signal'].eq('sell').sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += [
                "open", "close", "close_prev", "gap", "gap_abs",
                "gap_alcista", "gap_bajista", "gap_suficiente",
                "cuerpo_negativo", "cuerpo_positivo", "cond_buy", "cond_sell"
            ]

        return df[columnas]

    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return df_as_hold(df, razon="exception")

def df_as_hold(df: pd.DataFrame, razon: str) -> pd.DataFrame:
    logger.info(f"Retornando HOLD por: {razon}")
    df = df.copy()
    if "fecha" not in df.columns:
        return pd.DataFrame(columns=["fecha", "signal", "estrategia"])
    df["signal"] = "hold"
    df["estrategia"] = "gap_open_strategy_v5"
    return df[["fecha", "signal", "estrategia"]]
