"""
===============================================================================
 Estrategia: Bollinger Breakout - v4 (parametrizada, con tuning integrado)
===============================================================================

Descripcion:
------------
Estrategia de ruptura basada en bandas de Bollinger adaptadas al volumen.
Genera senales BUY cuando el precio rompe la banda superior bajo ciertas condiciones.

Mejoras v4:
-----------
✅ Parametros de tuning integrados tras grid search exhaustivo (junio 2025)
✅ Filtros adicionales: volumen, cuerpo, ATR
✅ Modo debug para trazabilidad

Configuracion validada:
------------------------
- window = 20
- s = 2.5
- ajuste_volatilidad = False
- usar_filtro_cuerpo = True
- usar_filtro_volumen = True
- atr_threshold = 0.008
- vol_multiplier = 1.05

Requiere:
---------
- Columnas: 'fecha', 'close', 'high', 'low', 'volume'
- Librerias: pandas, ta

Salida:
--------
DataFrame con columnas: ['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("bollinger_breakout_v4")

def generar_senales(df: pd.DataFrame,
                    window: int = 20,
                    s: float = 2.5,
                    ajuste_volatilidad: bool = False,
                    usar_filtro_cuerpo: bool = True,
                    usar_filtro_volumen: bool = True,
                    atr_threshold: float = 0.008,
                    vol_multiplier: float = 1.05,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req = {"fecha", "close", "high", "low", "volume"}
        if not req.issubset(df.columns):
            logger.warning(f"Faltan columnas: {req - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < window:
            return df_as_hold(df, "datos insuficientes")

        # Indicadores
        std = df["close"].rolling(window).std()
        df["media"] = df["close"].rolling(window).mean()
        df["bb_up"] = df["media"] + s * std

        if ajuste_volatilidad:
            df["bb_up"] *= df["volume"] / df["volume"].rolling(window).mean()

        df["breakout"] = df["close"] > df["bb_up"]
        if usar_filtro_cuerpo:
            cuerpo = abs(df["close"] - df["open"])
            sombra = abs(df["high"] - df["low"])
            df["f_cuerpo"] = cuerpo / sombra > 0.5
            df["breakout"] &= df["f_cuerpo"]

        if usar_filtro_volumen:
            promedio_vol = df["volume"].rolling(window).mean()
            df["f_vol"] = df["volume"] > promedio_vol * vol_multiplier
            df["breakout"] &= df["f_vol"]

        df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
        df["atr_ratio"] = df["atr"] / df["close"]
        df["f_atr"] = df["atr_ratio"] > atr_threshold
        df["breakout"] &= df["f_atr"]

        df["signal"] = "hold"
        df.loc[df["breakout"], "signal"] = "buy"
        df["estrategia"] = "bollinger_breakout_v4"

        logger.info(f"Breakout v4 | BUY={df['signal'].eq('buy').sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["media", "bb_up", "breakout", "atr", "atr_ratio"]
            if usar_filtro_cuerpo:
                columnas.append("f_cuerpo")
            if usar_filtro_volumen:
                columnas.append("f_vol")
        return df[columnas]

    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return df_as_hold(df, "exception")

def df_as_hold(df: pd.DataFrame, razon: str) -> pd.DataFrame:
    logger.info(f"Retornando HOLD por: {razon}")
    df = df.copy()
    if "fecha" not in df.columns:
        return pd.DataFrame(columns=["fecha", "signal", "estrategia"])
    df["signal"] = "hold"
    df["estrategia"] = "bollinger_breakout_v4"
    return df[["fecha", "signal", "estrategia"]]
