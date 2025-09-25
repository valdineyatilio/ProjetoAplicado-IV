import pandas as pd
from datetime import datetime

def normalize_column_names(df):
    """
    Normaliza os nomes das colunas para minúsculas, sem espaços e com underscore.
    """
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    return df

def build_iso_date(year, week):
    """
    Constrói uma data ISO (YYYY-MM-DD) a partir do ano e número da semana.
    """
    return datetime.fromisocalendar(int(year), int(week), 1)

def impute_missing_values(df, method="mean"):
    """
    Imputa valores faltantes usando média, mediana ou interpolação.
    """
    if method == "mean":
        return df.fillna(df.mean())
    elif method == "median":
        return df.fillna(df.median())
    elif method == "interpolate":
        return df.interpolate()
    else:
        raise ValueError("Método inválido. Use 'mean', 'median' ou 'interpolate'.")

def create_lag_features(df, column, lags=[1, 2, 3, 4]):
    """
    Cria colunas defasadas (lags) para uma variável temporal.
    """
    for lag in lags:
        df[f"{column}_lag{lag}"] = df[column].shift(lag)
    return df

def create_rolling_features(df, column, window=4):
    """
    Cria média móvel para uma variável temporal.
    """
    df[f"{column}_roll{window}"] = df[column].rolling(window=window).mean()
    return df
