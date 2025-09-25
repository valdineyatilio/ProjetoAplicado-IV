import pandas as pd
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler

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

def impute_missing_values(df, method="interpolate"):
    """
    Imputa valores faltantes nas colunas numéricas com segurança.
    """
    num_cols = df.select_dtypes(include=["number"]).columns
    df[num_cols] = df[num_cols].astype(float)

    if method == "mean":
        df[num_cols] = df[num_cols].fillna(df[num_cols].mean())
    elif method == "median":
        df[num_cols] = df[num_cols].fillna(df[num_cols].median())
    elif method == "interpolate":
        df[num_cols] = df[num_cols].interpolate(method="linear").ffill().bfill()
    else:
        raise ValueError("Método inválido. Use 'mean', 'median' ou 'interpolate'.")

    return df

def remove_outliers_zscore(df, columns, threshold=3):
    """
    Remove outliers com base no z-score para colunas especificadas.
    """
    for col in columns:
        if col in df.columns:
            z = (df[col] - df[col].mean()) / df[col].std()
            df = df[z.abs() <= threshold]
    return df

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

def normalize_numeric_columns(df, columns):
    """
    Aplica MinMaxScaler nas colunas numéricas especificadas.
    """
    scaler = MinMaxScaler()
    df[columns] = scaler.fit_transform(df[columns])
    return df
