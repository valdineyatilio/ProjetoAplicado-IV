import pandas as pd
import numpy as np
import requests
from datetime import datetime

def fetch_inmet_climate(station_codes, start_date, end_date):
    """
    Coleta dados climáticos semanais de estações INMET.
    """
    all_data = []

    for station in station_codes:
        try:
            url = f"https://apitempo.inmet.gov.br/estacao/{start_date}/{end_date}/{station}"
            response = requests.get(url)
            if response.status_code != 200:
                continue

            raw = pd.DataFrame(response.json())
            raw["datahora"] = pd.to_datetime(raw["data"])
            raw["year"] = raw["datahora"].dt.isocalendar().year
            raw["week"] = raw["datahora"].dt.isocalendar().week
            raw["station"] = station

            weekly = raw.groupby(["year", "week", "station"], as_index=False).agg({
                "tmed": "mean",
                "prec": "sum"
            }).rename(columns={"tmed": "t_mean", "prec": "precip_sum"})

            all_data.append(weekly)

        except Exception as e:
            print(f" Erro ao coletar dados da estação {station}: {e}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

def fetch_ibge_population_density():
    """
    Coleta densidade populacional dos municípios via API SIDRA.
    """
    try:
        url = "https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/93/p/2022/c315/7169"
        df = pd.read_json(url)

        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
        df["municipio_id"] = df["localidade"].str.extract(r"(\d+)")
        df["pop_density"] = pd.to_numeric(df["valor"], errors="coerce")

        return df[["municipio_id", "pop_density"]]

    except Exception as e:
        print(f" Erro ao acessar dados do IBGE: {e}")
        return pd.DataFrame()
