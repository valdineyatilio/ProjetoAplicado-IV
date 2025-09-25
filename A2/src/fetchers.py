import requests
import pandas as pd

def fetch_inmet_climate(
    station_ids: list[str],
    start: str,
    end: str,
    base_url: str = "https://apitempo.inmet.gov.br/estacao"
) -> pd.DataFrame:
    dfs = []
    for st in station_ids:
        resp = requests.get(
            f"{base_url}/{st}/dados",
            params={"dataInicial": start, "dataFinal": end}
        )
        resp.raise_for_status()
        raw = pd.DataFrame(resp.json())
        raw["datahora"] = pd.to_datetime(raw["datahora"])
        raw.set_index("datahora", inplace=True)
        weekly = (
            raw[["temp", "prec"]]
            .rename(columns={"temp": "t_mean", "prec": "precip_sum"})
            .resample("W-MON")
            .agg({"t_mean": "mean", "precip_sum": "sum"})
            .reset_index()
        )
        weekly["station"] = st
        weekly["year"]    = weekly["datahora"].dt.isocalendar().year
        weekly["week"]    = weekly["datahora"].dt.isocalendar().week
        dfs.append(weekly)
    return pd.concat(dfs, ignore_index=True)

def fetch_ibge_population_density(
    municipio_ids: list[int],
    year: int,
    base_url: str = "https://servicodados.ibge.gov.br/api/v3/agregados/6579"
) -> pd.DataFrame:
    records = []
    for m in municipio_ids:
        url = f"{base_url}/periodos/{year}/variaveis/9324?localidades=N3[{m}]"
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()[0]["resultados"][0]["series"][0]["serie"]
        density = float(data[str(year)])
        records.append({"municipio_id": m, "pop_density": density})
    return pd.DataFrame(records)
