# A2/src/fetchers.py

import requests
import pandas as pd

def fetch_inmet_climate(
    station_ids: list[str],
    start: str,
    end: str,
    base_url: str = "https://apitempo.inmet.gov.br/estacao"
) -> pd.DataFrame:
    """
    Coleta dados horários de temperatura e precipitação das estações INMET
    e retorna agregados semanais (média de t_mean e soma de precip_sum).
    Em caso de erro 404 ou outro HTTPError, emite aviso e pula a estação.
    """
    frames = []
    for st in station_ids:
        try:
            resp = requests.get(
                f"{base_url}/{st}/dados",
                params={"dataInicial": start, "dataFinal": end},
                timeout=10
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            print(f"[WARN] Estação {st} retornou HTTPError: {e}. Pulando.")
            continue
        except requests.RequestException as e:
            print(f"[WARN] Erro de conexão na estação {st}: {e}. Pulando.")
            continue

        raw = pd.DataFrame(resp.json())
        if raw.empty:
            print(f"[WARN] Sem dados para estação {st}. Pulando.")
            continue

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
        frames.append(weekly)

    if not frames:
        raise RuntimeError("Nenhuma estação retornou dados válidos.")

    return pd.concat(frames, ignore_index=True)


def fetch_ibge_population_density(
    municipio_ids: list[int],
    year: int,
    base_url: str = "https://servicodados.ibge.gov.br/api/v3/agregados/6579"
) -> pd.DataFrame:
    records = []
    for m in municipio_ids:
        try:
            url = f"{base_url}/periodos/{year}/variaveis/9324?localidades=N3[{m}]"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[WARN] Problema ao buscar IBGE para município {m}: {e}. Pulando.")
            continue

        data = resp.json()[0]["resultados"][0]["series"][0]["serie"]
        density = float(data.get(str(year), float("nan")))
        records.append({"municipio_id": m, "pop_density": density})

    return pd.DataFrame(records)
