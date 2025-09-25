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
    e retorna agregados semanais. Estações com erro HTTP ou JSON inválido são ignoradas.
    """
    frames = []
    for st in station_ids:
        url = f"{base_url}/{st}/dados"
        try:
            resp = requests.get(url, params={"dataInicial": start, "dataFinal": end}, timeout=10)
        except requests.RequestException as e:
            print(f"[WARN] Falha na requisição da estação {st}: {e}. Pulando.")
            continue

        if resp.status_code != 200:
            print(f"[WARN] Estação {st} retornou status {resp.status_code}. Pulando.")
            continue

        try:
            raw_json = resp.json()
        except ValueError:
            print(f"[WARN] Resposta não é JSON para estação {st}. Pulando.")
            continue

        if not raw_json:
            print(f"[WARN] Sem dados na estação {st}. Pulando.")
            continue

        df = pd.DataFrame(raw_json)
        df["datahora"] = pd.to_datetime(df["datahora"])
        df.set_index("datahora", inplace=True)

        weekly = (
            df[["temp", "prec"]]
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
        raise RuntimeError("Nenhuma estação INMET retornou dados válidos.")

    return pd.concat(frames, ignore_index=True)


def fetch_ibge_population_density(
    municipio_ids: list[int],
    year: int,
    base_url: str = "https://servicodados.ibge.gov.br/api/v3/agregados/6579"
) -> pd.DataFrame:
    """
    Coleta densidade populacional por município no ano informado.
    Municípios com erro na API são ignorados.
    """
    records = []
    for m in municipio_ids:
        url = f"{base_url}/periodos/{year}/variaveis/9324?localidades=N3[{m}]"
        try:
            resp = requests.get(url, timeout=10)
        except requests.RequestException as e:
            print(f"[WARN] Falha na requisição IBGE para município {m}: {e}. Pulando.")
            continue

        if resp.status_code != 200:
            print(f"[WARN] IBGE retornou status {resp.status_code} para município {m}. Pulando.")
            continue

        try:
            data = resp.json()[0]["resultados"][0]["series"][0]["serie"]
            density = float(data.get(str(year), float("nan")))
        except (ValueError, KeyError, IndexError) as e:
            print(f"[WARN] JSON inesperado para município {m}: {e}. Pulando.")
            continue

        records.append({"municipio_id": m, "pop_density": density})

    if not records:
        raise RuntimeError("Nenhum município IBGE retornou dados válidos.")

    return pd.DataFrame(records)
