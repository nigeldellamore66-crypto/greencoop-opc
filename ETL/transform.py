import pandas as pd

def split_infoclimat(df_raw: pd.DataFrame):
    stations_list = df_raw.loc[0, "stations"] 
    df_meta = pd.DataFrame(stations_list)

    # Récupère chaque colonne de hourly qui représente une station
    hourly_cols = [c for c in df_raw.columns if c.startswith("hourly.") and c != "hourly._params"]

    # Ajoute le contenu des différentes colonnes dans un même Dataframe
    frames = []
    for col in hourly_cols:
        rows = df_raw.loc[0, col]  # liste de dicts pour cette station
        if rows:  # évite None / liste vide
            frames.append(pd.DataFrame(rows))

    df_data = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    return df_meta, df_data
