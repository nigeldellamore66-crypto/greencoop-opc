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


def transform_wu_station(df_raw: pd.DataFrame, station_id: str) -> pd.DataFrame:

    def _strip_units(s: pd.Series) -> pd.Series:
        # Enlève les unités + espaces, ....
        return (
            s.astype(str)
                .str.replace("\u00a0", " ", regex=False)       # espace insécable
                .str.replace(",", ".", regex=False)            # si jamais virgule décimale
                .str.replace(r"[^0-9\.\-]+", "", regex=True)   # garde chiffres / . / -
                .replace({"": None, "nan": None})
        )

    def _to_float(s: pd.Series) -> pd.Series:
        return pd.to_numeric(_strip_units(s), errors="coerce").round(2)

    def _to_int(s: pd.Series) -> pd.Series:
        return pd.to_numeric(_strip_units(s), errors="coerce").astype("Int64")

    def _build_timestamp(df: pd.DataFrame, start_date: str = "2024-10-01", time_col: str = "Time") -> pd.Series:

        # Convertis l'heure au format date_time 24h
        times = pd.to_datetime(df[time_col], format="%H:%M:%S", errors="coerce")

        # détection changement de jour
        day_shift = (times < times.shift()).cumsum().fillna(0)

        base_date = pd.to_datetime(start_date) 
        # incrémentation de la date du jour
        dates = base_date + pd.to_timedelta(day_shift, unit="D")

        # création du timestamp en concaténant date + heure
        ts = pd.to_datetime(
            dates.dt.strftime("%Y-%m-%d") + " " + times.dt.strftime("%H:%M:%S"),
            errors="coerce"
        )

        # Ajout de la timezone
        ts = ts.dt.tz_localize("Europe/Paris")   # heure locale station
        ts = ts.dt.tz_convert("UTC")             # format Infoclimat

        return ts
    
    def _wind_dir_to_deg(s: pd.Series) -> pd.Series:
        # Définition du mapping cardinalité>degrés
        mapping = {
            "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5,
            "E": 90.0, "ESE": 112.5, "SE": 135.0, "SSE": 157.5,
            "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
            "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
        }
        # normalise un peu (espaces, casse)
        clean = s.astype(str).str.strip().str.upper()
        # Map remplace toutes les valeurs définis dans le mapping
        return clean.map(mapping)
    
    # Début fonction de transformation

    df = df_raw.copy()

    # Drop les lignes vides
    df = df.dropna(how="all")

    # Création du timestamp à partir de l'heure:
    if "Time" in df.columns:
        df["timestamp"] = _build_timestamp(df)

    # Création d'une variable direction du vent en degrés à partir de la cardinalité
    if "Wind" in df.columns:
        df["wind_direction_deg"] = _wind_dir_to_deg(df["Wind"])

    # Cleaning et transformations des différentes colonnes (changements d'unités)
    if "Temperature" in df.columns:
        df["temperature_c"] = ((_to_float(df["Temperature"]) - 32) / 1.8).round(1)
    if "Dew Point" in df.columns:
        df["dew_point_c"] = ((_to_float(df["Dew Point"]) - 32) / 1.8).round(1)
    if "Humidity" in df.columns:
        df["humidity_pct"] = _to_int(df["Humidity"])
    if "Pressure" in df.columns:
        df["pressure_hpa"] = (_to_float(df["Pressure"]) * 33.8639).round(1)
    if "Speed" in df.columns:
        df["wind_speed_ms"] = (_to_float(df["Speed"]) * 0.44704).round(1)
    if "Gust" in df.columns:
        df["wind_gust_ms"] = (_to_float(df["Gust"]) * 0.44704).round(1)
    if "Precip. Accum." in df.columns:
        df["precip_accum_mm"] = (_to_float(df["Precip. Accum."]) * 25.4).round(1)
    if "UV" in df.columns:
        df["uv"] = df["UV"]
    if "Solar" in df.columns:
        df["solar_w_sqm"] = (_to_float(df["Solar"])).round(1) 

    # Ajout de l'id de la station
    df["station_id"] = station_id

    # Sélection finale des colonnes à conserver
    keep = [
        "station_id", "timestamp",
        "temperature_c", "dew_point_c", "humidity_pct", "pressure_hpa",
        "wind_speed_ms", "wind_gust_ms", "wind_direction_deg", "precip_accum_mm",
           "uv", "solar_w_sqm"
    ]
    keep = [c for c in keep if c in df.columns]

    return df[keep]

def transform_infoclimat(df_raw: pd.DataFrame) -> pd.DataFrame:
    
    # Début fonction de transformation

    df = df_raw.copy()

    # Drop les lignes vides
    df = df.dropna(how="all")

    # Cleaning et renommage des différentes colonnes 
    if "id_station" in df.columns:
        df["station_id"] = df["id_station"]
    if "timestamp_utc" in df.columns:
        df["timestamp"] = df["timestamp_utc"]
    if "temperature" in df.columns:
        df["temperature_c"] = df["temperature"]
    if "point_de_rosee" in df.columns:
        df["dew_point_c"] = df["point_de_rosee"]
    if "humidite" in df.columns:
        df["humidity_pct"] = df["humidite"]
    if "pression" in df.columns:
        df["pressure_hpa"] = df["pression"]
    if "vent_moyen" in df.columns:
        df["wind_speed_ms"] = df["vent_moyen"]
    if "vent_rafales" in df.columns:
        df["wind_gust_ms"] = df["vent_rafales"]
    if "vent_direction" in df.columns:
        df["wind_direction_deg"] = df["vent_direction"]
    if "visibilite" in df.columns:
        df["visibility_m"] = df["visibilite"]
    if "pluie_1h" in df.columns:
        df["precip_accum_1h_mm"] = df["pluie_1h"]


    # Sélection finale des colonnes à conserver
    keep = [
        "station_id", "timestamp",
        "temperature_c", "dew_point_c", "humidity_pct", "pressure_hpa",
        "wind_speed_ms", "wind_gust_ms", "wind_direction_deg", "precip_accum_1h_mm",
           "visibility_m"
    ]
    keep = [c for c in keep if c in df.columns]

    return df[keep]