import pandas as pd
from transform import transform_infoclimat, transform_wu_station

def test_transform_infoclimat_keeps_expected_columns():
    df_raw = pd.DataFrame([{
        "id_station": "07015",
        "dh_utc": "2026-02-05T12:00:00Z",
        "temperature": 10.5,
        "humidite": 80,
        "pression": 1012,
        "pluie_1h": 0.4,
    }])

    df = transform_infoclimat(df_raw)

    assert "station_id" in df.columns
    assert "timestamp" in df.columns
    assert "temperature_c" in df.columns
    assert "humidity_pct" in df.columns
    assert "pressure_hpa" in df.columns
    assert "precip_accum_1h_mm" in df.columns
    assert len(df) == 1

def test_transform_infoclimat_drops_empty():
    df_raw = pd.DataFrame([{
        "id_station": "07015",
        "dh_utc": "2026-02-05T12:00:00Z",
        "temperature": 10.5,
        "humidite": 80,
        "pression": 1012,
        "pluie_1h": 0.4,
    },
    {
        "id_station": None,
        "dh_utc": None,
        "temperature":None,
        "humidite":None,
        "pression": None,
        "pluie_1h": None,
    }])

    df = transform_infoclimat(df_raw)

    assert len(df) == 1

def test_transform_infoclimat_drops_duplicates():
    df_raw = pd.DataFrame([{
        "id_station": "07015",
        "dh_utc": "2026-02-05T12:00:00Z",
        "temperature": 10.5,
        "humidite": 80,
        "pression": 1012,
        "pluie_1h": 0.4,
    },
    {
        "id_station": "07015",
        "dh_utc": "2026-02-05T12:00:00Z",
        "temperature": 10.5,
        "humidite": 80,
        "pression": 1012,
        "pluie_1h": 0.4,
    }])

    df = transform_infoclimat(df_raw)


    assert len(df) == 1

def test_transform_wu_temperature_f_to_c():
    df_raw = pd.DataFrame([{
        "Station_ID": "IICHTE19",
        "timestamp": "2026-02-05T12:00:00+00:00",
        "Temperature": "50.0 F",
    }])

    df = transform_wu_station(df_raw, station_id="IICHTE19")

    assert "temperature_c" in df.columns
    assert abs(df.loc[0, "temperature_c"] - 10.0) < 1e-6