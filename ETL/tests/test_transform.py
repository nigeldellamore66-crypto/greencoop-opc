import pandas as pd
from transform import transform_infoclimat

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