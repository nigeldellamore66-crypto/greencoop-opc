import pandas as pd
from validate import missing_report,empty_rows_count,duplicate_rows_count

def test_missing_values_counts():
    df = pd.DataFrame({"a":[1,None], "b":[None,None]})
    out = missing_report(df)
    assert out.loc["a","missing"] == 1
    assert out.loc["b","missing"] == 2

def test_empty_rows_counts():
    df = pd.DataFrame([{
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

    result=empty_rows_count(df)

    assert result== 1

def test_duplicate_rows_count():
    df = pd.DataFrame([{
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

    result=duplicate_rows_count(df)

    assert result== 1