import pandas as pd
from validate import missing_report

def test_missing_values_counts():
    df = pd.DataFrame({"a":[1,None], "b":[None,None]})
    out = missing_report(df)
    assert out.loc["a","missing"] == 1
    assert out.loc["b","missing"] == 2
