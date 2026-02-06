import pandas as pd

def validate_raw(df, name, output=None):
    out = output if output else None

    print(f"\n--- VALIDATION RAW : {name} ---", file=out)

    print(f"\n--- {name} ---", file=out)
    print(profile_schema(df), file=out)


    print("\nMissing values:", file=out)
    print(missing_report(df).head(10), file=out)

    print("\nDuplicate full rows:", duplicate_rows_count(df), file=out)
    print("\nEmpty rows:", empty_rows_count(df), file=out)

    print("\nDistinct values per column:\n", constant_values(df), file=out)


def profile_schema(df):
    lines = [
        f"Rows: {len(df)}",
        f"Columns: {df.shape[1]}",
        "Column names and types:",
        "\n".join([f"{c}: {t}" for c, t in df.dtypes.items()])
    ]
    return "\n".join(lines)

def missing_report(df):
    n = len(df) if len(df) else 1
    miss = df.isna().sum()
    pct = (miss / n * 100).round(2)
    out = (
        pd.DataFrame({"missing": miss, "missing_pct": pct})
        .sort_values("missing", ascending=False)
    )
    return out

def duplicate_rows_count(df):
    return int(df.astype(str).duplicated().sum())

def empty_rows_count(df):
    return int((df.isna().sum(axis=1) == df.shape[1]).sum())

def constant_values(df):
    return df.astype(str).nunique(dropna=False).sort_values(ascending=False)
