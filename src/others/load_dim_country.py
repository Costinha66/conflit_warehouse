import pandas as pd


def normalize_cols(cols):
    dic = {
        "ISO3 Country code": "iso_country",
        "UNHCR Country code": "country_unhcr",
        "UNSD Name": "country_unsd",
        "UNSD short name (context Asylum)": "name_destination_short",
        "UNSD short name (context Origin)": "name_origin_short",
        "UNHCR Region name": "region_unhcr",
        "UNSD Region name": "region_unsd",
        "UNSD Sub-region name": "sub_region_unhcr",
        "SDG Region name": "sub_region_sdg",
    }
    return dic.get(cols, cols.strip().lower().replace(" ", "_"))


def build_country_dim(
    PATH_COUNTRY="/home/faacosta0245695/conflit/conflit_warehouse/data/raw/dimcountry.csv",
    PARQUET_COUNTRY="/home/faacosta0245695/conflit/conflit_warehouse/warehouse/dims/countries_dim.parquet",
):
    df = pd.read_csv(PATH_COUNTRY, sep=";", quotechar='"', encoding="utf-8")
    df.columns = [normalize_cols(col) for col in df.columns]
    df = df.map(lambda s: s.strip() if isinstance(s, str) else s)
    df["iso_country"] = df["iso_country"].str.upper()

    # DQ
    assert df["iso_country"].str.match(r"^[A-Z]{3}$").all()

    keep_cols = [
        "iso_country",
        "country_unhcr",
        "country_unsd",
        "region_unhcr",
        "sub_region_unhcr",
    ]
    country_df = df[keep_cols]
    country_df.to_parquet(PARQUET_COUNTRY, index=False)


if __name__ == "__main__":
    build_country_dim()
