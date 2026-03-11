import os
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

import pandas as pd
import requests

INPUT_FILE = "output.xlsx"
OUTPUT_FILE = "output_keepa.xlsx"
KEEPA_DOMAIN = 5  # Amazon.co.jp
KEEPA_EPOCH = datetime(2011, 1, 1, tzinfo=timezone.utc)

ADDED_COLUMNS = [
    "keepa_title",
    "keepa_monthlySold",
    "keepa_lastSoldUpdate",
    "keepa_salesRankDrops30",
    "keepa_coefficient",
    "estimated_monthly_sales",
    "estimate_source",
    "estimate_confidence",
    "estimate_note",
]


def normalize_asin(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def keepa_minutes_to_datetime_str(minutes: Any) -> str | None:
    if minutes is None:
        return None
    try:
        minutes_int = int(minutes)
    except (TypeError, ValueError):
        return None
    if minutes_int <= 0:
        return None
    dt = KEEPA_EPOCH + timedelta(minutes=minutes_int)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def fetch_keepa_product(asin: str, api_key: str, domain: int = KEEPA_DOMAIN) -> dict[str, Any]:
    url = "https://api.keepa.com/product"
    params = {
        "key": api_key,
        "domain": domain,
        "asin": asin,
        "stats": 180,
        "history": 0,
        "buybox": 0,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    products = payload.get("products") or []
    if not products:
        raise ValueError("Keepa response does not include product data")

    product = products[0]
    stats = product.get("stats") or {}

    return {
        "asin": product.get("asin") or asin,
        "title": product.get("title"),
        "monthlySold": product.get("monthlySold"),
        "lastSoldUpdate": keepa_minutes_to_datetime_str(product.get("lastSoldUpdate")),
        "salesRankDrops30": stats.get("salesRankDrops30"),
    }


def collect_keepa_data(asins: list[str], api_key: str) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    data: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}

    for asin in asins:
        try:
            data[asin] = fetch_keepa_product(asin=asin, api_key=api_key)
        except Exception as exc:  # noqa: BLE001 - individual ASIN failures should not stop processing
            errors[asin] = str(exc)

    return data, errors


def calculate_coefficient(keepa_data: dict[str, dict[str, Any]]) -> float:
    ratios: list[float] = []

    for item in keepa_data.values():
        monthly = item.get("monthlySold")
        drops30 = item.get("salesRankDrops30")
        if monthly is None or drops30 in (None, 0):
            continue
        try:
            monthly_f = float(monthly)
            drops30_f = float(drops30)
        except (TypeError, ValueError):
            continue
        if drops30_f <= 0:
            continue
        ratios.append(monthly_f / drops30_f)

    if not ratios:
        return 1.0
    return float(median(ratios))


def build_estimation(
    asin: str,
    keepa_info: dict[str, Any] | None,
    coefficient: float,
) -> dict[str, Any]:
    if not asin:
        return {
            "keepa_title": None,
            "keepa_monthlySold": None,
            "keepa_lastSoldUpdate": None,
            "keepa_salesRankDrops30": None,
            "keepa_coefficient": None,
            "estimated_monthly_sales": None,
            "estimate_source": "unavailable",
            "estimate_confidence": "D",
            "estimate_note": "ASINが空欄のためスキップ",
        }

    if keepa_info is None:
        return {
            "keepa_title": None,
            "keepa_monthlySold": None,
            "keepa_lastSoldUpdate": None,
            "keepa_salesRankDrops30": None,
            "keepa_coefficient": coefficient,
            "estimated_monthly_sales": None,
            "estimate_source": "unavailable",
            "estimate_confidence": "D",
            "estimate_note": "Keepa取得失敗",
        }

    monthly = keepa_info.get("monthlySold")
    drops30 = keepa_info.get("salesRankDrops30")

    if monthly is not None:
        try:
            estimated = int(round(float(monthly)))
        except (TypeError, ValueError):
            estimated = monthly
        source = "monthlySold"
        confidence = "A"
        note = "Keepa monthlySold を採用"
    elif drops30 is not None:
        try:
            estimated = int(round(float(drops30) * coefficient))
        except (TypeError, ValueError):
            estimated = None
        source = "salesRankDrops30_calibrated"
        confidence = "B"
        note = "salesRankDrops30 × 補正係数で推定"
    else:
        estimated = None
        source = "unavailable"
        confidence = "D"
        note = "monthlySold と salesRankDrops30 が取得不可"

    return {
        "keepa_title": keepa_info.get("title"),
        "keepa_monthlySold": monthly,
        "keepa_lastSoldUpdate": keepa_info.get("lastSoldUpdate"),
        "keepa_salesRankDrops30": drops30,
        "keepa_coefficient": coefficient,
        "estimated_monthly_sales": estimated,
        "estimate_source": source,
        "estimate_confidence": confidence,
        "estimate_note": note,
    }


def enrich_dataframe(df: pd.DataFrame, keepa_data: dict[str, dict[str, Any]], coefficient: float) -> pd.DataFrame:
    asins = df["ASIN"].apply(normalize_asin)
    rows = [build_estimation(asin, keepa_data.get(asin), coefficient) for asin in asins]
    enrich_df = pd.DataFrame(rows)

    for col in ADDED_COLUMNS:
        df[col] = enrich_df[col]

    return df


def print_summary(df: pd.DataFrame, keepa_data: dict[str, dict[str, Any]], errors: dict[str, str], coefficient: float) -> None:
    asin_series = df["ASIN"].apply(normalize_asin)
    non_empty_asins = asin_series[asin_series != ""]

    print("=== Keepa Enrich Summary ===")
    print(f"Input rows: {len(df)}")
    print(f"Rows with ASIN: {len(non_empty_asins)}")
    print(f"Unique ASIN requested: {non_empty_asins.nunique()}")
    print(f"Keepa fetch success: {len(keepa_data)}")
    print(f"Keepa fetch failed: {len(errors)}")
    print(f"Calibration coefficient: {coefficient:.6f}")

    source_counts = df["estimate_source"].value_counts(dropna=False).to_dict()
    print(f"Estimate source breakdown: {source_counts}")

    if errors:
        print("Failed ASIN examples:")
        for asin, message in list(errors.items())[:10]:
            print(f"- {asin}: {message}")


def main() -> None:
    api_key = os.getenv("KEEPA_API_KEY")
    if not api_key:
        raise RuntimeError("環境変数 KEEPA_API_KEY が設定されていません。")

    df = pd.read_excel(INPUT_FILE)
    if "ASIN" not in df.columns:
        raise ValueError("入力ファイルに ASIN 列が存在しません。")

    asins = (
        df["ASIN"]
        .apply(normalize_asin)
        .loc[lambda s: s != ""]
        .drop_duplicates()
        .tolist()
    )

    keepa_data, errors = collect_keepa_data(asins=asins, api_key=api_key)
    coefficient = calculate_coefficient(keepa_data)

    enriched = enrich_dataframe(df=df, keepa_data=keepa_data, coefficient=coefficient)
    enriched.to_excel(OUTPUT_FILE, index=False)

    print_summary(enriched, keepa_data, errors, coefficient)
    print(f"Saved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
