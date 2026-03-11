import configparser
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd
import requests

DEFAULT_KEEPA_DOMAIN = 5  # Amazon.co.jp
DEFAULT_TIMEOUT_SEC = 30
DEFAULT_INPUT_FILE = "output.xlsx"
DEFAULT_OUTPUT_FILE = "output_keepa.xlsx"
DEFAULT_LOG_FILE = "keepa_enrich.log"
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


def get_base_dir() -> Path:
    """Resolve the directory of script/executable for portable file handling."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def normalize_asin(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


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


def load_settings(base_dir: Path) -> dict[str, Any]:
    """Load config.ini values with safe defaults."""
    config = configparser.ConfigParser()
    config_path = base_dir / "config.ini"
    config.read(config_path, encoding="utf-8")

    settings = {
        "api_key": config.get("keepa", "api_key", fallback="").strip(),
        "input_excel": config.get("files", "input_excel", fallback=DEFAULT_INPUT_FILE).strip() or DEFAULT_INPUT_FILE,
        "output_excel": config.get("files", "output_excel", fallback=DEFAULT_OUTPUT_FILE).strip() or DEFAULT_OUTPUT_FILE,
        "log_file": config.get("app", "log_file", fallback=DEFAULT_LOG_FILE).strip() or DEFAULT_LOG_FILE,
        "timeout_sec": config.getint("app", "timeout_sec", fallback=DEFAULT_TIMEOUT_SEC),
    }

    settings["input_path"] = (base_dir / settings["input_excel"]).resolve()
    settings["output_path"] = (base_dir / settings["output_excel"]).resolve()
    settings["log_path"] = (base_dir / settings["log_file"]).resolve()
    settings["config_path"] = config_path
    return settings


def configure_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("keepa_enrich")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def fetch_keepa_product(asin: str, api_key: str, timeout_sec: int, domain: int = DEFAULT_KEEPA_DOMAIN) -> dict[str, Any]:
    url = "https://api.keepa.com/product"
    params = {
        "key": api_key,
        "domain": domain,
        "asin": asin,
        "stats": 180,
        "history": 0,
        "buybox": 0,
    }
    response = requests.get(url, params=params, timeout=timeout_sec)
    response.raise_for_status()
    payload = response.json()

    if payload.get("tokensLeft") is not None and payload.get("tokensLeft") <= 0:
        raise ValueError("Keepa API tokensLeft is 0")

    products = payload.get("products") or []
    if not products:
        raise ValueError("Keepa response has no product")

    product = products[0]
    stats = product.get("stats") or {}

    return {
        "asin": product.get("asin") or asin,
        "title": product.get("title"),
        "monthlySold": product.get("monthlySold"),
        "lastSoldUpdate": keepa_minutes_to_datetime_str(product.get("lastSoldUpdate")),
        "salesRankDrops30": stats.get("salesRankDrops30"),
    }


def collect_keepa_data(
    asins: list[str],
    api_key: str,
    timeout_sec: int,
    logger: logging.Logger,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Fetch Keepa data by unique ASIN. Keep processing even if a subset fails."""
    data: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}

    for index, asin in enumerate(asins, start=1):
        try:
            data[asin] = fetch_keepa_product(asin=asin, api_key=api_key, timeout_sec=timeout_sec)
            logger.info("[%s/%s] Keepa fetched: %s", index, len(asins), asin)
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            errors[asin] = message
            logger.warning("[%s/%s] Keepa fetch failed: %s (%s)", index, len(asins), asin, message)

    return data, errors


def calculate_coefficient(keepa_data: dict[str, dict[str, Any]]) -> float:
    """Median(monthlySold/salesRankDrops30) where both are >=1, fallback is 1.0."""
    ratios: list[float] = []

    for item in keepa_data.values():
        monthly = safe_float(item.get("monthlySold"))
        drops30 = safe_float(item.get("salesRankDrops30"))
        if monthly is None or drops30 is None:
            continue
        if monthly < 1 or drops30 < 1:
            continue
        ratios.append(monthly / drops30)

    if not ratios:
        return 1.0
    return float(median(ratios))


def build_estimation(asin: str, keepa_info: dict[str, Any] | None, coefficient: float) -> dict[str, Any]:
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

    monthly_f = safe_float(monthly)
    drops30_f = safe_float(drops30)

    if monthly_f is not None and monthly_f >= 1:
        estimated = int(round(monthly_f))
        source = "monthlySold"
        confidence = "A"
        note = "Keepa monthlySold を採用"
    elif drops30_f is not None and drops30_f >= 1:
        estimated = int(round(drops30_f * coefficient))
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


def log_and_print_summary(
    df: pd.DataFrame,
    keepa_data: dict[str, dict[str, Any]],
    errors: dict[str, str],
    coefficient: float,
    logger: logging.Logger,
) -> None:
    asin_series = df["ASIN"].apply(normalize_asin)
    non_empty_asins = asin_series[asin_series != ""]
    source_counts = df["estimate_source"].value_counts(dropna=False).to_dict()

    lines = [
        "=== Keepa Enrich Summary ===",
        f"Input rows: {len(df)}",
        f"Rows with ASIN: {len(non_empty_asins)}",
        f"Unique ASIN requested: {non_empty_asins.nunique()}",
        f"Keepa fetch success: {len(keepa_data)}",
        f"Keepa fetch failed: {len(errors)}",
        f"Calibration coefficient: {coefficient:.6f}",
        f"Estimate source breakdown: {source_counts}",
    ]

    for line in lines:
        logger.info(line)

    if errors:
        logger.warning("Failed ASIN list (up to 30):")
        for asin, message in list(errors.items())[:30]:
            logger.warning("- %s: %s", asin, message)


def main() -> None:
    base_dir = get_base_dir()
    settings = load_settings(base_dir)
    logger = configure_logging(settings["log_path"])

    api_key = settings["api_key"] or os.getenv("KEEPA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Keepa APIキーが見つかりません。config.ini の [keepa] api_key または環境変数 KEEPA_API_KEY を設定してください。")

    logger.info("Base directory: %s", base_dir)
    logger.info("Using config file: %s", settings["config_path"])
    logger.info("Input Excel: %s", settings["input_path"])
    logger.info("Output Excel: %s", settings["output_path"])

    if not settings["input_path"].exists():
        raise FileNotFoundError(f"入力ファイルが見つかりません: {settings['input_path']}")

    df = pd.read_excel(settings["input_path"])
    if "ASIN" not in df.columns:
        raise ValueError("入力ファイルに ASIN 列が存在しません。")

    asins = (
        df["ASIN"].apply(normalize_asin).loc[lambda s: s != ""].drop_duplicates().tolist()
    )
    logger.info("Unique ASIN count: %s", len(asins))

    keepa_data, errors = collect_keepa_data(
        asins=asins,
        api_key=api_key,
        timeout_sec=settings["timeout_sec"],
        logger=logger,
    )
    coefficient = calculate_coefficient(keepa_data)

    enriched = enrich_dataframe(df=df, keepa_data=keepa_data, coefficient=coefficient)
    enriched.to_excel(settings["output_path"], index=False)

    log_and_print_summary(enriched, keepa_data, errors, coefficient, logger)
    logger.info("Saved output: %s", settings["output_path"])


if __name__ == "__main__":
    main()
