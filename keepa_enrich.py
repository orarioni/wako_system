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


class KeepaCommunicationError(Exception):
    """Raised for HTTP, timeout, requests, and JSON decode related failures."""


class KeepaProductNotFoundError(Exception):
    """Raised when Keepa returns no product for the requested ASIN."""


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


def is_monthly_sold_missing(value: Any) -> bool:
    """monthlySold missing criteria: null/NaN/0 (<=0 also treated as missing)."""
    num = safe_float(value)
    return num is None or num <= 0


def is_sales_rank_drops30_missing(value: Any) -> bool:
    """salesRankDrops30 missing criteria: null/NaN only (0 is not treated as missing)."""
    return safe_float(value) is None


def format_keepa_last_sold_update(raw_value: Any, asin: str, logger: logging.Logger) -> str | None:
    """Return YYYY-MM-DD HH:MM:SS text, or None for blank. Keep processing on failures."""
    if raw_value is None:
        return None

    # Keepa minute format
    if isinstance(raw_value, (int, float)):
        try:
            minutes_int = int(raw_value)
        except (TypeError, ValueError):
            logger.warning("ASIN=%s status=lastSoldUpdate_parse_error detail=invalid_numeric", asin)
            return None
        if minutes_int <= 0:
            return None
        dt = KEEPA_EPOCH + timedelta(minutes=minutes_int)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    # Already-formatted or date-like text
    text_value = str(raw_value).strip()
    if not text_value:
        return None
    try:
        dt = pd.to_datetime(text_value, utc=True)
    except Exception:  # noqa: BLE001
        logger.warning("ASIN=%s status=lastSoldUpdate_parse_error detail=unparseable_text", asin)
        return text_value
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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


def fetch_keepa_product(
    asin: str,
    api_key: str,
    timeout_sec: int,
    logger: logging.Logger,
    domain: int = DEFAULT_KEEPA_DOMAIN,
) -> dict[str, Any]:
    url = "https://api.keepa.com/product"
    params = {
        "key": api_key,
        "domain": domain,
        "asin": asin,
        "stats": 180,
        "history": 0,
        "buybox": 0,
    }

    try:
        response = requests.get(url, params=params, timeout=timeout_sec)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise KeepaCommunicationError(str(exc)) from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise KeepaCommunicationError(f"json_decode_error: {exc}") from exc

    products = payload.get("products") or []
    if not products:
        raise KeepaProductNotFoundError("products is empty")

    normalized_target = normalize_asin(asin).upper()
    matched = None
    for product in products:
        product_asin = normalize_asin(product.get("asin")).upper()
        if product_asin == normalized_target:
            matched = product
            break

    if matched is None:
        raise KeepaProductNotFoundError("requested ASIN not present in products")

    stats = matched.get("stats") or {}

    return {
        "asin": matched.get("asin") or asin,
        "title": matched.get("title"),
        "monthlySold": matched.get("monthlySold"),
        "lastSoldUpdate": format_keepa_last_sold_update(matched.get("lastSoldUpdate"), asin=asin, logger=logger),
        "salesRankDrops30": stats.get("salesRankDrops30"),
    }


def collect_keepa_data(
    asins: list[str],
    api_key: str,
    timeout_sec: int,
    logger: logging.Logger,
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    """Fetch Keepa data by unique ASIN. Keep processing even if a subset fails."""
    data: dict[str, dict[str, Any]] = {}
    metrics = {
        "communication_error_count": 0,
        "keepa_product_not_found_count": 0,
        "monthlySold_missing_count": 0,
        "salesRankDrops30_missing_count": 0,
    }

    for index, asin in enumerate(asins, start=1):
        try:
            item = fetch_keepa_product(asin=asin, api_key=api_key, timeout_sec=timeout_sec, logger=logger)
            data[asin] = item
            logger.info("[%s/%s] Keepa fetched: %s", index, len(asins), asin)

            if is_monthly_sold_missing(item.get("monthlySold")):
                metrics["monthlySold_missing_count"] += 1
                logger.info("ASIN=%s status=monthlySold_missing", asin)
            if is_sales_rank_drops30_missing(item.get("salesRankDrops30")):
                metrics["salesRankDrops30_missing_count"] += 1
                logger.info("ASIN=%s status=salesRankDrops30_missing", asin)

        except KeepaCommunicationError as exc:
            metrics["communication_error_count"] += 1
            logger.warning(
                "[%s/%s] ASIN=%s failure_type=communication_error detail=%s",
                index,
                len(asins),
                asin,
                str(exc),
            )
        except KeepaProductNotFoundError as exc:
            metrics["keepa_product_not_found_count"] += 1
            logger.warning(
                "[%s/%s] ASIN=%s failure_type=keepa_product_not_found detail=%s",
                index,
                len(asins),
                asin,
                str(exc),
            )
        except Exception as exc:  # noqa: BLE001
            metrics["communication_error_count"] += 1
            logger.warning(
                "[%s/%s] ASIN=%s failure_type=communication_error detail=unexpected_error:%s",
                index,
                len(asins),
                asin,
                str(exc),
            )

    return data, metrics


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
            "estimate_note": "ASIN missing",
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
            "estimate_note": "Keepa data unavailable",
        }

    monthly = keepa_info.get("monthlySold")
    drops30 = keepa_info.get("salesRankDrops30")

    monthly_f = safe_float(monthly)
    drops30_f = safe_float(drops30)

    if monthly_f is not None and monthly_f >= 1:
        estimated = int(round(monthly_f))
        source = "monthlySold"
        confidence = "A"
        note = "Keepa monthlySold used"
    elif drops30_f is not None and drops30_f >= 1:
        estimated = int(round(drops30_f * coefficient))
        source = "salesRankDrops30_calibrated"
        confidence = "B"
        note = "salesRankDrops30 calibrated by coefficient"
    else:
        estimated = None
        source = "unavailable"
        confidence = "D"
        note = "monthlySold and salesRankDrops30 unavailable"

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


def build_summary(df: pd.DataFrame, metrics: dict[str, int], coefficient: float) -> dict[str, Any]:
    asin_series = df["ASIN"].apply(normalize_asin)
    rows_with_missing_asin = int((asin_series == "").sum())
    rows_with_valid_asin = int((asin_series != "").sum())
    estimate_source_counts = df["estimate_source"].value_counts(dropna=False).to_dict()

    return {
        "total_input_rows": int(len(df)),
        "rows_with_missing_asin": rows_with_missing_asin,
        "rows_with_valid_asin": rows_with_valid_asin,
        "unique_valid_asins": int(asin_series[asin_series != ""].nunique()),
        "monthlySold_used_count": int(estimate_source_counts.get("monthlySold", 0)),
        "salesRankDrops30_calibrated_count": int(estimate_source_counts.get("salesRankDrops30_calibrated", 0)),
        "unavailable_count": int(estimate_source_counts.get("unavailable", 0)),
        "communication_error_count": int(metrics.get("communication_error_count", 0)),
        "keepa_product_not_found_count": int(metrics.get("keepa_product_not_found_count", 0)),
        "monthlySold_missing_count": int(metrics.get("monthlySold_missing_count", 0)),
        "salesRankDrops30_missing_count": int(metrics.get("salesRankDrops30_missing_count", 0)),
        "coefficient_value": float(coefficient),
    }


def log_and_print_summary(summary: dict[str, Any], logger: logging.Logger) -> None:
    logger.info("=== Keepa Enrich Summary ===")
    for key, value in summary.items():
        if key == "coefficient_value":
            logger.info("%s: %.6f", key, value)
        else:
            logger.info("%s: %s", key, value)


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

    # API request targets: unique + non-empty ASIN only.
    asins = df["ASIN"].apply(normalize_asin).loc[lambda s: s != ""].drop_duplicates().tolist()
    logger.info("Unique ASIN count for API fetch: %s", len(asins))

    keepa_data, metrics = collect_keepa_data(
        asins=asins,
        api_key=api_key,
        timeout_sec=settings["timeout_sec"],
        logger=logger,
    )
    coefficient = calculate_coefficient(keepa_data)

    # Output keeps all original rows, including missing-ASIN rows.
    enriched = enrich_dataframe(df=df, keepa_data=keepa_data, coefficient=coefficient)
    enriched.to_excel(settings["output_path"], index=False)

    summary = build_summary(enriched, metrics, coefficient)
    log_and_print_summary(summary, logger)
    logger.info("Saved output: %s", settings["output_path"])


if __name__ == "__main__":
    main()
