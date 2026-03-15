import argparse
import configparser
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd
import requests

from update_queue import (
    DEFAULT_REFRESH_POLICY,
    compute_next_fetch_after,
    decide_fetch_queue,
    load_cache,
    merge_cache_records,
    save_cache,
)

DEFAULT_KEEPA_DOMAIN = 5  # Amazon.co.jp
DEFAULT_TIMEOUT_SEC = 30
DEFAULT_INPUT_FILE = "output.xlsx"
DEFAULT_OUTPUT_FILE = "output_keepa.xlsx"
DEFAULT_LOG_FILE = "keepa_enrich.log"
DEFAULT_CACHE_FILE = "asin_cache.csv"
DEFAULT_MODE = "single"
DEFAULT_RESERVE_TOKENS = 10
DEFAULT_TOKENS_PER_MINUTE = 5.0
DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_MAX_MINUTES = 480
DEFAULT_STOP_WHEN_TOKENS_BELOW = 10
DEFAULT_MAX_ZERO_BUDGET_CYCLES = 3
DEFAULT_MAX_TOKEN_STATUS_FAILURES = 3
DEFAULT_MAX_BATCH_SIZE = 100
TOKEN_COST_PER_ASIN = 1
KEEPA_EPOCH = datetime(2011, 1, 1, tzinfo=timezone.utc)
TOKYO_TZ = timezone(timedelta(hours=9))

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


class SafeStreamHandler(logging.StreamHandler):
    """Stream handler that tolerates broken console streams on Windows/PowerShell."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except (OSError, ValueError):
            # Keep business processing alive even if console output is broken.
            return

    def flush(self) -> None:
        try:
            super().flush()
        except (OSError, ValueError):
            # Keep business processing alive even if console output is broken.
            return


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
        dt = pd.to_datetime(text_value)
    except Exception:  # noqa: BLE001
        logger.warning("ASIN=%s status=lastSoldUpdate_parse_error detail=unparseable_text", asin)
        return text_value
    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.tz_convert(TOKYO_TZ).tz_localize(None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fetch_keepa_products_batch(
    asins: list[str],
    api_key: str,
    timeout_sec: int,
    domain: int = DEFAULT_KEEPA_DOMAIN,
) -> list[dict[str, Any]]:
    url = "https://api.keepa.com/product"
    params = {
        "key": api_key,
        "domain": domain,
        "asin": ",".join(asins),
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
    if not isinstance(products, list):
        raise KeepaCommunicationError("products is not list")
    return products


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
        "cache_file": config.get("files", "asin_cache", fallback=DEFAULT_CACHE_FILE).strip() or DEFAULT_CACHE_FILE,
        "timeout_sec": config.getint("app", "timeout_sec", fallback=DEFAULT_TIMEOUT_SEC),
        "default_mode": config.get("run", "default_mode", fallback=DEFAULT_MODE).strip() or DEFAULT_MODE,
        "reserve_tokens": config.getint("run", "reserve_tokens", fallback=DEFAULT_RESERVE_TOKENS),
        "tokens_per_minute": config.getfloat("run", "tokens_per_minute", fallback=DEFAULT_TOKENS_PER_MINUTE),
        "interval_seconds": config.getint("run", "interval_seconds", fallback=DEFAULT_INTERVAL_SECONDS),
        "max_minutes": config.getint("run", "max_minutes", fallback=DEFAULT_MAX_MINUTES),
        "stop_when_tokens_below": config.getint("run", "stop_when_tokens_below", fallback=DEFAULT_STOP_WHEN_TOKENS_BELOW),
        "max_zero_budget_cycles": config.getint("run", "max_zero_budget_cycles", fallback=DEFAULT_MAX_ZERO_BUDGET_CYCLES),
        "max_token_status_failures": config.getint("run", "max_token_status_failures", fallback=DEFAULT_MAX_TOKEN_STATUS_FAILURES),
        "refresh_policy": {
            "communication_error_minutes": config.getint(
                "refresh_policy",
                "communication_error_minutes",
                fallback=DEFAULT_REFRESH_POLICY["communication_error_minutes"],
            ),
            "keepa_product_not_found_days": config.getint(
                "refresh_policy",
                "keepa_product_not_found_days",
                fallback=DEFAULT_REFRESH_POLICY["keepa_product_not_found_days"],
            ),
            "monthly_sold_present_days": config.getint(
                "refresh_policy",
                "monthly_sold_present_days",
                fallback=DEFAULT_REFRESH_POLICY["monthly_sold_present_days"],
            ),
            "sales_rank_only_days": config.getint(
                "refresh_policy",
                "sales_rank_only_days",
                fallback=DEFAULT_REFRESH_POLICY["sales_rank_only_days"],
            ),
            "both_missing_days": config.getint(
                "refresh_policy",
                "both_missing_days",
                fallback=DEFAULT_REFRESH_POLICY["both_missing_days"],
            ),
            "other_failure_days": config.getint(
                "refresh_policy",
                "other_failure_days",
                fallback=DEFAULT_REFRESH_POLICY["other_failure_days"],
            ),
        },
    }

    settings["input_path"] = (base_dir / settings["input_excel"]).resolve()
    settings["output_path"] = (base_dir / settings["output_excel"]).resolve()
    settings["log_path"] = (base_dir / settings["log_file"]).resolve()
    settings["config_path"] = config_path
    settings["cache_path"] = (base_dir / settings["cache_file"]).resolve()
    return settings


def configure_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("keepa_enrich")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    try:
        stream_handler = SafeStreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    except Exception:  # noqa: BLE001
        # File logging remains active even if console handler setup fails.
        pass

    return logger


def fetch_keepa_product(
    asin: str,
    api_key: str,
    timeout_sec: int,
    logger: logging.Logger,
    domain: int = DEFAULT_KEEPA_DOMAIN,
) -> dict[str, Any]:
    products = fetch_keepa_products_batch(
        asins=[asin],
        api_key=api_key,
        timeout_sec=timeout_sec,
        domain=domain,
    )
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


def normalize_product_for_asin(product: dict[str, Any], asin: str, logger: logging.Logger) -> dict[str, Any]:
    stats = product.get("stats") or {}
    return {
        "asin": product.get("asin") or asin,
        "title": product.get("title"),
        "monthlySold": product.get("monthlySold"),
        "lastSoldUpdate": format_keepa_last_sold_update(product.get("lastSoldUpdate"), asin=asin, logger=logger),
        "salesRankDrops30": stats.get("salesRankDrops30"),
    }


def collect_keepa_data(
    asins: list[str],
    api_key: str,
    timeout_sec: int,
    logger: logging.Logger,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Fetch Keepa data by unique ASIN. Keep processing even if a subset fails."""
    data: dict[str, dict[str, Any]] = {}
    metrics = {
        "communication_error_count": 0,
        "keepa_product_not_found_count": 0,
        "monthlySold_missing_count": 0,
        "salesRankDrops30_missing_count": 0,
        "failure_by_asin": {},
    }

    for batch_start in range(0, len(asins), DEFAULT_MAX_BATCH_SIZE):
        batch_asins = asins[batch_start : batch_start + DEFAULT_MAX_BATCH_SIZE]
        try:
            products = fetch_keepa_products_batch(
                asins=batch_asins,
                api_key=api_key,
                timeout_sec=timeout_sec,
            )
            products_by_asin = {normalize_asin(p.get("asin")).upper(): p for p in products}

            for asin in batch_asins:
                normalized = normalize_asin(asin).upper()
                product = products_by_asin.get(normalized)
                if product is None:
                    metrics["keepa_product_not_found_count"] += 1
                    metrics["failure_by_asin"][asin] = "keepa_product_not_found"
                    logger.warning("ASIN=%s failure_type=keepa_product_not_found detail=requested ASIN not present in products", asin)
                    continue

                item = normalize_product_for_asin(product=product, asin=asin, logger=logger)
                data[asin] = item
                logger.info("Keepa fetched: %s", asin)

                if is_monthly_sold_missing(item.get("monthlySold")):
                    metrics["monthlySold_missing_count"] += 1
                    logger.info("ASIN=%s status=monthlySold_missing", asin)
                if is_sales_rank_drops30_missing(item.get("salesRankDrops30")):
                    metrics["salesRankDrops30_missing_count"] += 1
                    logger.info("ASIN=%s status=salesRankDrops30_missing", asin)

        except KeepaCommunicationError as exc:
            for asin in batch_asins:
                metrics["communication_error_count"] += 1
                metrics["failure_by_asin"][asin] = "communication_error"
                logger.warning("ASIN=%s failure_type=communication_error detail=%s", asin, str(exc))
        except KeepaProductNotFoundError as exc:
            for asin in batch_asins:
                metrics["keepa_product_not_found_count"] += 1
                metrics["failure_by_asin"][asin] = "keepa_product_not_found"
                logger.warning("ASIN=%s failure_type=keepa_product_not_found detail=%s", asin, str(exc))
        except Exception as exc:  # noqa: BLE001
            for asin in batch_asins:
                metrics["communication_error_count"] += 1
                metrics["failure_by_asin"][asin] = "communication_error"
                logger.warning("ASIN=%s failure_type=communication_error detail=unexpected_error:%s", asin, str(exc))

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


def keepa_row_from_cache(cache_row: pd.Series | None) -> dict[str, Any] | None:
    if cache_row is None:
        return None
    return {
        "title": cache_row.get("keepa_title"),
        "monthlySold": cache_row.get("keepa_monthlySold"),
        "lastSoldUpdate": cache_row.get("keepa_lastSoldUpdate"),
        "salesRankDrops30": cache_row.get("keepa_salesRankDrops30"),
    }


def build_keepa_data_from_cache(valid_asins: list[str], cache_df: pd.DataFrame) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    keepa_data: dict[str, dict[str, Any]] = {}
    cache_hit = 0
    cache_idx = cache_df.set_index("asin", drop=False) if not cache_df.empty else pd.DataFrame(columns=["asin"]).set_index("asin", drop=False)

    for asin in valid_asins:
        if asin in cache_idx.index:
            keepa_info = keepa_row_from_cache(cache_idx.loc[asin])
            if keepa_info is not None:
                keepa_data[asin] = keepa_info
                cache_hit += 1

    return keepa_data, {"cache_hit_count": cache_hit, "cache_miss_count": len(valid_asins) - cache_hit}


def build_cache_updates(
    valid_asins: list[str],
    rows_seen: dict[str, int],
    fetched_keepa_data: dict[str, dict[str, Any]],
    fetch_metrics: dict[str, Any],
    existing_cache: pd.DataFrame,
    coefficient: float,
    now: datetime,
    attempted_asins: set[str],
    refresh_policy: dict[str, int],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    cache_idx = existing_cache.set_index("asin", drop=False) if not existing_cache.empty else pd.DataFrame(columns=["asin"]).set_index("asin", drop=False)
    failure_by_asin = fetch_metrics.get("failure_by_asin", {})
    updates: list[dict[str, Any]] = []

    fetched_success_count = 0
    fetched_failure_count = 0

    for asin in valid_asins:
        old = cache_idx.loc[asin] if asin in cache_idx.index else None
        attempted = asin in attempted_asins
        failure_type = failure_by_asin.get(asin)

        if asin in fetched_keepa_data:
            keepa_info = fetched_keepa_data[asin]
            est = build_estimation(asin, keepa_info, coefficient)
            fetched_success_count += 1
            consecutive_failures = 0
            last_fetched_at = now.strftime("%Y-%m-%d %H:%M:%S")
            last_success_at = now.strftime("%Y-%m-%d %H:%M:%S")
            last_failure_at = old.get("last_failure_at") if old is not None else None
            next_fetch_after = compute_next_fetch_after(
                now=now,
                failure_type=None,
                monthly_sold=est["keepa_monthlySold"],
                drops30=est["keepa_salesRankDrops30"],
                refresh_policy=refresh_policy,
            )
        elif attempted and failure_type:
            fetched_failure_count += 1
            if old is not None:
                keepa_info = keepa_row_from_cache(old)
                est = build_estimation(asin, keepa_info, coefficient)
            else:
                est = build_estimation(asin, None, coefficient)
            previous_failures = int(safe_float(old.get("consecutive_failures")) or 0) if old is not None else 0
            consecutive_failures = previous_failures + 1
            last_fetched_at = now.strftime("%Y-%m-%d %H:%M:%S")
            last_success_at = old.get("last_success_at") if old is not None else None
            last_failure_at = now.strftime("%Y-%m-%d %H:%M:%S")
            next_fetch_after = compute_next_fetch_after(
                now=now,
                failure_type=failure_type,
                monthly_sold=est["keepa_monthlySold"],
                drops30=est["keepa_salesRankDrops30"],
                refresh_policy=refresh_policy,
            )
        else:
            if old is None:
                continue
            keepa_info = keepa_row_from_cache(old)
            est = build_estimation(asin, keepa_info, coefficient)
            failure_type = old.get("failure_type")
            consecutive_failures = int(safe_float(old.get("consecutive_failures")) or 0)
            last_fetched_at = old.get("last_fetched_at")
            last_success_at = old.get("last_success_at")
            last_failure_at = old.get("last_failure_at")
            next_fetch_after_text = old.get("next_fetch_after")
            next_fetch_after = pd.to_datetime(next_fetch_after_text).to_pydatetime() if next_fetch_after_text else compute_next_fetch_after(
                now=now,
                failure_type=failure_type,
                monthly_sold=est["keepa_monthlySold"],
                drops30=est["keepa_salesRankDrops30"],
                refresh_policy=refresh_policy,
            )

        updates.append(
            {
                "asin": asin,
                "last_fetched_at": last_fetched_at,
                "last_success_at": last_success_at,
                "last_failure_at": last_failure_at,
                "keepa_title": est["keepa_title"],
                "keepa_lastSoldUpdate": est["keepa_lastSoldUpdate"],
                "keepa_monthlySold": est["keepa_monthlySold"],
                "keepa_salesRankDrops30": est["keepa_salesRankDrops30"],
                "estimate_source": est["estimate_source"],
                "estimate_confidence": est["estimate_confidence"],
                "estimate_note": est["estimate_note"],
                "failure_type": failure_type,
                "rows_seen_in_input": rows_seen.get(asin, 0),
                "fetch_priority": None,
                "next_fetch_after": next_fetch_after.strftime("%Y-%m-%d %H:%M:%S"),
                "consecutive_failures": consecutive_failures,
            }
        )

    return updates, {
        "fetched_success_count": fetched_success_count,
        "fetched_failure_count": fetched_failure_count,
    }


def build_summary(df: pd.DataFrame, metrics: dict[str, Any], coefficient: float) -> dict[str, Any]:
    asin_series = df["ASIN"].apply(normalize_asin)
    rows_with_missing_asin = int((asin_series == "").sum())
    rows_with_valid_asin = int((asin_series != "").sum())
    estimate_source_counts = df["estimate_source"].value_counts(dropna=False).to_dict()

    return {
        "total_input_rows": int(len(df)),
        "rows_with_missing_asin": rows_with_missing_asin,
        "rows_with_valid_asin": rows_with_valid_asin,
        "unique_valid_asins": int(asin_series[asin_series != ""].nunique()),
        "queued_for_fetch_count": int(metrics.get("queued_for_fetch_count", 0)),
        "skipped_by_cache_count": int(metrics.get("skipped_by_cache_count", 0)),
        "fetched_success_count": int(metrics.get("fetched_success_count", 0)),
        "fetched_failure_count": int(metrics.get("fetched_failure_count", 0)),
        "cache_hit_count": int(metrics.get("cache_hit_count", 0)),
        "cache_miss_count": int(metrics.get("cache_miss_count", 0)),
        "queue_priority_high_count": int(metrics.get("queue_priority_high_count", 0)),
        "queue_priority_medium_count": int(metrics.get("queue_priority_medium_count", 0)),
        "queue_priority_low_count": int(metrics.get("queue_priority_low_count", 0)),
        "monthlySold_used_count": int(estimate_source_counts.get("monthlySold", 0)),
        "salesRankDrops30_calibrated_count": int(estimate_source_counts.get("salesRankDrops30_calibrated", 0)),
        "unavailable_count": int(estimate_source_counts.get("unavailable", 0)),
        "communication_error_count": int(metrics.get("communication_error_count", 0)),
        "keepa_product_not_found_count": int(metrics.get("keepa_product_not_found_count", 0)),
        "monthlySold_missing_count": int(metrics.get("monthlySold_missing_count", 0)),
        "salesRankDrops30_missing_count": int(metrics.get("salesRankDrops30_missing_count", 0)),
        "mode": metrics.get("mode", "single"),
        "available_tokens_at_start": int(metrics.get("available_tokens_at_start", 0)),
        "reserve_tokens": int(metrics.get("reserve_tokens", 0)),
        "total_queue_count": int(metrics.get("total_queue_count", 0)),
        "selected_fetch_count": int(metrics.get("selected_fetch_count", 0)),
        "remaining_queue_count": int(metrics.get("remaining_queue_count", 0)),
        "cycle_count": int(metrics.get("cycle_count", 0)),
        "total_sleep_seconds": int(metrics.get("total_sleep_seconds", 0)),
        "run_duration_seconds": int(metrics.get("run_duration_seconds", 0)),
        "effective_tokens_per_minute": float(metrics.get("effective_tokens_per_minute", 0.0)),
        "max_minutes_reached": bool(metrics.get("max_minutes_reached", False)),
        "queue_exhausted": bool(metrics.get("queue_exhausted", False)),
        "stop_reason": metrics.get("stop_reason", ""),
        "stop_when_tokens_below": int(metrics.get("stop_when_tokens_below", 0)),
        "max_zero_budget_cycles": int(metrics.get("max_zero_budget_cycles", 0)),
        "max_token_status_failures": int(metrics.get("max_token_status_failures", 0)),
        "zero_budget_cycles": int(metrics.get("zero_budget_cycles", 0)),
        "token_status_failures": int(metrics.get("token_status_failures", 0)),
        "last_available_tokens": int(metrics.get("last_available_tokens", 0)),
        "coefficient_value": float(coefficient),
    }


def log_and_print_summary(summary: dict[str, Any], logger: logging.Logger) -> None:
    logger.info("=== Keepa Enrich Summary ===")
    for key, value in summary.items():
        if key == "coefficient_value":
            logger.info("%s: %.6f", key, value)
        else:
            logger.info("%s: %s", key, value)


def parse_args(settings: dict[str, Any]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Keepa monthly sales enrichment")
    parser.add_argument("--mode", choices=["single", "burst", "drip"], default=settings["default_mode"])
    parser.add_argument("--reserve-tokens", type=int, default=settings["reserve_tokens"])
    parser.add_argument("--tokens-per-minute", type=float, default=settings["tokens_per_minute"])
    parser.add_argument("--interval-seconds", type=int, default=settings["interval_seconds"])
    parser.add_argument("--max-minutes", type=int, default=settings["max_minutes"])
    parser.add_argument("--max-fetches", type=int, default=None)
    parser.add_argument("--stop-when-tokens-below", type=int, default=settings["stop_when_tokens_below"])
    parser.add_argument("--max-zero-budget-cycles", type=int, default=settings["max_zero_budget_cycles"])
    parser.add_argument("--max-token-status-failures", type=int, default=settings["max_token_status_failures"])
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def get_token_status(api_key: str, timeout_sec: int, logger: logging.Logger) -> int:
    url = "https://api.keepa.com/token"
    try:
        response = requests.get(url, params={"key": api_key}, timeout=timeout_sec)
        response.raise_for_status()
        payload = response.json()
        tokens = payload.get("tokensLeft")
        if tokens is None:
            tokens = payload.get("tokensleft")
        if tokens is None:
            logger.warning("token_status_unavailable detail=missing_tokens_field")
            return 0
        return max(0, int(tokens))
    except Exception as exc:  # noqa: BLE001
        logger.warning("token_status_unavailable detail=%s", str(exc))
        return 0


def get_token_status_safe(api_key: str, timeout_sec: int, logger: logging.Logger) -> tuple[int | None, bool]:
    url = "https://api.keepa.com/token"
    try:
        response = requests.get(url, params={"key": api_key}, timeout=timeout_sec)
        response.raise_for_status()
        payload = response.json()
        tokens = payload.get("tokensLeft")
        if tokens is None:
            tokens = payload.get("tokensleft")
        if tokens is None:
            logger.warning("token_status_unavailable detail=missing_tokens_field")
            return None, False
        return max(0, int(tokens)), True
    except Exception as exc:  # noqa: BLE001
        logger.warning("token_status_unavailable detail=%s", str(exc))
        return None, False


def should_stop_by_token_threshold(available_tokens: int, threshold: int) -> bool:
    return available_tokens <= threshold


def should_stop_by_usable_tokens(available_tokens: int, reserve_tokens: int) -> bool:
    return max(0, available_tokens - reserve_tokens) <= 0


def should_stop_zero_budget_cycles(zero_budget_cycles: int, max_zero_budget_cycles: int) -> bool:
    return zero_budget_cycles >= max_zero_budget_cycles


def should_stop_token_status_failures(token_status_failures: int, max_token_status_failures: int) -> bool:
    return token_status_failures >= max_token_status_failures


def compute_burst_budget(available_tokens: int, reserve_tokens: int, queue_count: int, max_fetches: int | None) -> int:
    usable_tokens = max(0, available_tokens - reserve_tokens)
    budget = min(queue_count, usable_tokens // TOKEN_COST_PER_ASIN)
    if max_fetches is not None:
        budget = min(budget, max_fetches)
    return max(0, budget)


def compute_drip_budget(
    available_tokens: int,
    reserve_tokens: int,
    tokens_per_minute: float,
    interval_seconds: int,
    queue_count: int,
    max_fetches: int | None,
) -> int:
    target_tokens_this_cycle = max(0, int(tokens_per_minute * interval_seconds / 60.0))
    usable_tokens = max(0, available_tokens - reserve_tokens)
    budget = min(queue_count, target_tokens_this_cycle, usable_tokens // TOKEN_COST_PER_ASIN, DEFAULT_MAX_BATCH_SIZE)
    if max_fetches is not None:
        budget = min(budget, max_fetches)
    return max(0, budget)


def sort_queued_asins(queue_decisions: list[Any]) -> list[str]:
    priority_rank = {"high": 0, "medium": 1, "low": 2}
    queued = [d for d in queue_decisions if d.queued]
    queued.sort(key=lambda d: priority_rank.get(d.priority, 9))
    return [d.asin for d in queued]


def select_fetch_batch(queued_asins: list[str], budget: int, max_batch_size: int = DEFAULT_MAX_BATCH_SIZE) -> list[str]:
    return queued_asins[: max(0, min(budget, max_batch_size))]


def merge_metrics(total: dict[str, Any], part: dict[str, Any]) -> dict[str, Any]:
    for key, value in part.items():
        if key == "failure_by_asin":
            total.setdefault("failure_by_asin", {}).update(value)
        elif isinstance(value, int):
            total[key] = int(total.get(key, 0)) + value
    return total


def run_single_mode(queued_asins: list[str], max_fetches: int | None, dry_run: bool) -> tuple[list[str], int, int, bool]:
    selected = queued_asins if max_fetches is None else queued_asins[:max_fetches]
    return selected, len(selected), 1, dry_run


def run_burst_mode(
    queued_asins: list[str],
    reserve_tokens: int,
    max_fetches: int | None,
    available_tokens: int,
    dry_run: bool,
) -> tuple[list[str], int, int, bool]:
    budget = compute_burst_budget(available_tokens, reserve_tokens, len(queued_asins), max_fetches)
    selected = queued_asins[:budget]
    return selected, budget, 1, dry_run


def run_drip_mode(
    queued_asins: list[str],
    reserve_tokens: int,
    tokens_per_minute: float,
    interval_seconds: int,
    max_minutes: int,
    max_fetches: int | None,
    api_key: str,
    timeout_sec: int,
    logger: logging.Logger,
    dry_run: bool,
) -> tuple[list[str], int, int, float, bool, bool]:
    if not queued_asins:
        return [], 0, 0, 0.0, False, True

    start = time.time()
    total_sleep_seconds = 0.0
    cycle_count = 0
    selected: list[str] = []
    queue_index = 0
    max_minutes_reached = False

    while queue_index < len(queued_asins):
        elapsed_minutes = (time.time() - start) / 60.0
        if elapsed_minutes >= max_minutes:
            max_minutes_reached = True
            break

        cycle_count += 1
        available_tokens = get_token_status(api_key=api_key, timeout_sec=timeout_sec, logger=logger)
        remaining_queue = len(queued_asins) - queue_index
        cycle_budget = compute_drip_budget(
            available_tokens=available_tokens,
            reserve_tokens=reserve_tokens,
            tokens_per_minute=tokens_per_minute,
            interval_seconds=interval_seconds,
            queue_count=remaining_queue,
            max_fetches=None if max_fetches is None else max(0, max_fetches - len(selected)),
        )
        batch = select_fetch_batch(queued_asins[queue_index:], cycle_budget)
        selected.extend(batch)
        queue_index += len(batch)

        logger.info(
            "mode=drip cycle=%s available_tokens=%s reserve_tokens=%s cycle_budget=%s selected_fetch_count=%s remaining_queue_count=%s sleep_seconds=%s",
            cycle_count,
            available_tokens,
            reserve_tokens,
            cycle_budget,
            len(batch),
            len(queued_asins) - queue_index,
            interval_seconds,
        )

        if queue_index >= len(queued_asins):
            break
        if max_fetches is not None and len(selected) >= max_fetches:
            break
        if dry_run:
            break

        time.sleep(interval_seconds)
        total_sleep_seconds += interval_seconds

    queue_exhausted = queue_index >= len(queued_asins)
    return selected, len(selected), cycle_count, total_sleep_seconds, max_minutes_reached, queue_exhausted


def main() -> None:
    base_dir = get_base_dir()
    settings = load_settings(base_dir)
    args = parse_args(settings)
    logger = configure_logging(settings["log_path"])

    api_key = settings["api_key"] or os.getenv("KEEPA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Keepa APIキーが見つかりません。config.ini の [keepa] api_key または環境変数 KEEPA_API_KEY を設定してください。")

    logger.info("Base directory: %s", base_dir)
    logger.info("Using config file: %s", settings["config_path"])
    logger.info("Input Excel: %s", settings["input_path"])
    logger.info("Output Excel: %s", settings["output_path"])
    logger.info("ASIN cache: %s", settings["cache_path"])
    logger.info("mode=%s dry_run=%s", args.mode, args.dry_run)

    if not settings["input_path"].exists():
        raise FileNotFoundError(f"入力ファイルが見つかりません: {settings['input_path']}")

    run_start = time.time()
    df = pd.read_excel(settings["input_path"])
    if "ASIN" not in df.columns:
        raise ValueError("入力ファイルに ASIN 列が存在しません。")

    asin_series = df["ASIN"].apply(normalize_asin)
    valid_asins = asin_series.loc[lambda s: s != ""].drop_duplicates().tolist()
    rows_seen = asin_series[asin_series != ""].value_counts().to_dict()
    now = datetime.now()

    cache_df = load_cache(settings["cache_path"])
    queue_decisions = decide_fetch_queue(valid_asins=valid_asins, rows_seen=rows_seen, cache=cache_df, now=now)
    for decision in queue_decisions:
        logger.info("ASIN=%s queue_decision=%s fetch_priority=%s", decision.asin, decision.decision, decision.priority)

    queued_asins = sort_queued_asins(queue_decisions)
    available_tokens_at_start = 0
    last_available_tokens = 0
    stop_reason = ""
    zero_budget_cycles = 0
    token_status_failures = 0

    fetch_metrics: dict[str, Any] = {
        "communication_error_count": 0,
        "keepa_product_not_found_count": 0,
        "monthlySold_missing_count": 0,
        "salesRankDrops30_missing_count": 0,
        "failure_by_asin": {},
    }
    fetched_keepa_data: dict[str, dict[str, Any]] = {}
    attempted_asins: set[str] = set()
    priority_by_asin = {d.asin: d.priority for d in queue_decisions}

    if args.mode == "single":
        selected_asins, selected_fetch_count, cycle_count, is_dry = run_single_mode(queued_asins, args.max_fetches, args.dry_run)
        total_sleep_seconds = 0.0
        max_minutes_reached = False
        queue_exhausted = selected_fetch_count >= len(queued_asins)
        available_tokens, token_ok = get_token_status_safe(api_key=api_key, timeout_sec=settings["timeout_sec"], logger=logger)
        if token_ok and available_tokens is not None:
            available_tokens_at_start = available_tokens
            last_available_tokens = available_tokens
            if should_stop_by_token_threshold(available_tokens, args.stop_when_tokens_below):
                selected_asins = []
                selected_fetch_count = 0
                queue_exhausted = False
                stop_reason = "tokens_below_threshold"
                logger.info(
                    "mode=single stop_reason=tokens_below_threshold available_tokens=%s stop_when_tokens_below=%s",
                    available_tokens,
                    args.stop_when_tokens_below,
                )
        else:
            token_status_failures = 1
            if should_stop_token_status_failures(token_status_failures, args.max_token_status_failures):
                selected_asins = []
                selected_fetch_count = 0
                queue_exhausted = False
                stop_reason = "token_status_failures_exceeded"
                logger.info("mode=single stop_reason=token_status_failures_exceeded token_status_failures=%s", token_status_failures)
    elif args.mode == "burst":
        available_tokens, token_ok = get_token_status_safe(api_key=api_key, timeout_sec=settings["timeout_sec"], logger=logger)
        if token_ok and available_tokens is not None:
            available_tokens_at_start = available_tokens
            last_available_tokens = available_tokens
        else:
            token_status_failures = 1
            available_tokens_at_start = 0

        selected_asins, selected_fetch_count, cycle_count, is_dry = run_burst_mode(
            queued_asins=queued_asins,
            reserve_tokens=args.reserve_tokens,
            max_fetches=args.max_fetches,
            available_tokens=available_tokens_at_start,
            dry_run=args.dry_run,
        )
        total_sleep_seconds = 0.0
        max_minutes_reached = False
        queue_exhausted = selected_fetch_count >= len(queued_asins)

        if not token_ok and should_stop_token_status_failures(token_status_failures, args.max_token_status_failures):
            selected_asins = []
            selected_fetch_count = 0
            queue_exhausted = False
            stop_reason = "token_status_failures_exceeded"
        elif should_stop_by_token_threshold(available_tokens_at_start, args.stop_when_tokens_below):
            selected_asins = []
            selected_fetch_count = 0
            queue_exhausted = False
            stop_reason = "tokens_below_threshold"
        elif should_stop_by_usable_tokens(available_tokens_at_start, args.reserve_tokens):
            selected_asins = []
            selected_fetch_count = 0
            queue_exhausted = False
            stop_reason = "usable_tokens_exhausted"

        logger.info(
            "mode=burst available_tokens=%s reserve_tokens=%s cycle_budget=%s selected_fetch_count=%s remaining_queue_count=%s stop_reason=%s",
            available_tokens_at_start,
            args.reserve_tokens,
            selected_fetch_count,
            selected_fetch_count,
            len(queued_asins) - selected_fetch_count,
            stop_reason,
        )
    else:
        selected_asins = []
        selected_fetch_count = 0
        cycle_count = 0
        total_sleep_seconds = 0.0
        max_minutes_reached = False
        queue_index = 0
        drip_start = time.time()

        while queue_index < len(queued_asins):
            elapsed_minutes = (time.time() - drip_start) / 60.0
            if elapsed_minutes >= args.max_minutes:
                max_minutes_reached = True
                stop_reason = "max_minutes_reached"
                break

            cycle_count += 1
            available_tokens, token_ok = get_token_status_safe(api_key=api_key, timeout_sec=settings["timeout_sec"], logger=logger)
            if token_ok and available_tokens is not None:
                token_status_failures = 0
                last_available_tokens = available_tokens
            else:
                token_status_failures += 1
                available_tokens = last_available_tokens
                if should_stop_token_status_failures(token_status_failures, args.max_token_status_failures):
                    stop_reason = "token_status_failures_exceeded"
                    logger.info("mode=drip stop_reason=token_status_failures_exceeded token_status_failures=%s", token_status_failures)
                    break

            if should_stop_by_token_threshold(available_tokens, args.stop_when_tokens_below):
                stop_reason = "tokens_below_threshold"
                logger.info(
                    "mode=drip stop_reason=tokens_below_threshold available_tokens=%s stop_when_tokens_below=%s",
                    available_tokens,
                    args.stop_when_tokens_below,
                )
                break

            remaining_queue = len(queued_asins) - queue_index
            cycle_budget = compute_drip_budget(
                available_tokens=available_tokens,
                reserve_tokens=args.reserve_tokens,
                tokens_per_minute=args.tokens_per_minute,
                interval_seconds=args.interval_seconds,
                queue_count=remaining_queue,
                max_fetches=None if args.max_fetches is None else max(0, args.max_fetches - selected_fetch_count),
            )
            if cycle_budget == 0:
                zero_budget_cycles += 1
            else:
                zero_budget_cycles = 0
            if should_stop_zero_budget_cycles(zero_budget_cycles, args.max_zero_budget_cycles):
                stop_reason = "zero_budget_cycles_exceeded"
                logger.info(
                    "mode=drip stop_reason=zero_budget_cycles_exceeded zero_budget_cycles=%s max_zero_budget_cycles=%s",
                    zero_budget_cycles,
                    args.max_zero_budget_cycles,
                )
                break

            batch = select_fetch_batch(queued_asins[queue_index:], cycle_budget)
            selected_fetch_count += len(batch)
            selected_asins.extend(batch)

            logger.info(
                "mode=drip cycle=%s available_tokens=%s reserve_tokens=%s cycle_budget=%s selected_fetch_count=%s remaining_queue_count=%s sleep_seconds=%s",
                cycle_count,
                available_tokens,
                args.reserve_tokens,
                cycle_budget,
                len(batch),
                len(queued_asins) - queue_index - len(batch),
                args.interval_seconds,
            )

            if not args.dry_run and batch:
                attempted_asins.update(batch)
                cycle_data, cycle_metrics = collect_keepa_data(
                    asins=batch,
                    api_key=api_key,
                    timeout_sec=settings["timeout_sec"],
                    logger=logger,
                )
                fetched_keepa_data.update(cycle_data)
                fetch_metrics = merge_metrics(fetch_metrics, cycle_metrics)

                cycle_keepa_for_coeff = {**build_keepa_data_from_cache(valid_asins=valid_asins, cache_df=cache_df)[0], **cycle_data}
                cycle_coefficient = calculate_coefficient(cycle_keepa_for_coeff)
                cycle_updates, _ = build_cache_updates(
                    valid_asins=batch,
                    rows_seen=rows_seen,
                    fetched_keepa_data=cycle_data,
                    fetch_metrics=cycle_metrics,
                    existing_cache=cache_df,
                    coefficient=cycle_coefficient,
                    now=datetime.now(),
                    attempted_asins=set(batch),
                    refresh_policy=settings["refresh_policy"],
                )
                for row in cycle_updates:
                    row["fetch_priority"] = priority_by_asin.get(row["asin"], "low")
                cache_df = merge_cache_records(cache_df, cycle_updates)
                save_cache(cache_df, settings["cache_path"], logger=logger)

            queue_index += len(batch)
            if queue_index >= len(queued_asins):
                break
            if args.max_fetches is not None and selected_fetch_count >= args.max_fetches:
                stop_reason = "max_fetches_reached"
                break
            if args.dry_run:
                stop_reason = "dry_run_completed"
                break

            time.sleep(args.interval_seconds)
            total_sleep_seconds += args.interval_seconds

        queue_exhausted = queue_index >= len(queued_asins)
        if queue_exhausted and not stop_reason:
            stop_reason = "queue_exhausted"
        is_dry = args.dry_run

    logger.info("mode=%s selected_fetch_count=%s total_queue_count=%s", args.mode, selected_fetch_count, len(queued_asins))

    if args.mode == "single" and not stop_reason and args.dry_run:
        stop_reason = "dry_run_completed"
    if args.mode == "single" and not stop_reason and queue_exhausted:
        stop_reason = "queue_exhausted"
    if args.mode == "burst" and not stop_reason and queue_exhausted:
        stop_reason = "queue_exhausted"
    if args.mode == "burst" and not stop_reason and args.max_fetches is not None and selected_fetch_count >= args.max_fetches:
        stop_reason = "max_fetches_reached"
    if args.mode == "burst" and not stop_reason and args.dry_run:
        stop_reason = "dry_run_completed"

    if args.mode in {"single", "burst"} and not is_dry and selected_asins:
        batches = [selected_asins[i:i + DEFAULT_MAX_BATCH_SIZE] for i in range(0, len(selected_asins), DEFAULT_MAX_BATCH_SIZE)]
        for batch in batches:
            attempted_asins.update(batch)
            batch_data, batch_metrics = collect_keepa_data(
                asins=batch,
                api_key=api_key,
                timeout_sec=settings["timeout_sec"],
                logger=logger,
            )
            fetched_keepa_data.update(batch_data)
            fetch_metrics = merge_metrics(fetch_metrics, batch_metrics)

    keepa_data_from_cache, cache_metrics = build_keepa_data_from_cache(valid_asins=valid_asins, cache_df=cache_df)
    keepa_data = {**keepa_data_from_cache, **fetched_keepa_data}
    coefficient = calculate_coefficient(keepa_data)

    if not is_dry:
        cache_updates, fetch_result_metrics = build_cache_updates(
            valid_asins=valid_asins,
            rows_seen=rows_seen,
            fetched_keepa_data=fetched_keepa_data,
            fetch_metrics=fetch_metrics,
            existing_cache=cache_df,
            coefficient=coefficient,
            now=now,
            attempted_asins=attempted_asins,
            refresh_policy=settings["refresh_policy"],
        )
        priority_by_asin = {d.asin: d.priority for d in queue_decisions}
        for row in cache_updates:
            row["fetch_priority"] = priority_by_asin.get(row["asin"], "low")

        cache_df = merge_cache_records(cache_df, cache_updates)
        save_cache(cache_df, settings["cache_path"], logger=logger)
        refreshed_data, _ = build_keepa_data_from_cache(valid_asins=valid_asins, cache_df=cache_df)
    else:
        fetch_result_metrics = {"fetched_success_count": 0, "fetched_failure_count": 0}
        refreshed_data = keepa_data

    enriched = enrich_dataframe(df=df, keepa_data=refreshed_data, coefficient=coefficient)
    if not is_dry:
        enriched.to_excel(settings["output_path"], index=False)

    queue_metrics = {
        "queued_for_fetch_count": len(queued_asins),
        "skipped_by_cache_count": len(valid_asins) - len(queued_asins),
        "queue_priority_high_count": len([d for d in queue_decisions if d.queued and d.priority == "high"]),
        "queue_priority_medium_count": len([d for d in queue_decisions if d.queued and d.priority == "medium"]),
        "queue_priority_low_count": len([d for d in queue_decisions if not d.queued]),
        "mode": args.mode,
        "available_tokens_at_start": available_tokens_at_start,
        "reserve_tokens": args.reserve_tokens,
        "total_queue_count": len(queued_asins),
        "selected_fetch_count": selected_fetch_count,
        "remaining_queue_count": max(0, len(queued_asins) - selected_fetch_count),
        "cycle_count": cycle_count,
        "total_sleep_seconds": int(total_sleep_seconds),
        "run_duration_seconds": int(time.time() - run_start),
        "effective_tokens_per_minute": 0.0 if args.mode != "drip" else (selected_fetch_count / max((time.time()-run_start)/60.0, 1e-9)),
        "max_minutes_reached": bool(max_minutes_reached),
        "queue_exhausted": bool(queue_exhausted),
        "stop_reason": stop_reason,
        "stop_when_tokens_below": args.stop_when_tokens_below,
        "max_zero_budget_cycles": args.max_zero_budget_cycles,
        "max_token_status_failures": args.max_token_status_failures,
        "zero_budget_cycles": zero_budget_cycles,
        "token_status_failures": token_status_failures,
        "last_available_tokens": last_available_tokens,
    }

    fetch_metrics.update(fetch_result_metrics)
    fetch_metrics.update(cache_metrics)
    fetch_metrics.update(queue_metrics)

    summary = build_summary(enriched, fetch_metrics, coefficient)
    if stop_reason:
        logger.info("stop_reason=%s", stop_reason)
    log_and_print_summary(summary, logger)
    if is_dry:
        logger.info("Dry-run completed. API fetch and file outputs were skipped.")
    else:
        logger.info("Saved output: %s", settings["output_path"])


if __name__ == "__main__":
    main()
