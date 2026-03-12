from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Any

import pandas as pd

CACHE_COLUMNS = [
    "asin",
    "last_fetched_at",
    "last_success_at",
    "last_failure_at",
    "keepa_title",
    "keepa_lastSoldUpdate",
    "keepa_monthlySold",
    "keepa_salesRankDrops30",
    "estimate_source",
    "estimate_confidence",
    "estimate_note",
    "failure_type",
    "rows_seen_in_input",
    "fetch_priority",
    "next_fetch_after",
    "consecutive_failures",
]

RETRY_DELAYS = {
    "communication_error": timedelta(minutes=30),
    "keepa_product_not_found": timedelta(days=7),
    "other_failure": timedelta(days=1),
    "monthly_sold_present": timedelta(days=7),
    "drops_only": timedelta(days=3),
    "both_missing": timedelta(days=2),
}

STALE_FETCH_DAYS = 14
STALE_LAST_SOLD_UPDATE_DAYS = 30
HIGH_ROWS_THRESHOLD = 20


@dataclass
class QueueDecision:
    asin: str
    queued: bool
    decision: str
    priority: str


def safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return pd.to_datetime(text).to_pydatetime()
    except Exception:  # noqa: BLE001
        return None


def load_cache(cache_path: Path) -> pd.DataFrame:
    if not cache_path.exists():
        return pd.DataFrame(columns=CACHE_COLUMNS)

    cache = pd.read_csv(cache_path, dtype={"asin": str})
    for col in CACHE_COLUMNS:
        if col not in cache.columns:
            cache[col] = None
    return cache[CACHE_COLUMNS]


def save_cache(cache: pd.DataFrame, cache_path: Path, logger: Any = None) -> None:
    ordered = cache.copy()
    for col in CACHE_COLUMNS:
        if col not in ordered.columns:
            ordered[col] = None

    tmp_path = cache_path.with_name(f"{cache_path.name}.tmp")
    try:
        ordered[CACHE_COLUMNS].to_csv(tmp_path, index=False, encoding="utf-8")
        os.replace(tmp_path, cache_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        if logger is not None:
            logger.exception("cache_save_failed path=%s", cache_path)
        raise


def compute_next_fetch_after(now: datetime, failure_type: str | None, monthly_sold: Any, drops30: Any) -> datetime:
    if failure_type == "communication_error":
        return now + RETRY_DELAYS["communication_error"]
    if failure_type == "keepa_product_not_found":
        return now + RETRY_DELAYS["keepa_product_not_found"]
    if failure_type:
        return now + RETRY_DELAYS["other_failure"]

    monthly = safe_float(monthly_sold)
    drops = safe_float(drops30)
    if monthly is not None and monthly >= 1:
        return now + RETRY_DELAYS["monthly_sold_present"]
    if drops is not None:
        return now + RETRY_DELAYS["drops_only"]
    return now + RETRY_DELAYS["both_missing"]


def decide_fetch_queue(valid_asins: list[str], rows_seen: dict[str, int], cache: pd.DataFrame, now: datetime) -> list[QueueDecision]:
    cache_idx = cache.set_index("asin", drop=False) if not cache.empty else pd.DataFrame(columns=CACHE_COLUMNS).set_index("asin", drop=False)
    decisions: list[QueueDecision] = []

    for asin in valid_asins:
        row = cache_idx.loc[asin] if asin in cache_idx.index else None
        if row is None:
            decisions.append(QueueDecision(asin=asin, queued=True, decision="new", priority="high"))
            continue

        failure_type = str(row.get("failure_type") or "").strip() or None
        estimate_source = str(row.get("estimate_source") or "").strip()
        monthly = safe_float(row.get("keepa_monthlySold"))
        drops = safe_float(row.get("keepa_salesRankDrops30"))

        next_fetch_after = parse_dt(row.get("next_fetch_after"))
        last_fetched_at = parse_dt(row.get("last_fetched_at"))
        keepa_last_sold = parse_dt(row.get("keepa_lastSoldUpdate"))

        if failure_type == "keepa_product_not_found":
            if next_fetch_after is not None and next_fetch_after <= now:
                decisions.append(QueueDecision(asin=asin, queued=True, decision="retry_not_found_due", priority="high"))
            else:
                decisions.append(QueueDecision(asin=asin, queued=False, decision="skip_not_found_cooldown", priority="low"))
        elif failure_type:
            decisions.append(QueueDecision(asin=asin, queued=True, decision="retry", priority="high"))
        elif estimate_source == "unavailable":
            decisions.append(QueueDecision(asin=asin, queued=True, decision="retry", priority="high"))
        elif monthly is None and drops is None:
            decisions.append(QueueDecision(asin=asin, queued=True, decision="retry", priority="high"))
        elif next_fetch_after is not None and next_fetch_after <= now:
            decisions.append(QueueDecision(asin=asin, queued=True, decision="stale", priority="high"))
        else:
            is_stale_fetch = last_fetched_at is None or last_fetched_at <= (now - timedelta(days=STALE_FETCH_DAYS))
            is_stale_keepa = keepa_last_sold is not None and keepa_last_sold <= (now - timedelta(days=STALE_LAST_SOLD_UPDATE_DAYS))
            high_rows = int(rows_seen.get(asin, 0)) >= HIGH_ROWS_THRESHOLD
            if is_stale_fetch or is_stale_keepa or high_rows:
                decisions.append(QueueDecision(asin=asin, queued=True, decision="stale", priority="medium"))
            else:
                decisions.append(QueueDecision(asin=asin, queued=False, decision="skip_cached", priority="low"))

    return decisions


def merge_cache_records(cache: pd.DataFrame, records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return cache

    updates = pd.DataFrame(records)
    for col in CACHE_COLUMNS:
        if col not in updates.columns:
            updates[col] = None

    if cache.empty:
        return updates[CACHE_COLUMNS]

    cache = cache.set_index("asin", drop=False)
    updates = updates.set_index("asin", drop=False)
    cache.update(updates)

    missing = updates.loc[~updates.index.isin(cache.index)]
    if not missing.empty:
        cache = pd.concat([cache, missing], axis=0)

    return cache.reset_index(drop=True)[CACHE_COLUMNS]
