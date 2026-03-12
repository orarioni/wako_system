from datetime import datetime, timedelta
import io
import logging
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from keepa_enrich import build_keepa_data_from_cache, enrich_dataframe, format_keepa_last_sold_update
from update_queue import decide_fetch_queue, load_cache, save_cache


class UpdateQueueTests(unittest.TestCase):
    def test_new_asin_is_queued(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        decisions = decide_fetch_queue(["A1"], {"A1": 1}, pd.DataFrame(), now)
        self.assertEqual(len(decisions), 1)
        self.assertTrue(decisions[0].queued)
        self.assertEqual(decisions[0].decision, "new")

    def test_recent_cached_asin_is_skipped(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        cache = pd.DataFrame(
            [
                {
                    "asin": "A1",
                    "last_fetched_at": (now - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
                    "keepa_lastSoldUpdate": (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
                    "keepa_monthlySold": 100,
                    "keepa_salesRankDrops30": 40,
                    "estimate_source": "monthlySold",
                    "next_fetch_after": (now + timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S"),
                    "failure_type": None,
                }
            ]
        )
        decisions = decide_fetch_queue(["A1"], {"A1": 1}, cache, now)
        self.assertFalse(decisions[0].queued)
        self.assertEqual(decisions[0].decision, "skip_cached")

    def test_communication_error_is_retry(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        cache = pd.DataFrame(
            [
                {
                    "asin": "A1",
                    "failure_type": "communication_error",
                    "estimate_source": "unavailable",
                    "next_fetch_after": (now + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
                }
            ]
        )
        decisions = decide_fetch_queue(["A1"], {"A1": 1}, cache, now)
        self.assertTrue(decisions[0].queued)
        self.assertEqual(decisions[0].decision, "retry")

    def test_cache_hit_is_used_for_output_enrichment(self):
        cache = pd.DataFrame(
            [
                {
                    "asin": "A1",
                    "keepa_title": "title",
                    "keepa_monthlySold": 77,
                    "keepa_lastSoldUpdate": "2026-01-01 00:00:00",
                    "keepa_salesRankDrops30": 20,
                }
            ]
        )
        keepa_data, metrics = build_keepa_data_from_cache(["A1", "A2"], cache)
        self.assertEqual(metrics["cache_hit_count"], 1)
        self.assertEqual(metrics["cache_miss_count"], 1)

        df = pd.DataFrame({"ASIN": ["A1", ""]})
        enriched = enrich_dataframe(df, keepa_data, coefficient=1.0)

        self.assertEqual(enriched.loc[0, "estimate_source"], "monthlySold")
        self.assertEqual(enriched.loc[0, "estimated_monthly_sales"], 77)
        self.assertEqual(enriched.loc[1, "estimate_source"], "unavailable")
        self.assertEqual(enriched.loc[1, "estimate_confidence"], "D")
        self.assertEqual(enriched.loc[1, "estimate_note"], "ASIN missing")

    def test_not_found_future_is_skipped(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        cache = pd.DataFrame(
            [
                {
                    "asin": "A1",
                    "failure_type": "keepa_product_not_found",
                    "estimate_source": "unavailable",
                    "keepa_monthlySold": None,
                    "keepa_salesRankDrops30": None,
                    "next_fetch_after": (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
                }
            ]
        )
        decisions = decide_fetch_queue(["A1"], {"A1": 1}, cache, now)
        self.assertFalse(decisions[0].queued)
        self.assertEqual(decisions[0].decision, "skip_not_found_cooldown")

    def test_not_found_past_is_queued(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        cache = pd.DataFrame(
            [
                {
                    "asin": "A1",
                    "failure_type": "keepa_product_not_found",
                    "estimate_source": "unavailable",
                    "keepa_monthlySold": None,
                    "keepa_salesRankDrops30": None,
                    "next_fetch_after": (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
                }
            ]
        )
        decisions = decide_fetch_queue(["A1"], {"A1": 1}, cache, now)
        self.assertTrue(decisions[0].queued)
        self.assertEqual(decisions[0].decision, "retry_not_found_due")

    def test_last_sold_update_timezone_keeps_jst_clock(self):
        logger = logging.getLogger("test_keepa_tz")
        value = format_keepa_last_sold_update("2026-03-10T03:22:54.9392186+09:00", asin="A1", logger=logger)
        self.assertEqual(value, "2026-03-10 03:22:54")

    def test_last_sold_update_unparseable_logs_warning(self):
        stream = io.StringIO()
        logger = logging.getLogger("test_keepa_parse")
        logger.handlers.clear()
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(stream)
        logger.addHandler(handler)

        value = format_keepa_last_sold_update("not-a-date", asin="A1", logger=logger)
        self.assertEqual(value, "not-a-date")
        self.assertIn("status=lastSoldUpdate_parse_error", stream.getvalue())

    def test_save_cache_writes_readable_csv(self):
        cache = pd.DataFrame([{"asin": "A1", "estimate_source": "monthlySold"}])
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "asin_cache.csv"
            save_cache(cache, cache_path)
            loaded = load_cache(cache_path)
            self.assertEqual(loaded.loc[0, "asin"], "A1")


if __name__ == "__main__":
    unittest.main()
