from datetime import datetime, timedelta
import unittest

import pandas as pd

from keepa_enrich import build_keepa_data_from_cache, enrich_dataframe
from update_queue import decide_fetch_queue


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


if __name__ == "__main__":
    unittest.main()
