import unittest

from keepa_enrich import compute_burst_budget, compute_drip_budget, run_single_mode, select_fetch_batch


class RunModeLogicTests(unittest.TestCase):
    def test_burst_budget_uses_available_minus_reserve(self):
        budget = compute_burst_budget(available_tokens=300, reserve_tokens=10, queue_count=500, max_fetches=None)
        self.assertEqual(budget, 290)

    def test_drip_budget_respects_target_tokens_this_cycle(self):
        budget = compute_drip_budget(
            available_tokens=100,
            reserve_tokens=10,
            tokens_per_minute=5,
            interval_seconds=60,
            queue_count=100,
            max_fetches=None,
        )
        self.assertEqual(budget, 5)

    def test_budget_becomes_zero_when_below_reserve(self):
        budget = compute_drip_budget(
            available_tokens=8,
            reserve_tokens=10,
            tokens_per_minute=5,
            interval_seconds=60,
            queue_count=100,
            max_fetches=None,
        )
        self.assertEqual(budget, 0)

    def test_select_fetch_batch_limits_by_budget(self):
        queued = [f"A{i}" for i in range(20)]
        batch = select_fetch_batch(queued, budget=7)
        self.assertEqual(len(batch), 7)

    def test_drip_queue_empty_can_exit_early(self):
        budget = compute_drip_budget(
            available_tokens=100,
            reserve_tokens=10,
            tokens_per_minute=5,
            interval_seconds=60,
            queue_count=0,
            max_fetches=None,
        )
        self.assertEqual(budget, 0)

    def test_single_mode_selection(self):
        selected, selected_count, cycle_count, is_dry = run_single_mode(["A1", "A2", "A3"], max_fetches=2, dry_run=False)
        self.assertEqual(selected, ["A1", "A2"])
        self.assertEqual(selected_count, 2)
        self.assertEqual(cycle_count, 1)
        self.assertFalse(is_dry)


if __name__ == "__main__":
    unittest.main()
