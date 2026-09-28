import email.utils
import datetime

from incsync.engine import run_sync, parse_retry_after
from incsync.errors import MalformedResponse
from .helpers import SyncTestCase, SYNC_ID


class TestRateLimitsAndErrors(SyncTestCase):
    def test_429_with_numeric_retry_after_backs_off_and_succeeds(self):
        source = self.seeded_source(n=5, page_size=10)
        source.queue_fault(1, {"type": "rate_limit", "retry_after": 3})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "complete")
        self.assertEqual(self.clock.now(), 3.0)  # virtual clock advanced by exactly the backoff

    def test_429_with_http_date_retry_after_is_parsed(self):
        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=10)
        http_date = email.utils.format_datetime(future, usegmt=True)
        seconds = parse_retry_after(http_date)
        self.assertGreater(seconds, 8)
        self.assertLess(seconds, 12)

    def test_transient_5xx_is_retried_and_bounded(self):
        source = self.seeded_source(n=5, page_size=10)
        source.queue_fault(1, {"type": "server_error", "status": 503})
        source.queue_fault(1, {"type": "server_error", "status": 503})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "complete")

    def test_retry_budget_exhausted_after_five_attempts_gives_up(self):
        source = self.seeded_source(n=5, page_size=10)
        for _ in range(5):
            source.queue_fault(1, {"type": "server_error", "status": 503})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("RetryBudgetExhausted", result.stop_reason)

    def test_retry_after_exceeding_remaining_budget_defers_rather_than_retries(self):
        source = self.seeded_source(n=5, page_size=10)
        source.queue_fault(1, {"type": "rate_limit", "retry_after": 500})  # exceeds 120s budget
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("RetryBudgetExhausted", result.stop_reason)
        self.assertEqual(self.clock.now(), 0.0)  # never actually slept past the budget

    def test_malformed_response_is_a_categorised_failure_never_success(self):
        source = self.seeded_source(n=5, page_size=10)
        source.queue_fault(1, {"type": "malformed", "reason": "unparseable JSON body"})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("MalformedResponse", result.stop_reason)
        self.assertEqual(len(self.dest.dump()), 0)
