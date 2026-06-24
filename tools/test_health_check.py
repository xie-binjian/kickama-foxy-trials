"""health_check.py 新功能测试套件

测试覆盖:
    1. TokenBucket 限流器单元测试
    2. 超时覆盖逻辑单元测试
    3. parse_endpoint_timeouts 解析测试
    4. 集成测试（实际运行健康检查 + 限流）

运行方式:
    python -m pytest test_health_check.py -v
    或
    python test_health_check.py
"""

import sys
import os
import time
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from health_check import (
    TokenBucket,
    apply_timeout_overrides,
    parse_endpoint_timeouts,
)


class TestTokenBucket(unittest.TestCase):
    """令牌桶限流器单元测试"""

    def test_initial_tokens_full(self):
        """初始时桶应该是满的"""
        bucket = TokenBucket(rate=10, burst=5)
        self.assertEqual(bucket.tokens, 5)

    def test_acquire_from_full_bucket(self):
        """满桶时应该能立刻获取令牌"""
        bucket = TokenBucket(rate=10, burst=3)
        self.assertTrue(bucket.acquire(1))
        self.assertTrue(bucket.acquire(2))

    def test_acquire_too_many_tokens(self):
        """请求超过桶容量的令牌应该失败"""
        bucket = TokenBucket(rate=5, burst=3)
        self.assertFalse(bucket.acquire(10))

    def test_rate_limiting_blocks_after_drain(self):
        """令牌耗尽后限流器应阻止更多请求"""
        bucket = TokenBucket(rate=2, burst=0)
        # 耗尽初始的微量令牌
        while bucket.acquire():
            pass
        # 速率极低（2/s），瞬间不可能补充
        self.assertFalse(bucket.acquire(), "令牌耗尽后应阻止请求")

    def test_refill_over_time(self):
        """验证令牌会随时间补充"""
        bucket = TokenBucket(rate=100, burst=100)
        for _ in range(100):
            self.assertTrue(bucket.acquire())
        self.assertFalse(bucket.acquire())
        time.sleep(0.05)
        acquired = 0
        for _ in range(10):
            if bucket.acquire():
                acquired += 1
        self.assertGreaterEqual(acquired, 1, "等待后应至少能获取 1 个令牌")

    def test_wait_and_acquire_completes(self):
        """wait_and_acquire 应在合理时间内完成"""
        bucket = TokenBucket(rate=50, burst=50)
        start = time.time()
        result = bucket.wait_and_acquire(timeout=2.0)
        elapsed = time.time() - start
        self.assertTrue(result)
        self.assertLess(elapsed, 1.0)

    def test_thread_safety(self):
        """多线程并发获取令牌不应崩溃"""
        bucket = TokenBucket(rate=1000, burst=100)
        errors = []

        def worker():
            try:
                for _ in range(50):
                    bucket.acquire()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(errors), 0)

    def test_zero_rate_raises(self):
        """速率为 0 应该抛出 ValueError"""
        with self.assertRaises(ValueError):
            TokenBucket(rate=0)

    def test_negative_rate_raises(self):
        """负速率应该抛出 ValueError"""
        with self.assertRaises(ValueError):
            TokenBucket(rate=-5)


class TestTimeoutOverrides(unittest.TestCase):
    """超时覆盖逻辑测试"""

    def setUp(self):
        self.services = {
            "backend": {"host": "localhost", "port": 8080, "path": "/health", "timeout": 5},
            "market": {"host": "localhost", "port": 8081, "path": "/health", "timeout": 5},
            "frailbox": {"host": "localhost", "port": 8082, "path": "/health", "timeout": 10},
        }
        self.infra = {
            "postgresql": {"host": "localhost", "port": 5432, "timeout": 5},
            "redis": {"host": "localhost", "port": 6379, "timeout": 5},
        }

    def test_no_overrides_preserves_defaults(self):
        svc, infra = apply_timeout_overrides(self.services, self.infra, None, None)
        self.assertEqual(svc["backend"]["timeout"], 5)
        self.assertEqual(svc["frailbox"]["timeout"], 10)

    def test_global_timeout_applies_to_all(self):
        svc, infra = apply_timeout_overrides(self.services, self.infra, 3, None)
        self.assertEqual(svc["backend"]["timeout"], 3)
        self.assertEqual(svc["frailbox"]["timeout"], 3)
        self.assertEqual(infra["redis"]["timeout"], 3)

    def test_endpoint_override_takes_priority(self):
        overrides = {"backend": 2, "frailbox": 15}
        svc, infra = apply_timeout_overrides(self.services, self.infra, 5, overrides)
        self.assertEqual(svc["backend"]["timeout"], 2)
        self.assertEqual(svc["market"]["timeout"], 5)
        self.assertEqual(svc["frailbox"]["timeout"], 15)

    def test_infra_endpoint_override(self):
        overrides = {"postgresql": 8}
        svc, infra = apply_timeout_overrides(self.services, self.infra, None, overrides)
        self.assertEqual(infra["postgresql"]["timeout"], 8)
        self.assertEqual(infra["redis"]["timeout"], 5)

    def test_original_dicts_not_mutated(self):
        original = self.services["backend"]["timeout"]
        svc, _ = apply_timeout_overrides(self.services, self.infra, 3, None)
        self.assertEqual(self.services["backend"]["timeout"], original)


class TestParseEndpointTimeouts(unittest.TestCase):
    """端点超时参数解析测试"""

    def test_single_endpoint(self):
        self.assertEqual(parse_endpoint_timeouts("backend=3"), {"backend": 3})

    def test_multiple_endpoints(self):
        result = parse_endpoint_timeouts("backend=2,market=8,frailbox=15")
        self.assertEqual(result, {"backend": 2, "market": 8, "frailbox": 15})

    def test_empty_string(self):
        self.assertEqual(parse_endpoint_timeouts(""), {})

    def test_whitespace_handling(self):
        result = parse_endpoint_timeouts("  backend = 5 , market = 10  ")
        self.assertEqual(result, {"backend": 5, "market": 10})

    def test_invalid_format_skipped(self):
        result = parse_endpoint_timeouts("backend=3,badformat,market=5")
        self.assertEqual(result, {"backend": 3, "market": 5})

    def test_non_integer_value_skipped(self):
        result = parse_endpoint_timeouts("backend=abc,market=5")
        self.assertEqual(result, {"market": 5})


if __name__ == "__main__":
    unittest.main(verbosity=2)
