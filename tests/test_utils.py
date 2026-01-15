import pytest
import time
from unittest.mock import Mock, patch
from src.utils.retry import retry, RetryManager
from src.utils.progress import ProgressTracker, create_progress_tracker


class TestRetry:
    """重试机制测试"""

    def test_retry_success_first_try(self):
        """测试一次成功"""
        @retry(max_retries=3, delay=1)
        def succeed_function():
            return "success"

        result = succeed_function()
        assert result == "success"

    def test_retry_success_after_failures(self):
        """测试失败后重试成功"""
        call_count = 0

        @retry(max_retries=3, delay=0.01)
        def fail_twice_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"

        result = fail_twice_function()
        assert result == "success"
        assert call_count == 3

    def test_retry_max_retries_exceeded(self):
        """测试超过最大重试次数"""
        call_count = 0

        @retry(max_retries=3, delay=0.01)
        def always_fail_function():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")

        with pytest.raises(ValueError):
            always_fail_function()

        assert call_count == 3

    def test_retry_with_exception_type(self):
        """测试重试特定异常类型"""
        call_count = 0

        @retry(max_retries=2, delay=0.01)
        def retryable_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Connection failed")
            return "success"

        result = retryable_function()
        assert result == "success"
        assert call_count == 2

    def test_retry_delay_backoff(self):
        """测试重试延迟递增"""
        call_times = []

        @retry(max_retries=3, delay=0.1, backoff=2)
        def slow_retry_function():
            call_times.append(time.time())
            if len(call_times) < 3:
                raise ValueError("Retry needed")
            return "success"

        result = slow_retry_function()
        assert result == "success"

        delays = [
            call_times[i+1] - call_times[i]
            for i in range(len(call_times)-1)
        ]

        assert delays[1] > delays[0]  # 第二次延迟应该比第一次长


class TestRetryManager:
    """重试管理器测试"""

    def test_execute_success_first_try(self):
        """测试一次成功"""
        manager = RetryManager(max_retries=3, delay=0.01)

        def succeed_function():
            return "success"

        result = manager.execute(succeed_function)
        assert result == "success"

    def test_execute_success_after_failures(self):
        """测试失败后重试成功"""
        call_count = 0

        def fail_twice_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"

        manager = RetryManager(max_retries=3, delay=0.01)
        result = manager.execute(fail_twice_function)

        assert result == "success"
        assert call_count == 3

    def test_execute_max_retries_exceeded(self):
        """测试超过最大重试次数"""
        call_count = 0

        def always_fail_function():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")

        manager = RetryManager(max_retries=3, delay=0.01)

        with pytest.raises(ValueError):
            manager.execute(always_fail_function)

        assert call_count == 3

    def test_execute_with_args_kwargs(self):
        """测试带参数的函数执行"""
        call_args = []

        def func_with_args(a, b, c=None):
            call_args.append((a, b, c))
            return "success"

        manager = RetryManager(max_retries=1, delay=0.01)
        result = manager.execute(func_with_args, 1, 2, c=3)

        assert result == "success"
        assert call_args[0] == (1, 2, 3)


class TestProgressTracker:
    """进度跟踪器测试"""

    def test_init_and_start(self):
        """测试初始化和启动"""
        tracker = ProgressTracker(100, "Test")
        tracker.start()

        assert tracker.total == 100
        assert tracker.desc == "Test"
        assert tracker.pbar is not None

        tracker.close()

    def test_update(self):
        """测试更新进度"""
        tracker = ProgressTracker(100, "Test")
        tracker.start()

        tracker.update(25)
        tracker.update(25)

        tracker.close()

    def test_set_description(self):
        """测试设置描述"""
        tracker = ProgressTracker(100, "Test")
        tracker.start()

        tracker.set_description("New Description")

        tracker.close()

    def test_close(self):
        """测试关闭进度条"""
        tracker = ProgressTracker(100, "Test")
        tracker.start()

        tracker.close()

        assert tracker.pbar is None

    def test_update_without_start(self):
        """测试未启动时更新"""
        tracker = ProgressTracker(100, "Test")

        tracker.update(50)

        assert tracker.pbar is None

    def test_create_progress_tracker(self):
        """测试创建进度跟踪器"""
        tracker = create_progress_tracker(50, "Creating")

        assert tracker.total == 50
        assert tracker.desc == "Creating"
        assert tracker.pbar is not None

        tracker.close()

    def test_multiple_trackers(self):
        """测试多个进度跟踪器"""
        tracker1 = ProgressTracker(100, "Tracker 1")
        tracker2 = ProgressTracker(200, "Tracker 2")

        tracker1.start()
        tracker2.start()

        tracker1.update(50)
        tracker2.update(100)

        tracker1.close()
        tracker2.close()

    def test_zero_total(self):
        """测试零总数"""
        tracker = ProgressTracker(0, "Zero")
        tracker.start()

        tracker.update(0)

        tracker.close()
