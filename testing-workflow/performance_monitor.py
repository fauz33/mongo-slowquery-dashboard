#!/usr/bin/env python3
"""
MongoDB Log Analyzer - Performance Monitor
Real-time performance monitoring during testing
"""

import psutil
import time
import threading
import json
import os
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any
import subprocess

from test_config import TestConfig, PERFORMANCE_METRICS


class PerformanceMonitor:
    """Monitor system and application performance during testing"""

    def __init__(self, app_port: int = 5001):
        self.app_port = app_port
        self.app_process = None
        self.monitoring = False
        self.metrics_data = defaultdict(list)
        self.start_time = None
        self.monitor_thread = None

    def start_monitoring(self, interval: float = 1.0):
        """Start performance monitoring in background thread"""
        if self.monitoring:
            print("⚠️  Performance monitoring already running")
            return

        self.monitoring = True
        self.start_time = time.time()
        self.metrics_data.clear()

        # Find application process
        self._find_app_process()

        # Start monitoring thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(interval,),
            daemon=True
        )
        self.monitor_thread.start()

        print(f"📊 Performance monitoring started (interval: {interval}s)")

    def stop_monitoring(self) -> Dict[str, Any]:
        """Stop monitoring and return collected metrics"""
        if not self.monitoring:
            return {}

        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)

        duration = time.time() - self.start_time if self.start_time else 0

        # Calculate statistics
        stats = self._calculate_performance_stats(duration)

        print(f"🏁 Performance monitoring stopped (duration: {duration:.1f}s)")
        return stats

    def _find_app_process(self):
        """Find the Flask application process"""
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                cmdline = proc.info.get('cmdline', [])
                if any('app.py' in arg for arg in cmdline) or any(str(self.app_port) in arg for arg in cmdline):
                    self.app_process = psutil.Process(proc.info['pid'])
                    print(f"📱 Found app process: PID {proc.info['pid']}")
                    return
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        print("⚠️  Could not find app process, monitoring system metrics only")

    def _monitor_loop(self, interval: float):
        """Main monitoring loop"""
        while self.monitoring:
            timestamp = time.time()
            metrics = self._collect_metrics(timestamp)

            # Store metrics
            for metric, value in metrics.items():
                self.metrics_data[metric].append({
                    'timestamp': timestamp,
                    'value': value
                })

            time.sleep(interval)

    def _collect_metrics(self, timestamp: float) -> Dict[str, float]:
        """Collect system and application metrics"""
        metrics = {}

        try:
            # System CPU and memory
            metrics['system_cpu_percent'] = psutil.cpu_percent()
            system_memory = psutil.virtual_memory()
            metrics['system_memory_percent'] = system_memory.percent
            metrics['system_memory_mb'] = system_memory.used / 1024 / 1024

            # System disk I/O
            disk_io = psutil.disk_io_counters()
            if disk_io:
                metrics['system_disk_read_mb'] = disk_io.read_bytes / 1024 / 1024
                metrics['system_disk_write_mb'] = disk_io.write_bytes / 1024 / 1024

            # System network I/O
            network_io = psutil.net_io_counters()
            if network_io:
                metrics['system_network_sent_mb'] = network_io.bytes_sent / 1024 / 1024
                metrics['system_network_recv_mb'] = network_io.bytes_recv / 1024 / 1024

            # Application-specific metrics (if process found)
            if self.app_process and self.app_process.is_running():
                try:
                    # CPU and memory for app process
                    metrics['app_cpu_percent'] = self.app_process.cpu_percent()

                    memory_info = self.app_process.memory_info()
                    metrics['app_memory_mb'] = memory_info.rss / 1024 / 1024
                    metrics['app_memory_vms_mb'] = memory_info.vms / 1024 / 1024

                    # File handles and threads
                    metrics['app_file_handles'] = self.app_process.num_fds() if hasattr(self.app_process, 'num_fds') else 0
                    metrics['app_threads'] = self.app_process.num_threads()

                    # I/O counters for app process
                    try:
                        io_counters = self.app_process.io_counters()
                        metrics['app_io_read_mb'] = io_counters.read_bytes / 1024 / 1024
                        metrics['app_io_write_mb'] = io_counters.write_bytes / 1024 / 1024
                    except (psutil.AccessDenied, AttributeError):
                        pass

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    self.app_process = None

        except Exception as e:
            print(f"⚠️  Error collecting metrics: {e}")

        return metrics

    def _calculate_performance_stats(self, duration: float) -> Dict[str, Any]:
        """Calculate performance statistics from collected data"""
        stats = {
            'monitoring_info': {
                'duration_seconds': round(duration, 2),
                'start_time': datetime.fromtimestamp(self.start_time).isoformat() if self.start_time else None,
                'end_time': datetime.now().isoformat(),
                'data_points': len(self.metrics_data.get('system_cpu_percent', [])),
                'app_process_found': self.app_process is not None
            },
            'metrics_summary': {},
            'peak_values': {},
            'average_values': {},
            'threshold_violations': []
        }

        # Calculate statistics for each metric
        for metric_name, data_points in self.metrics_data.items():
            if not data_points:
                continue

            values = [point['value'] for point in data_points]

            metric_stats = {
                'min': min(values),
                'max': max(values),
                'avg': sum(values) / len(values),
                'count': len(values)
            }

            # Add percentiles for key metrics
            if len(values) > 1:
                sorted_values = sorted(values)
                n = len(sorted_values)
                metric_stats['median'] = sorted_values[n // 2]
                metric_stats['p95'] = sorted_values[int(n * 0.95)]
                metric_stats['p99'] = sorted_values[int(n * 0.99)]

            stats['metrics_summary'][metric_name] = metric_stats
            stats['peak_values'][metric_name] = max(values)
            stats['average_values'][metric_name] = metric_stats['avg']

        # Check threshold violations
        stats['threshold_violations'] = self._check_threshold_violations(stats['peak_values'])

        return stats

    def _check_threshold_violations(self, peak_values: Dict[str, float]) -> List[Dict[str, Any]]:
        """Check for performance threshold violations"""
        violations = []

        thresholds = {
            'system_cpu_percent': 90.0,
            'system_memory_percent': 90.0,
            'app_memory_mb': TestConfig.PERFORMANCE_THRESHOLDS['memory_usage_mb'],
            'app_cpu_percent': 80.0
        }

        for metric, threshold in thresholds.items():
            if metric in peak_values and peak_values[metric] > threshold:
                violations.append({
                    'metric': metric,
                    'peak_value': peak_values[metric],
                    'threshold': threshold,
                    'violation_percent': ((peak_values[metric] - threshold) / threshold) * 100
                })

        return violations

    def measure_operation(self, operation_name: str, operation_func, *args, **kwargs):
        """Measure performance of a specific operation"""
        print(f"⏱️  Measuring operation: {operation_name}")

        start_time = time.time()
        start_memory = psutil.virtual_memory().used / 1024 / 1024

        try:
            result = operation_func(*args, **kwargs)
            success = True
            error = None
        except Exception as e:
            result = None
            success = False
            error = str(e)

        end_time = time.time()
        end_memory = psutil.virtual_memory().used / 1024 / 1024

        duration = end_time - start_time
        memory_delta = end_memory - start_memory

        measurement = {
            'operation': operation_name,
            'duration_seconds': round(duration, 3),
            'memory_delta_mb': round(memory_delta, 2),
            'success': success,
            'error': error,
            'start_time': datetime.fromtimestamp(start_time).isoformat(),
            'end_time': datetime.fromtimestamp(end_time).isoformat()
        }

        print(f"  ✅ {operation_name}: {duration:.2f}s, Δ{memory_delta:+.1f}MB memory")

        return result, measurement

    def get_database_size(self, db_path: str) -> Dict[str, Any]:
        """Get database file size information"""
        try:
            if os.path.exists(db_path):
                stat = os.stat(db_path)
                return {
                    'exists': True,
                    'size_bytes': stat.st_size,
                    'size_mb': round(stat.st_size / 1024 / 1024, 2),
                    'modified_time': datetime.fromtimestamp(stat.st_mtime).isoformat()
                }
            else:
                return {'exists': False}
        except Exception as e:
            return {'exists': False, 'error': str(e)}

    def check_port_availability(self, port: int) -> bool:
        """Check if a port is available"""
        try:
            connections = psutil.net_connections()
            for conn in connections:
                if conn.laddr and conn.laddr.port == port and conn.status == 'LISTEN':
                    return False
            return True
        except Exception:
            return True

    def wait_for_port(self, port: int, timeout: int = 30) -> bool:
        """Wait for a port to become available (app to start)"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            if not self.check_port_availability(port):
                return True
            time.sleep(1)

        return False

    def save_performance_report(self, stats: Dict[str, Any], output_path: str = None) -> str:
        """Save performance statistics to file"""
        if not output_path:
            timestamp = TestConfig.get_test_timestamp()
            output_path = f"{TestConfig.REPORTS_DIR}/performance_report_{timestamp}.json"

        with open(output_path, 'w') as f:
            json.dump(stats, f, indent=2, default=str)

        print(f"💾 Performance report saved to: {output_path}")
        return output_path

    def benchmark_file_operation(self, file_path: str, operation: str = "read") -> Dict[str, Any]:
        """Benchmark file I/O operations"""
        if not os.path.exists(file_path):
            return {'error': 'File not found'}

        file_size = os.path.getsize(file_path)

        if operation == "read":
            def read_file():
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    return sum(1 for _ in f)

            result, measurement = self.measure_operation(f"read_file_{os.path.basename(file_path)}", read_file)

        elif operation == "hash":
            import hashlib
            def hash_file():
                hash_sha256 = hashlib.sha256()
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_sha256.update(chunk)
                return hash_sha256.hexdigest()

            result, measurement = self.measure_operation(f"hash_file_{os.path.basename(file_path)}", hash_file)

        else:
            return {'error': f'Unknown operation: {operation}'}

        # Add file size context
        measurement['file_size_bytes'] = file_size
        measurement['file_size_mb'] = round(file_size / 1024 / 1024, 2)

        if measurement['duration_seconds'] > 0:
            measurement['throughput_mb_per_second'] = round(measurement['file_size_mb'] / measurement['duration_seconds'], 2)

        return measurement


if __name__ == "__main__":
    # Example usage
    monitor = PerformanceMonitor()

    # Test file operation benchmarking
    if os.path.exists(TestConfig.TEST_LOG_FILE):
        print("🧪 Testing file operations...")
        read_benchmark = monitor.benchmark_file_operation(TestConfig.TEST_LOG_FILE, "read")
        hash_benchmark = monitor.benchmark_file_operation(TestConfig.TEST_LOG_FILE, "hash")

        print(f"📖 Read benchmark: {read_benchmark}")
        print(f"🔍 Hash benchmark: {hash_benchmark}")

    print("✅ Performance monitor ready for testing")