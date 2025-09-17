#!/usr/bin/env python3
"""
MongoDB Log Analyzer - Main Test Orchestrator
Comprehensive testing workflow that validates functionality, performance, and data accuracy
"""

import os
import sys
import subprocess
import time
import json
import signal
from datetime import datetime
from typing import Dict, Any, Optional

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from test_config import TestConfig
from baseline_validator import BaselineValidator
from performance_monitor import PerformanceMonitor
from web_ui_tester import WebUITester
from data_validator import DataValidator


class TestOrchestrator:
    """Main test orchestrator that coordinates all testing phases"""

    def __init__(self):
        self.test_session_id = TestConfig.get_test_timestamp()
        self.app_process = None
        self.performance_monitor = PerformanceMonitor(TestConfig.APP_PORT)
        self.baseline_data = None
        self.test_results = {
            'session_id': self.test_session_id,
            'start_time': datetime.now().isoformat(),
            'config': self._get_test_config(),
            'phases': {},
            'summary': {}
        }

        # Ensure required directories exist
        TestConfig.ensure_directories()

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def run_complete_test_suite(self) -> Dict[str, Any]:
        """Execute the complete testing workflow"""
        print("🚀 Starting MongoDB Log Analyzer Test Suite")
        print(f"📅 Session ID: {self.test_session_id}")
        print(f"🗂️  Test log file: {TestConfig.TEST_LOG_FILE}")
        print("=" * 80)

        try:
            # Phase 1: Environment setup and baseline creation
            self._phase_1_baseline_analysis()

            # Phase 2: Application startup and performance monitoring
            self._phase_2_app_startup()

            # Phase 3: File upload and processing
            self._phase_3_file_upload()

            # Phase 4: Web UI functionality testing
            self._phase_4_web_ui_testing()

            # Phase 5: Data validation
            self._phase_5_data_validation()

            # Phase 6: Performance analysis
            self._phase_6_performance_analysis()

            # Final summary
            self._generate_final_summary()

        except KeyboardInterrupt:
            print("\n🛑 Test suite interrupted by user")
            self.test_results['status'] = 'INTERRUPTED'
        except Exception as e:
            print(f"\n❌ Test suite failed with error: {e}")
            self.test_results['status'] = 'ERROR'
            self.test_results['error'] = str(e)
        finally:
            # Cleanup
            self._cleanup()

        self.test_results['end_time'] = datetime.now().isoformat()
        return self.test_results

    def _phase_1_baseline_analysis(self):
        """Phase 1: Create baseline data from raw log file"""
        print("\n📊 PHASE 1: Baseline Data Analysis")
        print("-" * 50)

        phase_start = time.time()

        try:
            # Verify test log file exists
            if not os.path.exists(TestConfig.TEST_LOG_FILE):
                raise FileNotFoundError(f"Test log file not found: {TestConfig.TEST_LOG_FILE}")

            print(f"📂 Analyzing log file: {os.path.basename(TestConfig.TEST_LOG_FILE)}")

            # Benchmark file operations first
            file_benchmark = self.performance_monitor.benchmark_file_operation(TestConfig.TEST_LOG_FILE, "read")
            hash_benchmark = self.performance_monitor.benchmark_file_operation(TestConfig.TEST_LOG_FILE, "hash")

            # Create baseline data
            baseline_validator = BaselineValidator(TestConfig.TEST_LOG_FILE)
            self.baseline_data = baseline_validator.analyze_log_file()

            # Save baseline data
            baseline_path = baseline_validator.save_baseline()

            self.test_results['phases']['phase_1'] = {
                'name': 'Baseline Analysis',
                'status': 'PASSED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'baseline_data_path': baseline_path,
                'baseline_summary': {
                    'file_size_mb': self.baseline_data['file_info']['size_mb'],
                    'total_lines': self.baseline_data['parsing_stats']['total_lines'],
                    'slow_queries': self.baseline_data['event_counts']['slow_queries'],
                    'connections': self.baseline_data['event_counts']['connections'],
                    'authentications': self.baseline_data['event_counts']['authentications']
                },
                'file_benchmarks': {
                    'read_performance': file_benchmark,
                    'hash_performance': hash_benchmark
                }
            }

            print(f"✅ Phase 1 completed successfully ({time.time() - phase_start:.2f}s)")
            print(f"   📊 Baseline data: {self.baseline_data['event_counts']['slow_queries']} slow queries")

        except Exception as e:
            self.test_results['phases']['phase_1'] = {
                'name': 'Baseline Analysis',
                'status': 'FAILED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'error': str(e)
            }
            raise

    def _phase_2_app_startup(self):
        """Phase 2: Start application and verify it's running"""
        print("\n🚀 PHASE 2: Application Startup")
        print("-" * 50)

        phase_start = time.time()

        try:
            # Clean up any existing test database
            TestConfig.cleanup_test_artifacts()

            # Check if port is available
            if not self.performance_monitor.check_port_availability(TestConfig.APP_PORT):
                print(f"⚠️  Port {TestConfig.APP_PORT} is already in use, attempting to use it")

            # Activate virtual environment and start Flask app
            self._start_flask_app()

            # Wait for app to be ready
            if self.performance_monitor.wait_for_port(TestConfig.APP_PORT, timeout=30):
                print(f"✅ Application started successfully on port {TestConfig.APP_PORT}")

                # Start performance monitoring
                self.performance_monitor.start_monitoring(interval=2.0)

                self.test_results['phases']['phase_2'] = {
                    'name': 'Application Startup',
                    'status': 'PASSED',
                    'duration_seconds': round(time.time() - phase_start, 2),
                    'app_url': TestConfig.APP_URL,
                    'app_port': TestConfig.APP_PORT
                }

            else:
                raise Exception(f"Application failed to start on port {TestConfig.APP_PORT}")

        except Exception as e:
            self.test_results['phases']['phase_2'] = {
                'name': 'Application Startup',
                'status': 'FAILED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'error': str(e)
            }
            raise

    def _phase_3_file_upload(self):
        """Phase 3: Upload test file and measure processing performance"""
        print("\n📤 PHASE 3: File Upload and Processing")
        print("-" * 50)

        phase_start = time.time()

        try:
            # Measure file upload operation
            def upload_operation():
                ui_tester = WebUITester(TestConfig.APP_URL)
                return ui_tester._test_file_upload()

            upload_results, upload_measurement = self.performance_monitor.measure_operation(
                "file_upload_and_processing",
                upload_operation
            )

            # Check if upload was successful
            upload_success = any(
                result.get('status') == 'PASS' and 'file_upload' in result.get('test_name', '')
                for result in upload_results
            )

            if upload_success:
                print(f"✅ File upload and processing completed successfully")

                # Wait a bit for processing to complete
                time.sleep(5)

                # Check database size
                db_info = self.performance_monitor.get_database_size(TestConfig.TEST_DB_PATH)

                self.test_results['phases']['phase_3'] = {
                    'name': 'File Upload and Processing',
                    'status': 'PASSED',
                    'duration_seconds': round(time.time() - phase_start, 2),
                    'upload_measurement': upload_measurement,
                    'upload_results': upload_results,
                    'database_info': db_info,
                    'performance_check': upload_measurement['duration_seconds'] < TestConfig.PERFORMANCE_THRESHOLDS['file_upload_time']
                }

            else:
                raise Exception("File upload failed - check upload test results")

        except Exception as e:
            self.test_results['phases']['phase_3'] = {
                'name': 'File Upload and Processing',
                'status': 'FAILED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'error': str(e)
            }
            raise

    def _phase_4_web_ui_testing(self):
        """Phase 4: Comprehensive web UI functionality testing"""
        print("\n🌐 PHASE 4: Web UI Functionality Testing")
        print("-" * 50)

        phase_start = time.time()

        try:
            # Run web UI tests
            ui_tester = WebUITester(TestConfig.APP_URL)

            def ui_testing_operation():
                return ui_tester.run_all_tests()

            ui_test_results, ui_measurement = self.performance_monitor.measure_operation(
                "web_ui_testing",
                ui_testing_operation
            )

            # Save UI test results
            ui_results_path = ui_tester.save_test_results(ui_test_results)

            # Evaluate overall UI test success
            ui_summary = ui_test_results.get('summary', {})
            ui_success = ui_summary.get('status') == 'PASSED'

            self.test_results['phases']['phase_4'] = {
                'name': 'Web UI Testing',
                'status': 'PASSED' if ui_success else 'FAILED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'ui_test_summary': ui_summary,
                'ui_test_results_path': ui_results_path,
                'ui_measurement': ui_measurement
            }

            if ui_success:
                print(f"✅ Web UI testing completed: {ui_summary['passed']}/{ui_summary['total_tests']} tests passed")
            else:
                print(f"⚠️  Web UI testing issues: {ui_summary['failed']} failures, {ui_summary['errors']} errors")

        except Exception as e:
            self.test_results['phases']['phase_4'] = {
                'name': 'Web UI Testing',
                'status': 'FAILED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'error': str(e)
            }
            print(f"❌ Web UI testing failed: {e}")

    def _phase_5_data_validation(self):
        """Phase 5: Validate data accuracy against baseline"""
        print("\n🔍 PHASE 5: Data Validation")
        print("-" * 50)

        phase_start = time.time()

        try:
            if not self.baseline_data:
                raise Exception("Baseline data not available for validation")

            # Run data validation
            data_validator = DataValidator(TestConfig.APP_URL, self.baseline_data)

            def data_validation_operation():
                return data_validator.validate_all_data(self.baseline_data)

            validation_results, validation_measurement = self.performance_monitor.measure_operation(
                "data_validation",
                data_validation_operation
            )

            # Save validation results
            validation_results_path = data_validator.save_validation_results(validation_results)

            # Evaluate validation success
            validation_summary = validation_results.get('summary', {})
            validation_success = validation_summary.get('status') == 'PASSED'

            self.test_results['phases']['phase_5'] = {
                'name': 'Data Validation',
                'status': 'PASSED' if validation_success else 'FAILED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'validation_summary': validation_summary,
                'validation_results_path': validation_results_path,
                'validation_measurement': validation_measurement
            }

            if validation_success:
                print(f"✅ Data validation passed: {validation_summary['average_accuracy']}% average accuracy")
            else:
                print(f"⚠️  Data validation issues: {validation_summary['failed']} failures")
                print(f"   Average accuracy: {validation_summary['average_accuracy']}%")

        except Exception as e:
            self.test_results['phases']['phase_5'] = {
                'name': 'Data Validation',
                'status': 'FAILED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'error': str(e)
            }
            print(f"❌ Data validation failed: {e}")

    def _phase_6_performance_analysis(self):
        """Phase 6: Analyze performance metrics and generate report"""
        print("\n📈 PHASE 6: Performance Analysis")
        print("-" * 50)

        phase_start = time.time()

        try:
            # Stop performance monitoring and get results
            performance_stats = self.performance_monitor.stop_monitoring()

            # Save performance report
            performance_report_path = self.performance_monitor.save_performance_report(performance_stats)

            # Analyze performance against thresholds
            performance_analysis = self._analyze_performance_results(performance_stats)

            # Get final database size
            final_db_info = self.performance_monitor.get_database_size(TestConfig.TEST_DB_PATH)

            self.test_results['phases']['phase_6'] = {
                'name': 'Performance Analysis',
                'status': 'PASSED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'performance_report_path': performance_report_path,
                'performance_analysis': performance_analysis,
                'final_database_info': final_db_info,
                'monitoring_duration': performance_stats.get('monitoring_info', {}).get('duration_seconds', 0)
            }

            print(f"✅ Performance analysis completed")
            print(f"   📊 Monitoring duration: {performance_stats.get('monitoring_info', {}).get('duration_seconds', 0):.1f}s")
            print(f"   💾 Database size: {final_db_info.get('size_mb', 0):.1f}MB")

            if performance_analysis['threshold_violations']:
                print(f"   ⚠️  Performance issues: {len(performance_analysis['threshold_violations'])} threshold violations")

        except Exception as e:
            self.test_results['phases']['phase_6'] = {
                'name': 'Performance Analysis',
                'status': 'FAILED',
                'duration_seconds': round(time.time() - phase_start, 2),
                'error': str(e)
            }
            print(f"❌ Performance analysis failed: {e}")

    def _start_flask_app(self):
        """Start the Flask application using the specified virtual environment"""
        try:
            # Build command to activate virtual environment and start app
            start_command = [
                'bash', '-c',
                f'source {TestConfig.VENV_PATH} && '
                f'cd {TestConfig.PROJECT_ROOT} && '
                f'python app.py --port {TestConfig.APP_PORT} --host {TestConfig.APP_HOST}'
            ]

            print(f"🔄 Starting Flask app on {TestConfig.APP_HOST}:{TestConfig.APP_PORT}...")

            # Set environment variables
            env = os.environ.copy()
            env['FLASK_ENV'] = 'testing'
            env['FLASK_DEBUG'] = '0'

            # Start the process
            self.app_process = subprocess.Popen(
                start_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                preexec_fn=os.setsid  # Create new process group
            )

            print(f"📱 Flask app started with PID: {self.app_process.pid}")

        except Exception as e:
            raise Exception(f"Failed to start Flask app: {e}")

    def _analyze_performance_results(self, performance_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance results against thresholds"""
        analysis = {
            'threshold_violations': performance_stats.get('threshold_violations', []),
            'performance_score': 100,
            'memory_efficiency': 'GOOD',
            'cpu_efficiency': 'GOOD',
            'overall_rating': 'EXCELLENT'
        }

        # Calculate performance score based on violations
        violations = len(analysis['threshold_violations'])
        if violations == 0:
            analysis['performance_score'] = 100
            analysis['overall_rating'] = 'EXCELLENT'
        elif violations <= 2:
            analysis['performance_score'] = 85
            analysis['overall_rating'] = 'GOOD'
        elif violations <= 4:
            analysis['performance_score'] = 70
            analysis['overall_rating'] = 'FAIR'
        else:
            analysis['performance_score'] = 50
            analysis['overall_rating'] = 'POOR'

        # Analyze memory usage
        peak_memory = performance_stats.get('peak_values', {}).get('app_memory_mb', 0)
        if peak_memory > TestConfig.PERFORMANCE_THRESHOLDS['memory_usage_mb']:
            analysis['memory_efficiency'] = 'POOR'
        elif peak_memory > TestConfig.PERFORMANCE_THRESHOLDS['memory_usage_mb'] * 0.8:
            analysis['memory_efficiency'] = 'FAIR'

        # Analyze CPU usage
        peak_cpu = performance_stats.get('peak_values', {}).get('app_cpu_percent', 0)
        if peak_cpu > 80:
            analysis['cpu_efficiency'] = 'POOR'
        elif peak_cpu > 60:
            analysis['cpu_efficiency'] = 'FAIR'

        return analysis

    def _generate_final_summary(self):
        """Generate final test summary"""
        print("\n📋 GENERATING FINAL SUMMARY")
        print("-" * 50)

        # Count phase results
        phases = self.test_results.get('phases', {})
        total_phases = len(phases)
        passed_phases = sum(1 for phase in phases.values() if phase.get('status') == 'PASSED')
        failed_phases = sum(1 for phase in phases.values() if phase.get('status') == 'FAILED')

        # Calculate total duration
        total_duration = sum(
            phase.get('duration_seconds', 0)
            for phase in phases.values()
        )

        # Overall status
        overall_status = 'PASSED' if failed_phases == 0 else 'FAILED'

        # Generate summary
        summary = {
            'overall_status': overall_status,
            'total_phases': total_phases,
            'passed_phases': passed_phases,
            'failed_phases': failed_phases,
            'success_rate': round((passed_phases / total_phases * 100) if total_phases > 0 else 0, 1),
            'total_duration_seconds': round(total_duration, 2),
            'total_duration_minutes': round(total_duration / 60, 2),
            'baseline_summary': self.test_results.get('phases', {}).get('phase_1', {}).get('baseline_summary', {}),
            'recommendations': self._generate_recommendations()
        }

        self.test_results['summary'] = summary
        self.test_results['status'] = overall_status

        # Save final report
        self._save_final_report()

        # Print summary
        print(f"🎯 OVERALL STATUS: {overall_status}")
        print(f"✅ Phases passed: {passed_phases}/{total_phases}")
        print(f"⏱️  Total duration: {summary['total_duration_minutes']:.1f} minutes")

        if self.baseline_data:
            baseline_summary = summary['baseline_summary']
            print(f"📊 Baseline data: {baseline_summary.get('slow_queries', 0)} slow queries, "
                  f"{baseline_summary.get('file_size_mb', 0):.1f}MB file")

    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on test results"""
        recommendations = []

        phases = self.test_results.get('phases', {})

        # Check for failed phases
        for phase_name, phase_data in phases.items():
            if phase_data.get('status') == 'FAILED':
                recommendations.append(f"❌ Investigate {phase_data.get('name', phase_name)} failure: {phase_data.get('error', 'Unknown error')}")

        # Performance recommendations
        if 'phase_6' in phases:
            performance_analysis = phases['phase_6'].get('performance_analysis', {})
            violations = performance_analysis.get('threshold_violations', [])

            if violations:
                recommendations.append(f"⚡ Performance optimization needed: {len(violations)} threshold violations detected")

            if performance_analysis.get('memory_efficiency') in ['FAIR', 'POOR']:
                recommendations.append("💾 Consider memory optimization - peak usage exceeded recommendations")

        # Data validation recommendations
        if 'phase_5' in phases:
            validation_summary = phases['phase_5'].get('validation_summary', {})
            avg_accuracy = validation_summary.get('average_accuracy', 100)

            if avg_accuracy < 95:
                recommendations.append(f"🔍 Data accuracy improvement needed - current accuracy: {avg_accuracy:.1f}%")

        # File processing recommendations
        if 'phase_3' in phases:
            upload_measurement = phases['phase_3'].get('upload_measurement', {})
            upload_duration = upload_measurement.get('duration_seconds', 0)

            if upload_duration > TestConfig.PERFORMANCE_THRESHOLDS['file_upload_time']:
                recommendations.append(f"📤 File processing optimization recommended - current time: {upload_duration:.1f}s")

        # Add positive recommendations if no issues
        if not recommendations:
            recommendations.extend([
                "🎉 All tests passed successfully!",
                "✨ System performance is within acceptable thresholds",
                "📈 Data accuracy validation passed",
                "🚀 Web UI functionality tests completed successfully"
            ])

        return recommendations

    def _save_final_report(self):
        """Save final comprehensive test report"""
        report_path = f"{TestConfig.REPORTS_DIR}/comprehensive_test_report_{self.test_session_id}.json"

        with open(report_path, 'w') as f:
            json.dump(self.test_results, f, indent=2, default=str)

        print(f"💾 Comprehensive test report saved: {report_path}")

    def _cleanup(self):
        """Clean up resources and processes"""
        print("\n🧹 Cleaning up...")

        # Stop Flask app
        if self.app_process:
            try:
                # Send SIGTERM to the process group
                os.killpg(os.getpgid(self.app_process.pid), signal.SIGTERM)

                # Wait for graceful shutdown
                self.app_process.wait(timeout=10)
                print("✅ Flask app stopped gracefully")
            except (subprocess.TimeoutExpired, ProcessLookupError):
                # Force kill if necessary
                try:
                    os.killpg(os.getpgid(self.app_process.pid), signal.SIGKILL)
                    print("⚠️  Flask app force stopped")
                except ProcessLookupError:
                    pass

        # Stop performance monitoring
        if self.performance_monitor.monitoring:
            self.performance_monitor.stop_monitoring()

        print("✅ Cleanup completed")

    def _signal_handler(self, signum, frame):
        """Handle interrupt signals gracefully"""
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self._cleanup()
        sys.exit(1)

    def _get_test_config(self) -> Dict[str, Any]:
        """Get test configuration summary"""
        return {
            'project_root': TestConfig.PROJECT_ROOT,
            'test_log_file': TestConfig.TEST_LOG_FILE,
            'app_url': TestConfig.APP_URL,
            'venv_path': TestConfig.VENV_PATH,
            'performance_thresholds': TestConfig.PERFORMANCE_THRESHOLDS,
            'test_timeout': TestConfig.TEST_TIMEOUT
        }


def main():
    """Main entry point for the test suite"""
    print("🧪 MongoDB Log Analyzer - Comprehensive Test Suite")
    print("=" * 80)

    try:
        orchestrator = TestOrchestrator()
        results = orchestrator.run_complete_test_suite()

        # Final output
        print("\n" + "=" * 80)
        print("🏁 TEST SUITE COMPLETED")
        print("=" * 80)

        summary = results.get('summary', {})
        print(f"📊 Final Status: {summary.get('overall_status', 'UNKNOWN')}")
        print(f"⏱️  Total Duration: {summary.get('total_duration_minutes', 0):.1f} minutes")
        print(f"✅ Success Rate: {summary.get('success_rate', 0):.1f}%")

        recommendations = summary.get('recommendations', [])
        if recommendations:
            print("\n💡 Recommendations:")
            for i, rec in enumerate(recommendations[:5], 1):  # Show first 5
                print(f"   {i}. {rec}")

        # Exit with appropriate code
        exit_code = 0 if summary.get('overall_status') == 'PASSED' else 1
        sys.exit(exit_code)

    except KeyboardInterrupt:
        print("\n🛑 Test suite interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()