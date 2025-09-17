#!/usr/bin/env python3
"""
MongoDB Log Analyzer - Web UI Tester
Automated testing of web interface functionality
"""

import requests
import time
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Tuple
from urllib.parse import urljoin, urlparse, parse_qs
import re

from test_config import TestConfig, TestStatus, TestSeverity


class WebUITester:
    """Automated testing of web interface functionality"""

    def __init__(self, base_url: str = None):
        self.base_url = base_url or TestConfig.APP_URL
        self.session = requests.Session()
        self.test_results = []
        self.upload_success = False

    def run_all_tests(self) -> Dict[str, Any]:
        """Run complete web UI test suite"""
        print(f"🌐 Starting web UI tests on {self.base_url}")

        # Test results container
        results = {
            'test_start_time': datetime.now().isoformat(),
            'base_url': self.base_url,
            'tests': [],
            'summary': {}
        }

        # Test connectivity first
        if not self._test_connectivity():
            return self._create_failure_result("Failed to connect to web application")

        try:
            # Core functionality tests
            results['tests'].extend(self._test_file_upload())
            results['tests'].extend(self._test_dashboard())
            results['tests'].extend(self._test_slow_queries_page())
            results['tests'].extend(self._test_search_functionality())
            results['tests'].extend(self._test_analysis_pages())
            results['tests'].extend(self._test_export_functionality())
            results['tests'].extend(self._test_api_endpoints())

            # Summary statistics
            results['summary'] = self._calculate_test_summary(results['tests'])
            results['test_end_time'] = datetime.now().isoformat()

        except Exception as e:
            print(f"❌ Test suite failed with error: {e}")
            results['error'] = str(e)
            results['summary'] = {'status': 'ERROR', 'total_tests': 0, 'passed': 0, 'failed': 1}

        return results

    def _test_connectivity(self) -> bool:
        """Test basic connectivity to the web application"""
        try:
            response = self.session.get(self.base_url, timeout=10)
            if response.status_code == 200:
                print("✅ Web application is accessible")
                return True
            else:
                print(f"❌ Web application returned status code: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Failed to connect to web application: {e}")
            return False

    def _test_file_upload(self) -> List[Dict[str, Any]]:
        """Test file upload functionality"""
        tests = []

        print("📤 Testing file upload...")

        # Test 1: Upload the test log file
        test_result = self._create_test_result("file_upload", "Upload test log file")

        try:
            if not os.path.exists(TestConfig.TEST_LOG_FILE):
                test_result.update({
                    'status': TestStatus.FAIL,
                    'error': f"Test log file not found: {TestConfig.TEST_LOG_FILE}",
                    'severity': TestSeverity.CRITICAL
                })
            else:
                # Perform upload
                upload_url = urljoin(self.base_url, '/upload')

                with open(TestConfig.TEST_LOG_FILE, 'rb') as f:
                    files = {'file': (os.path.basename(TestConfig.TEST_LOG_FILE), f, 'text/plain')}
                    start_time = time.time()

                    response = self.session.post(upload_url, files=files, timeout=TestConfig.TEST_TIMEOUT)
                    upload_duration = time.time() - start_time

                if response.status_code in [200, 302]:  # 302 for redirect after upload
                    self.upload_success = True
                    test_result.update({
                        'status': TestStatus.PASS,
                        'duration_seconds': round(upload_duration, 2),
                        'response_code': response.status_code,
                        'performance_check': upload_duration < TestConfig.PERFORMANCE_THRESHOLDS['file_upload_time']
                    })
                    print(f"  ✅ File upload successful ({upload_duration:.2f}s)")
                else:
                    test_result.update({
                        'status': TestStatus.FAIL,
                        'error': f"Upload failed with status code: {response.status_code}",
                        'response_text': response.text[:500],
                        'severity': TestSeverity.CRITICAL
                    })

        except Exception as e:
            test_result.update({
                'status': TestStatus.ERROR,
                'error': str(e),
                'severity': TestSeverity.HIGH
            })

        tests.append(test_result)

        # Test 2: Check upload without file
        test_result = self._create_test_result("file_upload_validation", "Upload validation (no file)")

        try:
            upload_url = urljoin(self.base_url, '/upload')
            response = self.session.post(upload_url, timeout=10)

            # Should return error or redirect back to upload page
            if response.status_code in [400, 422, 302]:
                test_result.update({
                    'status': TestStatus.PASS,
                    'response_code': response.status_code
                })
            else:
                test_result.update({
                    'status': TestStatus.FAIL,
                    'error': f"Expected error response, got: {response.status_code}"
                })

        except Exception as e:
            test_result.update({
                'status': TestStatus.ERROR,
                'error': str(e)
            })

        tests.append(test_result)
        return tests

    def _test_dashboard(self) -> List[Dict[str, Any]]:
        """Test dashboard functionality"""
        tests = []

        print("📊 Testing dashboard...")

        # Test dashboard load
        test_result = self._create_test_result("dashboard_load", "Dashboard page load")

        try:
            dashboard_url = urljoin(self.base_url, '/')
            start_time = time.time()

            response = self.session.get(dashboard_url, timeout=TestConfig.TEST_TIMEOUT)
            load_duration = time.time() - start_time

            if response.status_code == 200:
                # Check for expected dashboard elements
                content = response.text
                dashboard_elements = [
                    'Total Slow Queries',
                    'Unique Databases',
                    'Average Duration',
                    'Date Range Filter'
                ]

                missing_elements = [elem for elem in dashboard_elements if elem not in content]

                if not missing_elements:
                    test_result.update({
                        'status': TestStatus.PASS,
                        'duration_seconds': round(load_duration, 2),
                        'performance_check': load_duration < TestConfig.PERFORMANCE_THRESHOLDS['dashboard_load_time'],
                        'elements_found': len(dashboard_elements)
                    })
                    print(f"  ✅ Dashboard loaded successfully ({load_duration:.2f}s)")
                else:
                    test_result.update({
                        'status': TestStatus.FAIL,
                        'error': f"Missing dashboard elements: {missing_elements}",
                        'severity': TestSeverity.MEDIUM
                    })
            else:
                test_result.update({
                    'status': TestStatus.FAIL,
                    'error': f"Dashboard failed to load: {response.status_code}",
                    'severity': TestSeverity.HIGH
                })

        except Exception as e:
            test_result.update({
                'status': TestStatus.ERROR,
                'error': str(e),
                'severity': TestSeverity.HIGH
            })

        tests.append(test_result)

        # Test API stats endpoint
        test_result = self._create_test_result("api_stats", "API stats endpoint")

        try:
            api_url = urljoin(self.base_url, '/api/stats')
            response = self.session.get(api_url, timeout=10)

            if response.status_code == 200:
                stats_data = response.json()
                expected_keys = ['total_queries', 'unique_databases', 'date_range']

                if all(key in stats_data for key in expected_keys):
                    test_result.update({
                        'status': TestStatus.PASS,
                        'stats_keys': list(stats_data.keys()),
                        'total_queries': stats_data.get('total_queries', 0)
                    })
                else:
                    test_result.update({
                        'status': TestStatus.FAIL,
                        'error': f"Missing expected keys in stats response",
                        'received_keys': list(stats_data.keys())
                    })
            else:
                test_result.update({
                    'status': TestStatus.FAIL,
                    'error': f"API stats failed: {response.status_code}"
                })

        except Exception as e:
            test_result.update({
                'status': TestStatus.ERROR,
                'error': str(e)
            })

        tests.append(test_result)
        return tests

    def _test_slow_queries_page(self) -> List[Dict[str, Any]]:
        """Test slow queries page functionality"""
        tests = []

        print("🐌 Testing slow queries page...")

        # Test basic page load
        test_result = self._create_test_result("slow_queries_load", "Slow queries page load")

        try:
            queries_url = urljoin(self.base_url, '/slow-queries')
            response = self.session.get(queries_url, timeout=TestConfig.TEST_TIMEOUT)

            if response.status_code == 200:
                content = response.text

                # Check for pagination elements
                pagination_elements = ['Previous', 'Next', 'Page']
                pagination_found = any(elem in content for elem in pagination_elements)

                # Check for filter elements
                filter_elements = ['Database Filter', 'Duration Filter', 'date_start', 'date_end']
                filters_found = sum(1 for elem in filter_elements if elem in content)

                test_result.update({
                    'status': TestStatus.PASS,
                    'pagination_found': pagination_found,
                    'filters_found': filters_found,
                    'content_length': len(content)
                })
                print("  ✅ Slow queries page loaded successfully")
            else:
                test_result.update({
                    'status': TestStatus.FAIL,
                    'error': f"Page failed to load: {response.status_code}"
                })

        except Exception as e:
            test_result.update({
                'status': TestStatus.ERROR,
                'error': str(e)
            })

        tests.append(test_result)

        # Test pagination
        test_result = self._create_test_result("slow_queries_pagination", "Slow queries pagination")

        try:
            # Test with specific page parameter
            queries_url = urljoin(self.base_url, '/slow-queries?page=2&per_page=10')
            response = self.session.get(queries_url, timeout=10)

            if response.status_code == 200:
                test_result.update({
                    'status': TestStatus.PASS,
                    'response_code': response.status_code
                })
            else:
                test_result.update({
                    'status': TestStatus.FAIL,
                    'error': f"Pagination failed: {response.status_code}"
                })

        except Exception as e:
            test_result.update({
                'status': TestStatus.ERROR,
                'error': str(e)
            })

        tests.append(test_result)
        return tests

    def _test_search_functionality(self) -> List[Dict[str, Any]]:
        """Test search functionality"""
        tests = []

        print("🔍 Testing search functionality...")

        for test_case in TestConfig.SEARCH_TEST_CASES:
            test_name = f"search_{test_case['type']}_{test_case['value']}"
            test_result = self._create_test_result(test_name, f"Search: {test_case['type']} = {test_case['value']}")

            try:
                # Build search URL with parameters
                search_url = urljoin(self.base_url, '/search-logs')
                params = {test_case['type']: test_case['value']}

                start_time = time.time()
                response = self.session.get(search_url, params=params, timeout=TestConfig.TEST_TIMEOUT)
                search_duration = time.time() - start_time

                if response.status_code == 200:
                    # Try to extract result count from response
                    content = response.text
                    result_count = self._extract_result_count(content)

                    performance_ok = search_duration < TestConfig.PERFORMANCE_THRESHOLDS['search_response_time']

                    test_result.update({
                        'status': TestStatus.PASS,
                        'duration_seconds': round(search_duration, 2),
                        'result_count': result_count,
                        'performance_check': performance_ok,
                        'search_params': params
                    })

                    if result_count >= test_case.get('expected_min', 0):
                        print(f"  ✅ Search {test_case['type']} successful: {result_count} results ({search_duration:.2f}s)")
                    else:
                        print(f"  ⚠️  Search {test_case['type']} returned fewer results than expected")

                else:
                    test_result.update({
                        'status': TestStatus.FAIL,
                        'error': f"Search failed: {response.status_code}",
                        'search_params': params
                    })

            except Exception as e:
                test_result.update({
                    'status': TestStatus.ERROR,
                    'error': str(e),
                    'search_params': test_case
                })

            tests.append(test_result)

        return tests

    def _test_analysis_pages(self) -> List[Dict[str, Any]]:
        """Test analysis page functionality"""
        tests = []

        print("📈 Testing analysis pages...")

        analysis_routes = [
            ('index_suggestions', 'Index Suggestions'),
            ('slow_query_analysis', 'Query Pattern Analysis'),
            ('workload_summary', 'Workload Summary'),
            ('search_user_access', 'User Access Analysis'),
            ('query_trend', 'Query Trend Analysis'),
            ('current_op_analyzer', 'Current Operations Analysis')
        ]

        for route_key, page_name in analysis_routes:
            test_result = self._create_test_result(f"analysis_{route_key}", f"{page_name} page load")

            try:
                page_url = urljoin(self.base_url, TestConfig.TEST_ROUTES[route_key])
                start_time = time.time()

                response = self.session.get(page_url, timeout=30)  # Analysis pages may take longer
                load_duration = time.time() - start_time

                if response.status_code == 200:
                    content = response.text

                    # Check for basic page structure
                    has_content = len(content) > 1000  # Basic content check
                    has_form = 'form' in content.lower()
                    has_table = 'table' in content.lower()

                    test_result.update({
                        'status': TestStatus.PASS,
                        'duration_seconds': round(load_duration, 2),
                        'content_length': len(content),
                        'has_form': has_form,
                        'has_table': has_table,
                        'has_content': has_content
                    })
                    print(f"  ✅ {page_name} loaded ({load_duration:.2f}s)")

                else:
                    test_result.update({
                        'status': TestStatus.FAIL,
                        'error': f"Page failed to load: {response.status_code}",
                        'severity': TestSeverity.MEDIUM
                    })

            except Exception as e:
                test_result.update({
                    'status': TestStatus.ERROR,
                    'error': str(e),
                    'severity': TestSeverity.MEDIUM
                })

            tests.append(test_result)

        return tests

    def _test_export_functionality(self) -> List[Dict[str, Any]]:
        """Test export functionality"""
        tests = []

        print("📤 Testing export functionality...")

        for export_key, export_route in TestConfig.EXPORT_ROUTES.items():
            test_result = self._create_test_result(f"export_{export_key}", f"Export {export_key}")

            try:
                export_url = urljoin(self.base_url, export_route)
                response = self.session.get(export_url, timeout=30)

                if response.status_code == 200:
                    # Check if response is JSON
                    try:
                        if response.headers.get('content-type', '').startswith('application/json'):
                            export_data = response.json()
                            test_result.update({
                                'status': TestStatus.PASS,
                                'content_type': response.headers.get('content-type'),
                                'data_size': len(json.dumps(export_data)),
                                'is_json': True
                            })
                        else:
                            test_result.update({
                                'status': TestStatus.PASS,
                                'content_type': response.headers.get('content-type'),
                                'data_size': len(response.content),
                                'is_json': False
                            })

                        print(f"  ✅ Export {export_key} successful")

                    except json.JSONDecodeError:
                        test_result.update({
                            'status': TestStatus.FAIL,
                            'error': "Response is not valid JSON",
                            'content_type': response.headers.get('content-type')
                        })

                else:
                    test_result.update({
                        'status': TestStatus.FAIL,
                        'error': f"Export failed: {response.status_code}",
                        'severity': TestSeverity.MEDIUM
                    })

            except Exception as e:
                test_result.update({
                    'status': TestStatus.ERROR,
                    'error': str(e),
                    'severity': TestSeverity.MEDIUM
                })

            tests.append(test_result)

        return tests

    def _test_api_endpoints(self) -> List[Dict[str, Any]]:
        """Test API endpoints"""
        tests = []

        print("🔌 Testing API endpoints...")

        # Test stats API (already tested in dashboard, but separate test)
        test_result = self._create_test_result("api_stats_detailed", "API stats endpoint detailed")

        try:
            api_url = urljoin(self.base_url, '/api/stats')
            response = self.session.get(api_url, timeout=10)

            if response.status_code == 200:
                stats_data = response.json()

                # Validate data structure and types
                validation_results = self._validate_stats_api_response(stats_data)

                test_result.update({
                    'status': TestStatus.PASS if validation_results['valid'] else TestStatus.FAIL,
                    'validation_results': validation_results,
                    'response_size': len(json.dumps(stats_data))
                })

                if validation_results['valid']:
                    print("  ✅ API stats endpoint validation passed")
                else:
                    print(f"  ⚠️  API stats validation issues: {validation_results['errors']}")

            else:
                test_result.update({
                    'status': TestStatus.FAIL,
                    'error': f"API failed: {response.status_code}"
                })

        except Exception as e:
            test_result.update({
                'status': TestStatus.ERROR,
                'error': str(e)
            })

        tests.append(test_result)
        return tests

    def _validate_stats_api_response(self, stats_data: dict) -> Dict[str, Any]:
        """Validate the structure of stats API response"""
        validation = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        # Expected numeric fields
        numeric_fields = ['total_queries', 'unique_databases', 'unique_collections', 'average_duration']

        for field in numeric_fields:
            if field in stats_data:
                if not isinstance(stats_data[field], (int, float)):
                    validation['errors'].append(f"{field} should be numeric")
                    validation['valid'] = False
            else:
                validation['warnings'].append(f"Optional field {field} not present")

        return validation

    def _extract_result_count(self, html_content: str) -> int:
        """Extract result count from HTML content"""
        # Look for common patterns indicating result count
        patterns = [
            r'(\d+)\s+results?',
            r'Showing\s+(\d+)',
            r'Found\s+(\d+)',
            r'Total:\s+(\d+)'
        ]

        for pattern in patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                return int(match.group(1))

        return 0

    def _create_test_result(self, test_name: str, description: str) -> Dict[str, Any]:
        """Create a test result template"""
        return {
            'test_name': test_name,
            'description': description,
            'timestamp': datetime.now().isoformat(),
            'status': TestStatus.SKIP,
            'severity': TestSeverity.INFO
        }

    def _create_failure_result(self, error_message: str) -> Dict[str, Any]:
        """Create a failure result for the entire test suite"""
        return {
            'test_start_time': datetime.now().isoformat(),
            'base_url': self.base_url,
            'tests': [],
            'error': error_message,
            'summary': {
                'status': 'FAILED',
                'total_tests': 0,
                'passed': 0,
                'failed': 1,
                'errors': 1
            }
        }

    def _calculate_test_summary(self, tests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate summary statistics from test results"""
        total = len(tests)
        passed = sum(1 for test in tests if test.get('status') == TestStatus.PASS)
        failed = sum(1 for test in tests if test.get('status') == TestStatus.FAIL)
        errors = sum(1 for test in tests if test.get('status') == TestStatus.ERROR)
        skipped = sum(1 for test in tests if test.get('status') == TestStatus.SKIP)

        success_rate = (passed / total * 100) if total > 0 else 0

        # Performance summary
        performance_issues = sum(1 for test in tests if test.get('performance_check') is False)

        return {
            'status': 'PASSED' if failed == 0 and errors == 0 else 'FAILED',
            'total_tests': total,
            'passed': passed,
            'failed': failed,
            'errors': errors,
            'skipped': skipped,
            'success_rate': round(success_rate, 1),
            'performance_issues': performance_issues,
            'file_upload_success': self.upload_success
        }

    def save_test_results(self, results: Dict[str, Any], output_path: str = None) -> str:
        """Save test results to file"""
        if not output_path:
            timestamp = TestConfig.get_test_timestamp()
            output_path = f"{TestConfig.REPORTS_DIR}/web_ui_test_results_{timestamp}.json"

        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)

        print(f"💾 Web UI test results saved to: {output_path}")
        return output_path


if __name__ == "__main__":
    # Example usage
    tester = WebUITester()
    results = tester.run_all_tests()
    output_path = tester.save_test_results(results)

    print(f"\n📋 Web UI Testing Summary:")
    summary = results['summary']
    print(f"   Status: {summary['status']}")
    print(f"   Tests: {summary['passed']}/{summary['total_tests']} passed")
    print(f"   Success Rate: {summary['success_rate']}%")
    print(f"   Results saved to: {output_path}")