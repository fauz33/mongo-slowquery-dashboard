#!/usr/bin/env python3
"""
MongoDB Log Analyzer - Data Validator
Compare web UI displayed data with baseline truth data
"""

import json
import requests
import re
from datetime import datetime
from typing import Dict, List, Any, Tuple
from urllib.parse import urljoin

from test_config import TestConfig, TestStatus, TestSeverity


class DataValidator:
    """Validates data accuracy between baseline and web UI"""

    def __init__(self, base_url: str = None, baseline_data: Dict[str, Any] = None):
        self.base_url = base_url or TestConfig.APP_URL
        self.baseline_data = baseline_data or {}
        self.session = requests.Session()
        self.validation_results = []

    def validate_all_data(self, baseline_data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform comprehensive data validation"""
        self.baseline_data = baseline_data

        print("🔍 Starting data validation against baseline...")

        validation_report = {
            'validation_start_time': datetime.now().isoformat(),
            'baseline_info': self._extract_baseline_info(),
            'validations': [],
            'summary': {}
        }

        try:
            # Core data validations
            validation_report['validations'].extend(self._validate_dashboard_stats())
            validation_report['validations'].extend(self._validate_slow_queries_data())
            validation_report['validations'].extend(self._validate_search_results())
            validation_report['validations'].extend(self._validate_filter_functionality())
            validation_report['validations'].extend(self._validate_analysis_data())
            validation_report['validations'].extend(self._validate_export_data())

            # Calculate validation summary
            validation_report['summary'] = self._calculate_validation_summary(validation_report['validations'])
            validation_report['validation_end_time'] = datetime.now().isoformat()

        except Exception as e:
            print(f"❌ Data validation failed with error: {e}")
            validation_report['error'] = str(e)
            validation_report['summary'] = {'status': 'ERROR', 'total_validations': 0, 'passed': 0, 'failed': 1}

        return validation_report

    def _extract_baseline_info(self) -> Dict[str, Any]:
        """Extract key information from baseline data"""
        if not self.baseline_data:
            return {}

        return {
            'file_size_mb': self.baseline_data.get('file_info', {}).get('size_mb', 0),
            'total_slow_queries': self.baseline_data.get('event_counts', {}).get('slow_queries', 0),
            'total_connections': self.baseline_data.get('event_counts', {}).get('connections', 0),
            'total_authentications': self.baseline_data.get('event_counts', {}).get('authentications', 0),
            'unique_databases': self.baseline_data.get('unique_values', {}).get('databases', 0),
            'unique_collections': self.baseline_data.get('unique_values', {}).get('collections', 0),
            'time_range': self.baseline_data.get('time_range', {}),
            'analysis_timestamp': self.baseline_data.get('file_info', {}).get('analysis_timestamp')
        }

    def _validate_dashboard_stats(self) -> List[Dict[str, Any]]:
        """Validate dashboard statistics against baseline"""
        validations = []

        print("📊 Validating dashboard statistics...")

        # Get dashboard stats from API
        validation = self._create_validation_result("dashboard_api_stats", "Dashboard API statistics validation")

        try:
            api_url = urljoin(self.base_url, '/api/stats')
            response = self.session.get(api_url, timeout=10)

            if response.status_code == 200:
                ui_stats = response.json()
                baseline_stats = self.baseline_data.get('event_counts', {})

                # Validate key metrics
                validations_to_check = [
                    ('total_queries', 'slow_queries', 'Total slow queries count'),
                    ('unique_databases', 'databases', 'Unique databases count'),
                    ('unique_collections', 'collections', 'Unique collections count')
                ]

                accuracy_results = []

                for ui_key, baseline_key, description in validations_to_check:
                    ui_value = ui_stats.get(ui_key, 0)

                    if baseline_key in ['databases', 'collections']:
                        baseline_value = self.baseline_data.get('unique_values', {}).get(baseline_key, 0)
                    else:
                        baseline_value = baseline_stats.get(baseline_key, 0)

                    accuracy = self._calculate_accuracy(ui_value, baseline_value)
                    accuracy_results.append({
                        'metric': description,
                        'ui_value': ui_value,
                        'baseline_value': baseline_value,
                        'accuracy_percent': accuracy,
                        'within_tolerance': accuracy >= (100 - TestConfig.VALIDATION_TOLERANCE * 100)
                    })

                # Overall validation status
                all_within_tolerance = all(result['within_tolerance'] for result in accuracy_results)

                validation.update({
                    'status': TestStatus.PASS if all_within_tolerance else TestStatus.FAIL,
                    'accuracy_results': accuracy_results,
                    'overall_accuracy': sum(r['accuracy_percent'] for r in accuracy_results) / len(accuracy_results),
                    'ui_stats': ui_stats
                })

                if all_within_tolerance:
                    print("  ✅ Dashboard statistics validation passed")
                else:
                    print("  ⚠️  Dashboard statistics have accuracy issues")
                    validation['severity'] = TestSeverity.HIGH

            else:
                validation.update({
                    'status': TestStatus.FAIL,
                    'error': f"API request failed: {response.status_code}",
                    'severity': TestSeverity.HIGH
                })

        except Exception as e:
            validation.update({
                'status': TestStatus.ERROR,
                'error': str(e),
                'severity': TestSeverity.HIGH
            })

        validations.append(validation)
        return validations

    def _validate_slow_queries_data(self) -> List[Dict[str, Any]]:
        """Validate slow queries page data"""
        validations = []

        print("🐌 Validating slow queries data...")

        # Test pagination and count accuracy
        validation = self._create_validation_result("slow_queries_pagination", "Slow queries pagination accuracy")

        try:
            # Get first page to check total count
            queries_url = urljoin(self.base_url, '/slow-queries?per_page=100')
            response = self.session.get(queries_url, timeout=30)

            if response.status_code == 200:
                content = response.text

                # Extract pagination info
                pagination_info = self._extract_pagination_info(content)
                baseline_count = self.baseline_data.get('event_counts', {}).get('slow_queries', 0)

                if pagination_info['total_count'] > 0:
                    accuracy = self._calculate_accuracy(pagination_info['total_count'], baseline_count)

                    validation.update({
                        'status': TestStatus.PASS if accuracy >= 95 else TestStatus.FAIL,
                        'ui_total_count': pagination_info['total_count'],
                        'baseline_count': baseline_count,
                        'accuracy_percent': accuracy,
                        'pagination_info': pagination_info,
                        'within_tolerance': accuracy >= (100 - TestConfig.VALIDATION_TOLERANCE * 100)
                    })

                    if accuracy >= 95:
                        print(f"  ✅ Slow queries count validation passed: {pagination_info['total_count']} vs {baseline_count} baseline")
                    else:
                        print(f"  ⚠️  Slow queries count mismatch: {pagination_info['total_count']} vs {baseline_count} baseline")
                        validation['severity'] = TestSeverity.HIGH

                else:
                    validation.update({
                        'status': TestStatus.FAIL,
                        'error': "No slow queries found in UI",
                        'baseline_count': baseline_count,
                        'severity': TestSeverity.CRITICAL
                    })

            else:
                validation.update({
                    'status': TestStatus.FAIL,
                    'error': f"Slow queries page failed to load: {response.status_code}",
                    'severity': TestSeverity.HIGH
                })

        except Exception as e:
            validation.update({
                'status': TestStatus.ERROR,
                'error': str(e),
                'severity': TestSeverity.HIGH
            })

        validations.append(validation)

        # Validate individual query data accuracy
        validation = self._create_validation_result("query_data_accuracy", "Individual query data accuracy")

        try:
            # Get detailed view of first few queries
            queries_url = urljoin(self.base_url, '/slow-queries?per_page=10')
            response = self.session.get(queries_url, timeout=15)

            if response.status_code == 200:
                content = response.text

                # Extract query data from HTML (simplified validation)
                query_samples = self._extract_query_samples(content)
                baseline_samples = self.baseline_data.get('raw_data', {}).get('slow_queries', [])[:10]

                if query_samples and baseline_samples:
                    # Compare key fields
                    field_accuracy = self._compare_query_samples(query_samples, baseline_samples)

                    validation.update({
                        'status': TestStatus.PASS if field_accuracy['overall_accuracy'] >= 90 else TestStatus.FAIL,
                        'field_accuracy': field_accuracy,
                        'samples_compared': min(len(query_samples), len(baseline_samples))
                    })

                    if field_accuracy['overall_accuracy'] >= 90:
                        print(f"  ✅ Query data accuracy validation passed: {field_accuracy['overall_accuracy']:.1f}%")
                    else:
                        print(f"  ⚠️  Query data accuracy issues: {field_accuracy['overall_accuracy']:.1f}%")

                else:
                    validation.update({
                        'status': TestStatus.FAIL,
                        'error': "Could not extract query samples for comparison",
                        'severity': TestSeverity.MEDIUM
                    })

            else:
                validation.update({
                    'status': TestStatus.FAIL,
                    'error': f"Failed to get query details: {response.status_code}"
                })

        except Exception as e:
            validation.update({
                'status': TestStatus.ERROR,
                'error': str(e)
            })

        validations.append(validation)
        return validations

    def _validate_search_results(self) -> List[Dict[str, Any]]:
        """Validate search functionality accuracy"""
        validations = []

        print("🔍 Validating search results...")

        for test_case in TestConfig.SEARCH_TEST_CASES[:3]:  # Test first 3 search cases
            validation = self._create_validation_result(
                f"search_accuracy_{test_case['type']}",
                f"Search accuracy: {test_case['type']} = {test_case['value']}"
            )

            try:
                search_url = urljoin(self.base_url, '/search-logs')
                params = {test_case['type']: test_case['value']}

                response = self.session.get(search_url, params=params, timeout=30)

                if response.status_code == 200:
                    content = response.text
                    result_count = self._extract_result_count(content)

                    # Calculate expected count from baseline (simplified)
                    expected_count = self._calculate_expected_search_results(test_case, self.baseline_data)

                    if expected_count > 0:
                        accuracy = self._calculate_accuracy(result_count, expected_count)

                        validation.update({
                            'status': TestStatus.PASS if accuracy >= 80 else TestStatus.FAIL,
                            'search_params': params,
                            'ui_result_count': result_count,
                            'expected_count': expected_count,
                            'accuracy_percent': accuracy,
                            'within_tolerance': abs(result_count - expected_count) <= max(1, expected_count * 0.1)
                        })

                        if accuracy >= 80:
                            print(f"  ✅ Search {test_case['type']} accuracy: {result_count} vs ~{expected_count} expected")
                        else:
                            print(f"  ⚠️  Search {test_case['type']} inaccuracy: {result_count} vs ~{expected_count} expected")

                    else:
                        validation.update({
                            'status': TestStatus.SKIP,
                            'search_params': params,
                            'ui_result_count': result_count,
                            'note': "Cannot calculate expected count from baseline"
                        })

                else:
                    validation.update({
                        'status': TestStatus.FAIL,
                        'error': f"Search request failed: {response.status_code}",
                        'search_params': params
                    })

            except Exception as e:
                validation.update({
                    'status': TestStatus.ERROR,
                    'error': str(e),
                    'search_params': test_case
                })

            validations.append(validation)

        return validations

    def _validate_filter_functionality(self) -> List[Dict[str, Any]]:
        """Validate filtering functionality"""
        validations = []

        print("🗂️  Validating filter functionality...")

        for filter_case in TestConfig.FILTER_TEST_CASES:
            validation = self._create_validation_result(
                f"filter_{filter_case['filter_type']}",
                f"Filter validation: {filter_case['filter_type']}"
            )

            try:
                # Build filter parameters
                params = self._build_filter_params(filter_case)

                queries_url = urljoin(self.base_url, '/slow-queries')
                response = self.session.get(queries_url, params=params, timeout=30)

                if response.status_code == 200:
                    content = response.text
                    filtered_count = self._extract_pagination_info(content)['total_count']

                    # Basic validation: filtered count should be <= total count
                    baseline_total = self.baseline_data.get('event_counts', {}).get('slow_queries', 0)

                    if filtered_count <= baseline_total:
                        validation.update({
                            'status': TestStatus.PASS,
                            'filter_params': params,
                            'filtered_count': filtered_count,
                            'baseline_total': baseline_total,
                            'filter_ratio': (filtered_count / baseline_total * 100) if baseline_total > 0 else 0
                        })
                        print(f"  ✅ Filter {filter_case['filter_type']}: {filtered_count} results")

                    else:
                        validation.update({
                            'status': TestStatus.FAIL,
                            'error': f"Filtered count ({filtered_count}) exceeds baseline total ({baseline_total})",
                            'filter_params': params,
                            'severity': TestSeverity.HIGH
                        })

                else:
                    validation.update({
                        'status': TestStatus.FAIL,
                        'error': f"Filter request failed: {response.status_code}",
                        'filter_params': params
                    })

            except Exception as e:
                validation.update({
                    'status': TestStatus.ERROR,
                    'error': str(e),
                    'filter_params': filter_case
                })

            validations.append(validation)

        return validations

    def _validate_analysis_data(self) -> List[Dict[str, Any]]:
        """Validate analysis page data accuracy"""
        validations = []

        print("📈 Validating analysis data...")

        # Validate index suggestions
        validation = self._create_validation_result("index_suggestions", "Index suggestions analysis")

        try:
            suggestions_url = urljoin(self.base_url, '/index-suggestions')
            response = self.session.get(suggestions_url, timeout=30)

            if response.status_code == 200:
                content = response.text
                suggestions_count = self._count_index_suggestions(content)

                validation.update({
                    'status': TestStatus.PASS,
                    'suggestions_count': suggestions_count,
                    'has_suggestions': suggestions_count > 0,
                    'content_length': len(content)
                })
                print(f"  ✅ Index suggestions: {suggestions_count} found")

            else:
                validation.update({
                    'status': TestStatus.FAIL,
                    'error': f"Index suggestions page failed: {response.status_code}"
                })

        except Exception as e:
            validation.update({
                'status': TestStatus.ERROR,
                'error': str(e)
            })

        validations.append(validation)

        return validations

    def _validate_export_data(self) -> List[Dict[str, Any]]:
        """Validate export data accuracy"""
        validations = []

        print("📤 Validating export data...")

        # Test slow queries export
        validation = self._create_validation_result("export_slow_queries", "Slow queries export data validation")

        try:
            export_url = urljoin(self.base_url, '/export-slow-queries')
            response = self.session.get(export_url, timeout=60)

            if response.status_code == 200:
                export_data = response.json()

                if isinstance(export_data, list):
                    export_count = len(export_data)
                    baseline_count = self.baseline_data.get('event_counts', {}).get('slow_queries', 0)

                    accuracy = self._calculate_accuracy(export_count, baseline_count)

                    validation.update({
                        'status': TestStatus.PASS if accuracy >= 95 else TestStatus.FAIL,
                        'export_count': export_count,
                        'baseline_count': baseline_count,
                        'accuracy_percent': accuracy,
                        'data_structure_valid': True,
                        'within_tolerance': accuracy >= (100 - TestConfig.VALIDATION_TOLERANCE * 100)
                    })

                    if accuracy >= 95:
                        print(f"  ✅ Export data validation passed: {export_count} records")
                    else:
                        print(f"  ⚠️  Export count mismatch: {export_count} vs {baseline_count} baseline")
                        validation['severity'] = TestSeverity.MEDIUM

                else:
                    validation.update({
                        'status': TestStatus.FAIL,
                        'error': "Export data is not a list",
                        'data_type': type(export_data).__name__
                    })

            else:
                validation.update({
                    'status': TestStatus.FAIL,
                    'error': f"Export request failed: {response.status_code}"
                })

        except Exception as e:
            validation.update({
                'status': TestStatus.ERROR,
                'error': str(e)
            })

        validations.append(validation)
        return validations

    def _calculate_accuracy(self, ui_value: int, baseline_value: int) -> float:
        """Calculate accuracy percentage between UI and baseline values"""
        if baseline_value == 0:
            return 100.0 if ui_value == 0 else 0.0

        accuracy = (1 - abs(ui_value - baseline_value) / baseline_value) * 100
        return max(0.0, min(100.0, accuracy))

    def _extract_pagination_info(self, html_content: str) -> Dict[str, int]:
        """Extract pagination information from HTML"""
        pagination_info = {'total_count': 0, 'current_page': 1, 'per_page': 20}

        # Look for pagination patterns
        patterns = [
            r'Showing\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)',
            r'(\d+)\s+total\s+results',
            r'Page\s+(\d+)\s+of\s+(\d+)',
            r'(\d+)\s+entries'
        ]

        for pattern in patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                if len(match.groups()) >= 3:
                    pagination_info['total_count'] = int(match.group(3))
                elif len(match.groups()) >= 1:
                    pagination_info['total_count'] = int(match.group(1))
                break

        return pagination_info

    def _extract_result_count(self, html_content: str) -> int:
        """Extract search result count from HTML"""
        patterns = [
            r'(\d+)\s+results?\s+found',
            r'Found\s+(\d+)\s+results?',
            r'(\d+)\s+matches?',
            r'Showing\s+\d+\s+to\s+\d+\s+of\s+(\d+)',
            r'(\d+)\s+total'
        ]

        for pattern in patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                return int(match.group(1))

        return 0

    def _extract_query_samples(self, html_content: str) -> List[Dict[str, Any]]:
        """Extract query sample data from HTML (simplified)"""
        # This is a simplified extraction - in a real implementation,
        # you might use BeautifulSoup or similar to parse the HTML properly
        samples = []

        # Look for database names, durations, etc. in the content
        duration_matches = re.findall(r'(\d+)\s*ms', html_content)
        database_matches = re.findall(r'Database:\s*([a-zA-Z0-9_-]+)', html_content)

        for i, duration in enumerate(duration_matches[:10]):
            sample = {
                'duration': int(duration),
                'database': database_matches[i] if i < len(database_matches) else 'unknown'
            }
            samples.append(sample)

        return samples

    def _compare_query_samples(self, ui_samples: List[Dict], baseline_samples: List[Dict]) -> Dict[str, Any]:
        """Compare UI query samples with baseline samples"""
        if not ui_samples or not baseline_samples:
            return {'overall_accuracy': 0, 'field_accuracies': {}}

        field_accuracies = {}
        comparable_count = min(len(ui_samples), len(baseline_samples))

        # Compare durations
        duration_matches = 0
        for i in range(comparable_count):
            ui_duration = ui_samples[i].get('duration', 0)
            baseline_duration = baseline_samples[i].get('duration', 0)

            if abs(ui_duration - baseline_duration) <= max(1, baseline_duration * 0.05):  # 5% tolerance
                duration_matches += 1

        field_accuracies['duration'] = (duration_matches / comparable_count * 100) if comparable_count > 0 else 0

        overall_accuracy = sum(field_accuracies.values()) / len(field_accuracies) if field_accuracies else 0

        return {
            'overall_accuracy': overall_accuracy,
            'field_accuracies': field_accuracies,
            'samples_compared': comparable_count
        }

    def _calculate_expected_search_results(self, test_case: Dict, baseline_data: Dict) -> int:
        """Calculate expected search results from baseline data (simplified)"""
        # This is a simplified calculation - real implementation would
        # analyze baseline data more thoroughly
        search_type = test_case['type']
        search_value = test_case['value']

        if search_type == 'keyword':
            # Estimate based on keyword frequency in baseline data
            total_queries = baseline_data.get('event_counts', {}).get('slow_queries', 0)
            # Simple heuristic: common keywords like 'find' appear in ~50% of queries
            if search_value.lower() in ['find', 'query']:
                return int(total_queries * 0.5)
            else:
                return int(total_queries * 0.1)

        elif search_type == 'database':
            # Check if database exists in baseline
            databases = baseline_data.get('database_list', [])
            if search_value in databases:
                return int(baseline_data.get('event_counts', {}).get('slow_queries', 0) * 0.2)

        elif search_type == 'duration_min':
            # Estimate based on duration threshold
            duration_stats = baseline_data.get('performance_stats', {}).get('duration_ms', {})
            if duration_stats:
                threshold = int(search_value)
                avg_duration = duration_stats.get('avg', 0)
                if threshold < avg_duration:
                    return int(baseline_data.get('event_counts', {}).get('slow_queries', 0) * 0.5)

        return test_case.get('expected_min', 0)

    def _build_filter_params(self, filter_case: Dict) -> Dict[str, str]:
        """Build filter parameters for URL"""
        params = {}

        filter_type = filter_case['filter_type']

        if filter_type == 'duration':
            params['duration_min'] = str(filter_case['min_val'])
            params['duration_max'] = str(filter_case['max_val'])
        elif filter_type == 'docs_examined':
            params['docs_examined_min'] = str(filter_case['min_val'])
            params['docs_examined_max'] = str(filter_case['max_val'])
        elif filter_type == 'plan_summary':
            params['plan_summary'] = filter_case['value']

        return params

    def _count_index_suggestions(self, html_content: str) -> int:
        """Count index suggestions in HTML content"""
        # Look for suggestion patterns
        patterns = [
            r'CREATE INDEX',
            r'db\.\w+\.createIndex',
            r'ensureIndex',
            r'Index on'
        ]

        total_suggestions = 0
        for pattern in patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            total_suggestions += len(matches)

        return total_suggestions

    def _create_validation_result(self, validation_name: str, description: str) -> Dict[str, Any]:
        """Create a validation result template"""
        return {
            'validation_name': validation_name,
            'description': description,
            'timestamp': datetime.now().isoformat(),
            'status': TestStatus.SKIP,
            'severity': TestSeverity.INFO
        }

    def _calculate_validation_summary(self, validations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate summary statistics from validation results"""
        total = len(validations)
        passed = sum(1 for v in validations if v.get('status') == TestStatus.PASS)
        failed = sum(1 for v in validations if v.get('status') == TestStatus.FAIL)
        errors = sum(1 for v in validations if v.get('status') == TestStatus.ERROR)
        skipped = sum(1 for v in validations if v.get('status') == TestStatus.SKIP)

        accuracy_scores = [v.get('accuracy_percent', 0) for v in validations if 'accuracy_percent' in v]
        avg_accuracy = sum(accuracy_scores) / len(accuracy_scores) if accuracy_scores else 0

        critical_failures = sum(1 for v in validations if v.get('severity') == TestSeverity.CRITICAL)
        high_severity_issues = sum(1 for v in validations if v.get('severity') == TestSeverity.HIGH)

        return {
            'status': 'PASSED' if failed == 0 and errors == 0 and critical_failures == 0 else 'FAILED',
            'total_validations': total,
            'passed': passed,
            'failed': failed,
            'errors': errors,
            'skipped': skipped,
            'success_rate': round((passed / total * 100) if total > 0 else 0, 1),
            'average_accuracy': round(avg_accuracy, 1),
            'critical_failures': critical_failures,
            'high_severity_issues': high_severity_issues,
            'data_quality_score': round(avg_accuracy, 1) if avg_accuracy > 0 else 0
        }

    def save_validation_results(self, results: Dict[str, Any], output_path: str = None) -> str:
        """Save validation results to file"""
        if not output_path:
            timestamp = TestConfig.get_test_timestamp()
            output_path = f"{TestConfig.REPORTS_DIR}/data_validation_results_{timestamp}.json"

        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)

        print(f"💾 Data validation results saved to: {output_path}")
        return output_path


if __name__ == "__main__":
    # Example usage
    validator = DataValidator()

    # Load baseline data (example)
    baseline_path = f"{TestConfig.REPORTS_DIR}/baseline_data_latest.json"
    if os.path.exists(baseline_path):
        with open(baseline_path, 'r') as f:
            baseline_data = json.load(f)

        results = validator.validate_all_data(baseline_data)
        output_path = validator.save_validation_results(results)

        print(f"\n📋 Data Validation Summary:")
        summary = results['summary']
        print(f"   Status: {summary['status']}")
        print(f"   Validations: {summary['passed']}/{summary['total_validations']} passed")
        print(f"   Average Accuracy: {summary['average_accuracy']}%")
        print(f"   Results saved to: {output_path}")
    else:
        print(f"❌ Baseline data not found at: {baseline_path}")