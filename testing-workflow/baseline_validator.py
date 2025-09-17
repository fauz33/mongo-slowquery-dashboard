#!/usr/bin/env python3
"""
MongoDB Log Analyzer - Baseline Data Validator
Direct log file analysis to establish ground truth baseline for testing
"""

import json
import re
import gzip
import os
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Any
import hashlib

from test_config import TestConfig, EXPECTED_LOG_PATTERNS


class BaselineValidator:
    """Analyzes raw log files to create baseline data for validation"""

    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path
        self.baseline_data = {}
        self.patterns = {name: re.compile(pattern) for name, pattern in EXPECTED_LOG_PATTERNS.items()}

    def analyze_log_file(self) -> Dict[str, Any]:
        """
        Perform comprehensive analysis of the log file to create baseline data
        Returns detailed metrics for comparison with web UI
        """
        print(f"🔍 Analyzing log file: {self.log_file_path}")

        if not os.path.exists(self.log_file_path):
            raise FileNotFoundError(f"Log file not found: {self.log_file_path}")

        # Initialize counters
        slow_queries = []
        connections = []
        authentications = []
        line_count = 0
        error_lines = 0
        file_size = os.path.getsize(self.log_file_path)

        # Pattern counters
        pattern_counts = {name: 0 for name in self.patterns.keys()}

        # Data extraction counters
        databases = set()
        collections = set()
        namespaces = set()
        query_hashes = set()
        connection_ids = set()
        usernames = set()
        ip_addresses = set()

        # Time range tracking
        earliest_time = None
        latest_time = None

        # Performance metrics
        durations = []
        docs_examined = []
        docs_returned = []
        keys_examined = []

        # Plan summary analysis
        plan_summaries = Counter()

        print("📖 Reading and parsing log file...")

        try:
            # Handle both regular and gzipped files
            opener = gzip.open if self.log_file_path.endswith('.gz') else open

            with opener(self.log_file_path, 'rt', encoding='utf-8', errors='ignore') as file:
                for line_num, line in enumerate(file, 1):
                    line_count += 1
                    line = line.strip()

                    if not line or not line.startswith('{'):
                        continue

                    try:
                        # Count pattern matches
                        for pattern_name, pattern in self.patterns.items():
                            if pattern.search(line):
                                pattern_counts[pattern_name] += 1

                        # Parse JSON line
                        log_entry = json.loads(line)

                        # Extract timestamp
                        timestamp = self._extract_timestamp(log_entry)
                        if timestamp:
                            if earliest_time is None or timestamp < earliest_time:
                                earliest_time = timestamp
                            if latest_time is None or timestamp > latest_time:
                                latest_time = timestamp

                        # Process slow queries
                        if self._is_slow_query(line, log_entry):
                            slow_query = self._extract_slow_query_data(log_entry, line_num)
                            if slow_query:
                                slow_queries.append(slow_query)

                                # Aggregate metrics
                                databases.add(slow_query.get('database', ''))
                                collections.add(slow_query.get('collection', ''))
                                namespaces.add(slow_query.get('namespace', ''))

                                if slow_query.get('query_hash'):
                                    query_hashes.add(slow_query['query_hash'])

                                if slow_query.get('connection_id'):
                                    connection_ids.add(slow_query['connection_id'])

                                # Performance metrics
                                if slow_query.get('duration'):
                                    durations.append(slow_query['duration'])
                                if slow_query.get('docs_examined'):
                                    docs_examined.append(slow_query['docs_examined'])
                                if slow_query.get('docs_returned'):
                                    docs_returned.append(slow_query['docs_returned'])
                                if slow_query.get('keys_examined'):
                                    keys_examined.append(slow_query['keys_examined'])

                                # Plan summary
                                if slow_query.get('plan_summary'):
                                    plan_summaries[slow_query['plan_summary']] += 1

                        # Process connections
                        elif self._is_connection_event(line, log_entry):
                            connection = self._extract_connection_data(log_entry, line_num)
                            if connection:
                                connections.append(connection)
                                if connection.get('connection_id'):
                                    connection_ids.add(connection['connection_id'])
                                if connection.get('ip'):
                                    ip_addresses.add(connection['ip'])

                        # Process authentications
                        elif self._is_auth_event(line, log_entry):
                            auth = self._extract_auth_data(log_entry, line_num)
                            if auth:
                                authentications.append(auth)
                                if auth.get('username'):
                                    usernames.add(auth['username'])
                                if auth.get('ip'):
                                    ip_addresses.add(auth['ip'])

                    except (json.JSONDecodeError, Exception) as e:
                        error_lines += 1
                        continue

                    # Progress indicator
                    if line_count % 10000 == 0:
                        print(f"  📊 Processed {line_count:,} lines...")

        except Exception as e:
            print(f"❌ Error reading log file: {e}")
            raise

        # Calculate statistics
        self.baseline_data = self._calculate_baseline_stats(
            file_size, line_count, error_lines, pattern_counts,
            slow_queries, connections, authentications,
            databases, collections, namespaces, query_hashes,
            connection_ids, usernames, ip_addresses,
            earliest_time, latest_time, durations,
            docs_examined, docs_returned, keys_examined, plan_summaries
        )

        print(f"✅ Baseline analysis complete:")
        print(f"   📁 File size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
        print(f"   📄 Total lines: {line_count:,}")
        print(f"   🐌 Slow queries: {len(slow_queries):,}")
        print(f"   🔗 Connections: {len(connections):,}")
        print(f"   🔐 Authentications: {len(authentications):,}")
        print(f"   ❌ Error lines: {error_lines:,}")

        return self.baseline_data

    def _extract_timestamp(self, log_entry: dict) -> datetime:
        """Extract timestamp from log entry"""
        try:
            t = log_entry.get('t', {})
            if isinstance(t, dict) and '$date' in t:
                date_str = t['$date']
                if isinstance(date_str, str):
                    # Handle MongoDB ISO format
                    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return None
        except Exception:
            return None

    def _is_slow_query(self, line: str, log_entry: dict) -> bool:
        """Check if line represents a slow query"""
        return '"Slow query"' in line or log_entry.get('msg') == 'Slow query'

    def _is_connection_event(self, line: str, log_entry: dict) -> bool:
        """Check if line represents a connection event"""
        return ('"connection accepted"' in line or
                '"Connection accepted"' in line or
                log_entry.get('msg') in ['Connection accepted', 'connection accepted'])

    def _is_auth_event(self, line: str, log_entry: dict) -> bool:
        """Check if line represents an authentication event"""
        msg = log_entry.get('msg', '')
        return (msg in ['Successfully authenticated', 'Authentication succeeded', 'Authentication failed'] or
                '"Successfully authenticated"' in line or
                '"Authentication failed"' in line)

    def _extract_slow_query_data(self, log_entry: dict, line_num: int) -> dict:
        """Extract slow query data from log entry"""
        attr = log_entry.get('attr', {})
        ns = attr.get('ns', '')

        if '.' in ns:
            database, collection = ns.split('.', 1)
        else:
            database = collection = 'unknown'

        # Extract command for query text
        command = attr.get('command', {})

        return {
            'line_number': line_num,
            'timestamp': self._extract_timestamp(log_entry),
            'database': database,
            'collection': collection,
            'namespace': ns,
            'duration': attr.get('durationMillis', 0),
            'docs_examined': attr.get('docsExamined', 0),
            'docs_returned': attr.get('nReturned', 0) or attr.get('nreturned', 0) or attr.get('numReturned', 0),
            'keys_examined': attr.get('keysExamined', 0),
            'query_hash': attr.get('queryHash'),
            'plan_summary': attr.get('planSummary', ''),
            'connection_id': self._extract_connection_id(log_entry),
            'query_text': json.dumps(command) if command else '{}',
            'cpu_nanos': attr.get('cpuNanos', 0),
            'bytes_read': attr.get('storage', {}).get('data', {}).get('bytesRead', 0) or attr.get('bytesRead', 0),
            'bytes_written': attr.get('storage', {}).get('data', {}).get('bytesWritten', 0) or attr.get('bytesWritten', 0)
        }

    def _extract_connection_data(self, log_entry: dict, line_num: int) -> dict:
        """Extract connection data from log entry"""
        attr = log_entry.get('attr', {})
        remote = attr.get('remote', '')

        ip = port = ''
        if ':' in remote:
            ip, port = remote.rsplit(':', 1)

        return {
            'line_number': line_num,
            'timestamp': self._extract_timestamp(log_entry),
            'connection_id': self._extract_connection_id(log_entry),
            'ip': ip,
            'port': port,
            'remote': remote,
            'type': 'connection_accepted'
        }

    def _extract_auth_data(self, log_entry: dict, line_num: int) -> dict:
        """Extract authentication data from log entry"""
        attr = log_entry.get('attr', {})
        msg = log_entry.get('msg', '')

        # Determine auth result
        if msg in ['Successfully authenticated', 'Authentication succeeded']:
            result = 'auth_success'
        elif msg == 'Authentication failed':
            result = 'auth_failed'
        else:
            result = 'unknown'

        # Extract user info
        username = attr.get('user', attr.get('principalName', ''))
        if isinstance(username, dict):
            username = username.get('user', username.get('name', str(username)))

        return {
            'line_number': line_num,
            'timestamp': self._extract_timestamp(log_entry),
            'username': str(username) if username else '',
            'database': attr.get('db', attr.get('authenticationDatabase', '')),
            'result': result,
            'connection_id': self._extract_connection_id(log_entry),
            'mechanism': attr.get('mechanism', 'SCRAM-SHA-256'),
            'remote': attr.get('remote', ''),
            'ip': self._extract_ip(attr.get('remote', ''))
        }

    def _extract_connection_id(self, log_entry: dict) -> str:
        """Extract connection ID from log entry"""
        ctx = log_entry.get('ctx', '')
        if ctx.startswith('conn'):
            return ctx.replace('conn', '')
        return 'unknown'

    def _extract_ip(self, remote: str) -> str:
        """Extract IP address from remote string"""
        if ':' in remote:
            return remote.rsplit(':', 1)[0]
        return remote

    def _calculate_baseline_stats(self, file_size, line_count, error_lines, pattern_counts,
                                slow_queries, connections, authentications,
                                databases, collections, namespaces, query_hashes,
                                connection_ids, usernames, ip_addresses,
                                earliest_time, latest_time, durations,
                                docs_examined, docs_returned, keys_examined, plan_summaries) -> dict:
        """Calculate comprehensive baseline statistics"""

        # Performance statistics
        duration_stats = self._calculate_stats(durations) if durations else {}
        docs_examined_stats = self._calculate_stats(docs_examined) if docs_examined else {}
        docs_returned_stats = self._calculate_stats(docs_returned) if docs_returned else {}
        keys_examined_stats = self._calculate_stats(keys_examined) if keys_examined else {}

        return {
            'file_info': {
                'path': self.log_file_path,
                'size_bytes': file_size,
                'size_mb': round(file_size / 1024 / 1024, 2),
                'hash': self._calculate_file_hash(),
                'analysis_timestamp': datetime.now().isoformat()
            },
            'parsing_stats': {
                'total_lines': line_count,
                'error_lines': error_lines,
                'success_rate': round((line_count - error_lines) / line_count * 100, 2) if line_count > 0 else 0
            },
            'pattern_counts': pattern_counts,
            'event_counts': {
                'slow_queries': len(slow_queries),
                'connections': len(connections),
                'authentications': len(authentications),
                'total_events': len(slow_queries) + len(connections) + len(authentications)
            },
            'unique_values': {
                'databases': len(databases),
                'collections': len(collections),
                'namespaces': len(namespaces),
                'query_hashes': len(query_hashes),
                'connection_ids': len(connection_ids),
                'usernames': len(usernames),
                'ip_addresses': len(ip_addresses)
            },
            'time_range': {
                'earliest': earliest_time.isoformat() if earliest_time else None,
                'latest': latest_time.isoformat() if latest_time else None,
                'duration_hours': ((latest_time - earliest_time).total_seconds() / 3600) if earliest_time and latest_time else 0
            },
            'performance_stats': {
                'duration_ms': duration_stats,
                'docs_examined': docs_examined_stats,
                'docs_returned': docs_returned_stats,
                'keys_examined': keys_examined_stats
            },
            'plan_summaries': dict(plan_summaries.most_common(10)),
            'database_list': sorted(list(databases)),
            'collection_list': sorted(list(collections)),
            'namespace_list': sorted(list(namespaces)),
            'raw_data': {
                'slow_queries': slow_queries[:100],  # First 100 for validation
                'connections': connections[:100],
                'authentications': authentications[:100]
            }
        }

    def _calculate_stats(self, values: List[float]) -> dict:
        """Calculate statistical summary for numeric values"""
        if not values:
            return {}

        values = sorted(values)
        n = len(values)

        return {
            'count': n,
            'min': min(values),
            'max': max(values),
            'avg': round(sum(values) / n, 2),
            'median': values[n // 2],
            'p95': values[int(n * 0.95)] if n > 1 else values[0],
            'p99': values[int(n * 0.99)] if n > 1 else values[0]
        }

    def _calculate_file_hash(self) -> str:
        """Calculate SHA-256 hash of the log file"""
        hash_sha256 = hashlib.sha256()
        try:
            with open(self.log_file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception:
            return "unknown"

    def save_baseline(self, output_path: str = None) -> str:
        """Save baseline data to JSON file"""
        if not output_path:
            timestamp = TestConfig.get_test_timestamp()
            output_path = f"{TestConfig.REPORTS_DIR}/baseline_data_{timestamp}.json"

        with open(output_path, 'w') as f:
            json.dump(self.baseline_data, f, indent=2, default=str)

        print(f"💾 Baseline data saved to: {output_path}")
        return output_path

    def load_baseline(self, baseline_path: str) -> dict:
        """Load baseline data from JSON file"""
        with open(baseline_path, 'r') as f:
            self.baseline_data = json.load(f)
        return self.baseline_data


if __name__ == "__main__":
    # Example usage
    validator = BaselineValidator(TestConfig.TEST_LOG_FILE)
    baseline_data = validator.analyze_log_file()
    baseline_path = validator.save_baseline()
    print(f"\n📋 Baseline analysis complete. Data saved to: {baseline_path}")