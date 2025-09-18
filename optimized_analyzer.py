#!/usr/bin/env python3
"""
Optimized MongoDB Log Analyzer with Performance Improvements
Phase 1: JSON optimization + Memory allocation reduction
Phase 2: Chunked processing + Streaming analysis  
Phase 3: Multi-threading + Incremental analysis
"""

import json
import re
import os
import datetime
import time
import threading
import sqlite3
import mmap
from collections import defaultdict, deque, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import calendar
import queue

# Global Index Build Manager for handling background index creation with separate connections
class GlobalIndexBuildManager:
    """Global manager for background index builds with separate connections and queuing"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.index_queue = queue.Queue()
        self.worker_thread = None
        self.is_running = False
        self.worker_lock = threading.Lock()
        self._initialized = True
        print("🔧 Global Index Build Manager initialized")
    
    def queue_index_build(self, db_path, heavy_indexes, instance_id):
        """Queue index build request with separate connection"""
        task = {
            'db_path': db_path,
            'indexes': heavy_indexes,
            'instance_id': instance_id,
            'timestamp': time.time()
        }
        
        self.index_queue.put(task)
        print(f"📋 Queued index build for instance {instance_id}")
        
        # Start worker if not running
        with self.worker_lock:
            if not self.is_running:
                self._start_background_worker()
    
    def _start_background_worker(self):
        """Start global background index worker thread"""
        if self.is_running:
            return
            
        self.is_running = True
        self.worker_thread = threading.Thread(
            target=self._background_index_worker, 
            daemon=False,  # Non-daemon to ensure completion
            name="IndexBuildWorker"
        )
        self.worker_thread.start()
        print("🚀 Background index worker started")
    
    def _background_index_worker(self):
        """Background worker processing index queue with separate connections"""
        while True:
            try:
                # Wait for tasks with timeout
                task = self.index_queue.get(timeout=30)
                self._process_index_task(task)
                self.index_queue.task_done()
                
            except queue.Empty:
                # No tasks for 30 seconds, shutdown worker
                with self.worker_lock:
                    self.is_running = False
                print("💤 Background index worker shutting down (idle)")
                break
                
            except Exception as e:
                print(f"❌ Background index worker error: {e}")
                # Continue processing other tasks
    
    def _process_index_task(self, task):
        """Process single index creation task with dedicated connection"""
        bg_conn = None
        db_path = task['db_path']
        instance_id = task['instance_id']
        
        try:
            # Small delay to allow UI to stabilize first
            time.sleep(2)
            
            print(f"🔨 Background: Starting index creation for instance {instance_id}")
            
            # Create dedicated connection for this background operation
            bg_conn = sqlite3.connect(db_path, check_same_thread=False)
            bg_conn.execute('PRAGMA busy_timeout=60000')  # Higher timeout for background ops
            bg_conn.execute('PRAGMA journal_mode=WAL')
            bg_conn.execute('PRAGMA synchronous=NORMAL')  # Safe but not excessive
            
            # Add REGEXP function support for regex filtering
            import re
            def regexp(pattern, text):
                if text is None:
                    return False
                try:
                    return bool(re.search(pattern, str(text)))
                except:
                    return False
            bg_conn.create_function("REGEXP", 2, regexp)
            
            # Create all heavy indexes in this dedicated connection
            created_count = 0
            for idx_sql in task['indexes']:
                try:
                    bg_conn.execute(idx_sql)
                    created_count += 1
                except Exception as e:
                    print(f"⚠️  Background: Failed to create index: {e}")
                    # Continue with other indexes
            
            bg_conn.commit()
            print(f"✅ Background: Created {created_count}/{len(task['indexes'])} indexes for instance {instance_id}")
            
        except Exception as e:
            print(f"❌ Background: Index creation failed for instance {instance_id}: {e}")
            
        finally:
            if bg_conn:
                try:
                    bg_conn.close()
                except:
                    pass

# Global instance
_global_index_manager = GlobalIndexBuildManager()

# Pre-compiled regex patterns (Phase 1: Reduce allocations)
JSON_LINE_PATTERN = re.compile(r'^{.*}$')
NAMESPACE_PATTERN = re.compile(r'"ns":"([^"]+)"')
DURATION_PATTERN = re.compile(r'"durationMillis":(\d+)')
CONN_PATTERN = re.compile(r'conn(\d+)')

class OptimizedMongoLogAnalyzer:
    """Phase 1-3: Optimized MongoDB Log Analyzer"""
    
    @staticmethod
    def parse_timestamp_to_unix(timestamp_str):
        """Convert timestamp string to Unix epoch for faster comparisons"""
        if not timestamp_str:
            return None
            
        try:
            # Handle MongoDB's JSON timestamp format: {"$date":"2025-09-07T13:38:42.411-04:00"}
            if isinstance(timestamp_str, dict) and '$date' in timestamp_str:
                timestamp_str = timestamp_str['$date']
            
            # Handle ISO 8601 format with timezone
            if 'T' in timestamp_str:
                # Replace 'Z' with '+00:00' for UTC
                timestamp_str = timestamp_str.replace('Z', '+00:00')
                
                # Parse with timezone info
                if '+' in timestamp_str[-6:] or '-' in timestamp_str[-6:]:
                    dt = datetime.datetime.fromisoformat(timestamp_str)
                    # Convert to UTC naive datetime
                    dt = dt.replace(tzinfo=None) - dt.utcoffset()
                else:
                    dt = datetime.datetime.fromisoformat(timestamp_str)
            else:
                # Handle date-only format
                dt = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d')
            
            # Convert to Unix timestamp (integer)
            return int(calendar.timegm(dt.timetuple()))
            
        except (ValueError, TypeError, AttributeError):
            # Fallback: try to extract epoch from string if it's already a number
            try:
                return int(float(timestamp_str))
            except (ValueError, TypeError):
                return None
    
    @staticmethod  
    def unix_to_datetime_str(unix_timestamp):
        """Convert Unix timestamp back to readable string format"""
        if not unix_timestamp:
            return None
        try:
            dt = datetime.datetime.fromtimestamp(unix_timestamp)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError, OSError):
            return None
    
    @staticmethod
    def normalize_timestamp_for_comparison(timestamp):
        """Normalize timestamp for efficient database comparisons"""
        if isinstance(timestamp, int):
            return timestamp  # Already Unix timestamp
        elif isinstance(timestamp, str):
            return OptimizedMongoLogAnalyzer.parse_timestamp_to_unix(timestamp)
        elif isinstance(timestamp, datetime.datetime):
            return int(calendar.timegm(timestamp.timetuple()))
        else:
            return None
    
    def __init__(self, config=None):
        # Phase 1: Reduce memory allocations - use slots
        self.config = config or {
            'chunk_size': 50000,           # Phase 2: Chunked processing
            'max_workers': 4,              # Phase 3: Multi-threading
            'store_raw_lines': True,       # Configurable raw storage
            'progress_callback': None,     # Phase 2: Progress reporting
            'database_path': 'mongodb_analysis.db',  # Database file path
            'use_database': True           # Enable database mode by default for better performance
        }
        
        # Core data structures - connections and authentications still use memory for simplicity
        self.connections = []
        self.authentications = []
        self.database_access = []
        self.raw_log_data = {}
        
        # Query result caching mechanism
        self.query_cache = {}
        self.cache_ttl = 300  # 5 minutes cache TTL
        self.cache_max_size = 100  # Maximum number of cached queries
        
        # Phase 1: Pre-allocated dictionaries for better performance
        self.parsing_summary = {
            'total_lines': 0,
            'json_lines': 0,
            'text_lines': 0,
            'parsed_lines': 0,
            'error_lines': 0,
            'skipped_lines': 0,
            'connection_events': 0,
            'auth_events': 0,
            'command_events': 0,
            'slow_query_events': 0,
            'parsing_errors': [],
            'file_summaries': []
        }
        
        # Phase 3: Incremental analysis state
        self.incremental_stats = {
            'processed_files': set(),
            'collection_patterns': defaultdict(int),
            'hourly_trends': defaultdict(lambda: {'count': 0, 'total_duration': 0}),
            'query_signatures': {}
        }
        # Source files info for streaming (grep-like) search
        self.source_log_files = []
        self.source_total_bytes = 0

        # Phase 3: Always initialize database backend
        self.init_database()
        
    def init_database(self):
        """Phase 3: Initialize SQLite database for memory efficiency with optimized settings"""
        # Use persistent database file or in-memory based on config
        self.db_path = self.config.get('database_path', ':memory:')
        self.db_conn = sqlite3.connect(self.db_path, check_same_thread=False)
        
        # Add REGEXP function support for regex filtering
        import re
        def regexp(pattern, text):
            if text is None:
                return False
            try:
                return bool(re.search(pattern, str(text)))
            except:
                return False
        self.db_conn.create_function("REGEXP", 2, regexp)
        
        # Track bulk load state
        self.bulk_load_mode = False
        
        # Use normal settings during initialization - bulk load mode will be enabled later
        self.configure_database_for_operations()
        
        # Create enhanced schema with additional performance fields and OS metrics
        self.db_conn.execute('''
            CREATE TABLE IF NOT EXISTS slow_queries (
                id INTEGER PRIMARY KEY,
                timestamp TEXT,
                ts_epoch INTEGER DEFAULT NULL,
                database TEXT,
                collection TEXT,
                duration INTEGER,
                docs_examined INTEGER,
                docs_returned INTEGER,
                keys_examined INTEGER,
                query_hash TEXT,
                plan_summary TEXT,
                file_path TEXT,
                line_number INTEGER,
                namespace TEXT,
                query_text TEXT,
                connection_id TEXT,
                username TEXT,
                cpu_nanos INTEGER DEFAULT NULL,
                bytes_read INTEGER DEFAULT NULL,
                bytes_written INTEGER DEFAULT NULL,
                time_reading_micros INTEGER DEFAULT NULL,
                time_writing_micros INTEGER DEFAULT NULL
            )
        ''')
        
        # Create additional tables for better organization
        self.db_conn.execute('''
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY,
                conn_id TEXT,
                timestamp INTEGER NOT NULL,  -- Changed to INTEGER for better performance
                timestamp_str TEXT,          -- Keep original for compatibility
                action TEXT,
                details TEXT
            )
        ''')
        
        self.db_conn.execute('''
            CREATE TABLE IF NOT EXISTS authentications (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER NOT NULL,  -- Changed to INTEGER for better performance
                timestamp_str TEXT,          -- Keep original for compatibility
                user TEXT,
                database TEXT,
                result TEXT,
                connection_id TEXT,          -- Added connection_id column
                remote TEXT,                 -- Added remote field for IP:port
                mechanism TEXT               -- Added mechanism column
            )
        ''')
        
        # Initialize minimal indexes first, defer performance indexes until after bulk load
        self.create_minimal_indexes()
        self.db_conn.commit()
        self.db_lock = threading.Lock()
        
        # Dynamic batch size based on file size - optimization for large files
        self.batch_size = self.config.get('batch_size', 1000)
        self.dynamic_batch_size = 1000  # Will be adjusted based on file size
        self.slow_queries_batch = []
        self.connections_batch = []
        self.authentications_batch = []
        
        print(f"🗄️  Database initialized: {self.db_path}")
        if self.db_path != ':memory:':
            print(f"💾 Using persistent database for memory efficiency")
    
    def configure_database_for_bulk_load(self):
        """Configure database with optimized settings for bulk loading"""
        # OPTIMIZED settings for bulk loads (keep WAL mode to avoid locking issues)
        self.db_conn.execute('PRAGMA synchronous=OFF')       # No disk sync during bulk load
        self.db_conn.execute('PRAGMA cache_size=200000')      # 800MB cache for large loads - increased for better performance
        self.db_conn.execute('PRAGMA temp_store=MEMORY')
        self.db_conn.execute('PRAGMA busy_timeout=30000')     # 30s timeout to reduce lock contention
        self.db_conn.execute('PRAGMA wal_autocheckpoint=10000') # Larger WAL before auto-checkpoint
        self.db_conn.execute('PRAGMA mmap_size=1073741824')   # 1GB memory mapping
        self.bulk_load_mode = True
        print("🚀 Database configured for optimized bulk loading")
    
    def configure_database_for_operations(self):
        """Configure database for normal operation settings"""
        self.db_conn.execute('PRAGMA journal_mode=WAL')
        self.db_conn.execute('PRAGMA synchronous=NORMAL')
        self.db_conn.execute('PRAGMA locking_mode=NORMAL')
        self.db_conn.execute('PRAGMA cache_size=50000')      # 200MB cache for normal operations
        self.db_conn.execute('PRAGMA temp_store=MEMORY')
        self.db_conn.execute('PRAGMA mmap_size=536870912')   # 512MB memory mapping
        self.db_conn.execute('PRAGMA page_size=4096')
        self.bulk_load_mode = False
        self.heavy_indexes_created = False  # Track if heavy indexes are created
        print("🔄 Database configured for normal operations")
    
    def create_minimal_indexes(self):
        """Create no indexes during bulk load for maximum performance"""
        # Skip ALL index creation during bulk load to eliminate INSERT overhead
        # Tables will have only PRIMARY KEY constraints (auto-created)
        # All indexes (including basic timestamp) will be created in finish_bulk_load()
        print("📝 No indexes created during bulk load for optimal performance")
    
    def create_essential_indexes_only(self):
        """Create ONLY essential indexes immediately for fast UI response"""
        print("⚡ Creating essential indexes only (fast UI response)...")
        
        # ESSENTIAL INDEXES ONLY - for immediate UI functionality
        essential_indexes = [
            # Basic timestamp for general sorting (lightweight)
            'CREATE INDEX IF NOT EXISTS idx_basic_timestamp ON slow_queries(timestamp)',
            # Simple duration index for basic slow query filtering
            'CREATE INDEX IF NOT EXISTS idx_duration_desc ON slow_queries(duration DESC)',
            # Basic ts_epoch for time-based queries
            'CREATE INDEX IF NOT EXISTS idx_ts_epoch ON slow_queries(ts_epoch)',
            # Lightweight indexes for other tables
            'CREATE INDEX IF NOT EXISTS idx_conn_timestamp ON connections(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_auth_timestamp ON authentications(timestamp)'
        ]
        
        for idx_sql in essential_indexes:
            self.db_conn.execute(idx_sql)
        
        self.db_conn.commit()
        print("✅ Essential indexes created - UI ready in seconds!")
        
    def create_performance_indexes(self):
        """Create HEAVY composite indexes on-demand (called lazily when needed)"""
        print("🔨 Creating heavy composite performance indexes...")
        
        # HEAVY COMPOSITE INDEXES - created on-demand for better query performance
        heavy_indexes = [
            # Complex 4-column composite indexes (SLOW to create but fast to query)
            'CREATE INDEX IF NOT EXISTS idx_db_plan_dur_ts ON slow_queries(database, plan_summary, duration DESC, timestamp DESC)',
            'CREATE INDEX IF NOT EXISTS idx_db_coll_dur_ts ON slow_queries(database, collection, duration DESC, timestamp DESC)',
            'CREATE INDEX IF NOT EXISTS idx_ts_db_dur ON slow_queries(timestamp DESC, database, duration DESC)',
            
            # Specialized indexes for complex queries
            'CREATE INDEX IF NOT EXISTS idx_namespace ON slow_queries(namespace)',
            'CREATE INDEX IF NOT EXISTS idx_plan_summary ON slow_queries(plan_summary)',
            'CREATE INDEX IF NOT EXISTS idx_hash_dur ON slow_queries(query_hash, duration DESC)',
            'CREATE INDEX IF NOT EXISTS idx_timestamp_desc ON slow_queries(timestamp DESC)',
            'CREATE INDEX IF NOT EXISTS idx_auth_user ON authentications(user)'
        ]
        
        for idx_sql in heavy_indexes:
            self.db_conn.execute(idx_sql)
        
        self.db_conn.commit()
        print("✅ Heavy composite indexes created - queries now fully optimized")
    
    def get_heavy_indexes_list(self):
        """Get list of heavy indexes for background creation"""
        return [
            # Complex 4-column composite indexes (SLOW to create but fast to query)
            'CREATE INDEX IF NOT EXISTS idx_db_plan_dur_ts ON slow_queries(database, plan_summary, duration DESC, timestamp DESC)',
            'CREATE INDEX IF NOT EXISTS idx_db_coll_dur_ts ON slow_queries(database, collection, duration DESC, timestamp DESC)',
            'CREATE INDEX IF NOT EXISTS idx_ts_db_dur ON slow_queries(timestamp DESC, database, duration DESC)',
            
            # Specialized indexes for complex queries
            'CREATE INDEX IF NOT EXISTS idx_namespace ON slow_queries(namespace)',
            'CREATE INDEX IF NOT EXISTS idx_plan_summary ON slow_queries(plan_summary)',
            'CREATE INDEX IF NOT EXISTS idx_hash_dur ON slow_queries(query_hash, duration DESC)',
            'CREATE INDEX IF NOT EXISTS idx_timestamp_desc ON slow_queries(timestamp DESC)',
            'CREATE INDEX IF NOT EXISTS idx_auth_user ON authentications(user)'
        ]
    
    def ensure_heavy_indexes_created(self):
        """Lazily create heavy indexes when first needed for complex queries"""
        if not self.heavy_indexes_created:
            print("📝 First complex query detected - creating heavy indexes for optimal performance...")
            self.create_performance_indexes()
            self.heavy_indexes_created = True
    
    def create_fts_index(self):
        """Create FTS5 virtual table after bulk load"""
        try:
            self.db_conn.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS query_fts USING fts5(
                    query_text, namespace, 
                    content='slow_queries',
                    content_rowid='id'
                )
            ''')
            # Rebuild FTS index
            self.db_conn.execute("INSERT INTO query_fts(query_fts) VALUES('rebuild')")
            self.db_conn.commit()
            print("🔍 FTS5 full-text search index created")
        except Exception as e:
            print(f"⚠️  FTS5 index creation failed: {e}")
    
    def adjust_batch_size_for_file(self, file_size_mb):
        """Dynamically adjust batch size based on file size"""
        if file_size_mb > 1000:  # > 1GB
            self.dynamic_batch_size = 50000
        elif file_size_mb > 100:   # > 100MB
            self.dynamic_batch_size = 10000
        elif file_size_mb > 10:    # > 10MB
            self.dynamic_batch_size = 5000
        else:
            self.dynamic_batch_size = 1000
        
        print(f"📊 Adjusted batch size to {self.dynamic_batch_size} for {file_size_mb:.1f}MB file")
    
    def start_bulk_load(self, file_size_mb=0):
        """Initialize optimized bulk loading"""
        self.configure_database_for_bulk_load()
        self.adjust_batch_size_for_file(file_size_mb)
        # Start a transaction for the entire bulk load
        self.db_conn.execute('BEGIN IMMEDIATE')
        print(f"🚀 Bulk load started with batch size {self.dynamic_batch_size}")
    
    def finish_bulk_load(self):
        """Complete bulk loading and restore normal operations with optimized background indexing"""
        # Commit the entire transaction
        self.db_conn.commit()
        
        # Restore normal database settings
        self.configure_database_for_operations()
        
        # PERFORMANCE FIX: Create only ESSENTIAL indexes immediately for fast UI response
        self.create_essential_indexes_only()
        
        # OPTIMIZED: Queue heavy index creation with separate connection through global manager
        global _global_index_manager
        heavy_indexes = self.get_heavy_indexes_list()
        instance_id = id(self)
        
        _global_index_manager.queue_index_build(
            db_path=self.db_path,
            heavy_indexes=heavy_indexes,
            instance_id=instance_id
        )
        
        print("⚡ Bulk load completed - UI ready immediately!")
        print("🔨 Heavy indexes queued for background creation with separate connection...")
    
    def flush_batches(self, final_flush=False):
        """Flush all batch buffers to database"""
        # Only flush if database is enabled and initialized
        if not hasattr(self, 'db_lock') or not hasattr(self, 'db_conn'):
            return
        
        with self.db_lock:
            if self.slow_queries_batch:
                # Use backward-compatible INSERT that works with both old and new data formats
                self.insert_slow_queries_batch(self.slow_queries_batch)
                self.slow_queries_batch.clear()
                
            if self.connections_batch:
                self.db_conn.executemany('''
                    INSERT INTO connections (conn_id, timestamp, timestamp_str, action, details)
                    VALUES (?, ?, ?, ?, ?)
                ''', self.connections_batch)
                self.connections_batch.clear()
                
            if self.authentications_batch:
                self.db_conn.executemany('''
                    INSERT INTO authentications (timestamp, timestamp_str, user, database, result, connection_id, remote, mechanism)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', self.authentications_batch)
                self.authentications_batch.clear()
                
            # PERFORMANCE OPTIMIZATION: Only commit on final flush or when explicitly requested
            # This reduces I/O overhead during bulk loading
            if final_flush or getattr(self, '_force_commits', False):
                self.db_conn.commit()
    
    def insert_slow_queries_batch(self, batch):
        """Insert slow queries with backward compatibility for different tuple sizes"""
        if not batch:
            return
        
        # Check the size of the first tuple to determine format
        first_item = batch[0] if batch else None
        if first_item is None:
            return
        
        try:
            # Handle different tuple sizes
            if len(first_item) == 21:  # New format with OS resource metrics
                self.db_conn.executemany('''
                    INSERT INTO slow_queries 
                    (timestamp, ts_epoch, database, collection, duration, docs_examined, docs_returned,
                     keys_examined, query_hash, plan_summary, file_path, 
                     line_number, namespace, query_text, connection_id, username,
                     cpu_nanos, bytes_read, bytes_written, time_reading_micros, time_writing_micros)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch)
                print(f"✅ Inserted {len(batch)} entries with OS resource metrics (cpu_nanos, bytes_read, etc.)")
            elif len(first_item) == 16:  # New format with ts_epoch, connection_id and username (no query_pattern)
                self.db_conn.executemany('''
                    INSERT INTO slow_queries 
                    (timestamp, ts_epoch, database, collection, duration, docs_examined, docs_returned,
                     keys_examined, query_hash, plan_summary, file_path, 
                     line_number, namespace, query_text, connection_id, username)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch)
                print(f"✅ Inserted {len(batch)} entries with ts_epoch, connection_id and username")
            elif len(first_item) == 17:  # Legacy format with query_pattern (backward compatibility)
                # Remove query_pattern from legacy tuple and insert
                batch_without_pattern = [tuple(item[:8] + item[9:]) for item in batch]  # Skip position 8 (query_pattern)
                self.db_conn.executemany('''
                    INSERT INTO slow_queries 
                    (timestamp, ts_epoch, database, collection, duration, docs_examined, docs_returned,
                     keys_examined, query_hash, plan_summary, file_path, 
                     line_number, namespace, query_text, connection_id, username)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch_without_pattern)
                print(f"✅ Inserted {len(batch)} entries (legacy format, removed query_pattern)")
            elif len(first_item) == 15:  # Format with ts_epoch and connection_id (no username, no query_pattern)
                self.db_conn.executemany('''
                    INSERT INTO slow_queries 
                    (timestamp, ts_epoch, database, collection, duration, docs_examined, docs_returned,
                     keys_examined, query_hash, plan_summary, file_path, 
                     line_number, namespace, query_text, connection_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch)
                print(f"✅ Inserted {len(batch)} entries with ts_epoch and connection_id")
            elif len(first_item) == 14:  # Format with ts_epoch only
                self.db_conn.executemany('''
                    INSERT INTO slow_queries 
                    (timestamp, ts_epoch, database, collection, duration, docs_examined, docs_returned,
                     keys_examined, query_hash, plan_summary, file_path, 
                     line_number, namespace, query_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch)
                print(f"✅ Inserted {len(batch)} entries with ts_epoch")
            elif len(first_item) == 13:  # Old format without ts_epoch
                self.db_conn.executemany('''
                    INSERT INTO slow_queries 
                    (timestamp, database, collection, duration, docs_examined, docs_returned,
                     keys_examined, query_hash, plan_summary, file_path, 
                     line_number, namespace, query_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch)
                print(f"✅ Inserted {len(batch)} entries without ts_epoch")
            else:
                # Try to handle other tuple sizes gracefully
                print(f"⚠️  Unexpected tuple size: {len(first_item)}, skipping batch")
        except Exception as e:
            print(f"❌ Database insertion error: {e}")
            print(f"   Batch size: {len(batch)}")
            print(f"   First item type: {type(first_item)}")
            print(f"   First item length: {len(first_item) if hasattr(first_item, '__len__') else 'N/A'}")
            if batch:
                print(f"   Sample item: {first_item[:3] if hasattr(first_item, '__getitem__') else first_item}")
            import traceback
            traceback.print_exc()
    
    def add_slow_query_to_batch(self, query_data):
        """Add slow query to batch buffer with dynamic batch size"""
        self.slow_queries_batch.append(query_data)
        if len(self.slow_queries_batch) >= self.dynamic_batch_size:
            self.flush_batches()
    
    def add_connection_to_batch(self, conn_data):
        """Add connection to batch buffer with dynamic batch size"""
        self.connections_batch.append(conn_data)
        if len(self.connections_batch) >= self.dynamic_batch_size:
            self.flush_batches()
    
    def add_auth_to_batch(self, auth_data):
        """Add authentication to batch buffer with dynamic batch size"""
        self.authentications_batch.append(auth_data)
        if len(self.authentications_batch) >= self.dynamic_batch_size:
            self.flush_batches()
    
    def _normalize_timestamp(self, timestamp_str):
        """Normalize timestamp to naive datetime for consistent comparison"""
        if not timestamp_str:
            return None
        try:
            dt = datetime.datetime.fromisoformat(timestamp_str)
            # Convert to naive datetime if timezone-aware
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        except (ValueError, TypeError):
            return None
    
    def _format_timestamp_for_template(self, timestamp):
        """Format timestamp for template compatibility (prevents subscript errors)"""
        if timestamp and hasattr(timestamp, 'strftime'):
            return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        elif timestamp and isinstance(timestamp, str):
            return timestamp
        return None
    
    @property
    def slow_queries(self):
        """Returns slow queries from database"""
        queries = self.get_slow_queries_from_db()
        
        # Ensure all queries have query_hash (generate synthetic if missing)
        for query in queries:
            if not query.get('query_hash'):
                query['query_hash'] = self._generate_synthetic_query_hash(query)
        
        return queries
    
    @slow_queries.setter
    def slow_queries(self, value):
        """Setter for compatibility - database mode doesn't support direct setting"""
        # Database mode doesn't support direct setting of slow_queries
        pass
    
    @property
    def use_database(self):
        """Returns whether database mode is enabled"""
        return self.config.get('use_database', True)
    
    def _get_cache_key(self, query_type, **params):
        """Generate cache key for query results"""
        import hashlib
        key_data = f"{query_type}:{str(sorted(params.items()))}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_cached_result(self, cache_key):
        """Get cached result if still valid"""
        import time
        if cache_key in self.query_cache:
            cached_data, timestamp = self.query_cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return cached_data
            else:
                # Remove expired cache entry
                del self.query_cache[cache_key]
        return None
    
    def _cache_result(self, cache_key, result):
        """Cache query result with TTL"""
        import time
        # Implement LRU-like behavior: remove oldest entries if cache is full
        if len(self.query_cache) >= self.cache_max_size:
            oldest_key = min(self.query_cache.keys(), 
                           key=lambda k: self.query_cache[k][1])
            del self.query_cache[oldest_key]
        
        self.query_cache[cache_key] = (result, time.time())

    def _get_nested_field(self, obj, field_path):
        """Get nested field value using dot notation (e.g., 'attr.remote', 'command.find')"""
        if not field_path or not isinstance(obj, dict):
            return None
        cur = obj
        for part in str(field_path).split('.'): 
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    def _parse_epoch_from_log_entry(self, log_entry):
        """Extract epoch seconds from a parsed JSON log entry"""
        try:
            t = log_entry.get('t') if isinstance(log_entry, dict) else None
            return self.parse_timestamp_to_unix(t)
        except Exception:
            return None

    def _line_matches_conditions(self, raw_line, parsed_json, conditions):
        """Evaluate chained AND conditions against a line's raw and parsed content"""
        if not conditions:
            return True
        import re
        compiled = []
        for c in conditions:
            ctype = (c or {}).get('type', 'keyword')
            name = (c or {}).get('name', '')
            value = (c or {}).get('value', '')
            is_regex = bool((c or {}).get('regex'))
            case_sensitive = bool((c or {}).get('case_sensitive'))
            negate = bool((c or {}).get('negate'))
            flags = 0 if case_sensitive else re.IGNORECASE
            patt = None
            if is_regex and value:
                try:
                    patt = re.compile(value, flags)
                except re.error:
                    # Invalid regex: treat as no-match by forcing a pattern that never matches
                    patt = None
            compiled.append((ctype, name, value, patt, is_regex, case_sensitive, negate))

        for (ctype, name, value, patt, is_regex, case_sensitive, negate) in compiled:
            matched = False
            if ctype == 'field':
                field_val = self._get_nested_field(parsed_json or {}, name) if name else None
                field_str = '' if field_val is None else str(field_val)
                if is_regex and patt is not None:
                    matched = patt.search(field_str) is not None
                elif is_regex and patt is None:
                    matched = False
                else:
                    # substring
                    if case_sensitive:
                        matched = value in field_str
                    else:
                        matched = value.lower() in field_str.lower()
            else:  # keyword
                text = raw_line or ''
                if is_regex and patt is not None:
                    matched = patt.search(text) is not None
                elif is_regex and patt is None:
                    matched = False
                else:
                    matched = value in text if case_sensitive else (value.lower() in text.lower())

            if negate:
                matched = not matched
            if not matched:
                return False
        return True

    def search_logs_ephemeral(self, raw_map, keyword=None, field_name=None, field_value=None,
                               start_date=None, end_date=None, limit=100, conditions=None):
        """Search small datasets loaded into memory and return results with total counts."""
        results = []
        total_found = 0
        start_epoch = int(start_date.timestamp()) if start_date else None
        end_epoch = int(end_date.timestamp()) if end_date else None

        # Build effective conditions from legacy params
        eff_conditions = []
        if keyword:
            eff_conditions.append({'type': 'keyword', 'value': keyword, 'regex': False, 'case_sensitive': False, 'negate': False})
        if field_name and field_value:
            eff_conditions.append({'type': 'field', 'name': field_name, 'value': field_value, 'regex': False, 'case_sensitive': False, 'negate': False})
        if conditions:
            eff_conditions.extend(conditions)

        for file_path, lines in (raw_map or {}).items():
            for idx, line in enumerate(lines, 1):
                line_stripped = line.rstrip('\n')
                parsed_json = None
                epoch = None
                try:
                    parsed_json = json.loads(line_stripped)
                    epoch = self._parse_epoch_from_log_entry(parsed_json)
                except Exception:
                    parsed_json = None
                    epoch = None

                # Enforce date-first filtering
                if start_epoch is not None or end_epoch is not None:
                    if epoch is None:
                        continue
                    if start_epoch is not None and epoch < start_epoch:
                        continue
                    if end_epoch is not None and epoch > end_epoch:
                        continue

                # Chained condition checks
                if not self._line_matches_conditions(line_stripped, parsed_json, eff_conditions):
                    continue

                total_found += 1
                if len(results) < limit:
                    ts = datetime.datetime.fromtimestamp(epoch) if epoch is not None else None
                    results.append({
                        'file_path': file_path,
                        'line_number': idx,
                        'timestamp': ts,
                        'raw_line': line_stripped,
                    })

        return {'results': results, 'total_found': total_found}

    def search_logs_streaming(self, keyword=None, field_name=None, field_value=None,
                               start_date=None, end_date=None, limit=100, compute_total=False, conditions=None):
        """Stream over recorded files on disk, apply filters, and optionally compute totals."""
        results = []
        total_found = 0
        start_epoch = int(start_date.timestamp()) if start_date else None
        end_epoch = int(end_date.timestamp()) if end_date else None

        # Build effective conditions from legacy params
        eff_conditions = []
        if keyword:
            eff_conditions.append({'type': 'keyword', 'value': keyword, 'regex': False, 'case_sensitive': False, 'negate': False})
        if field_name and field_value:
            eff_conditions.append({'type': 'field', 'name': field_name, 'value': field_value, 'regex': False, 'case_sensitive': False, 'negate': False})
        if conditions:
            eff_conditions.extend(conditions)

        files = getattr(self, 'source_log_files', []) or []
        used_files = files if files else list((getattr(self, 'raw_log_data', {}) or {}).keys())

        for file_path in used_files:
            try:
                if files:
                    fh = open(file_path, 'r', encoding='utf-8', errors='ignore')
                    close_after = True
                    line_iter = fh
                else:
                    # Fallback to in-memory lines if available
                    lines = getattr(self, 'raw_log_data', {}).get(file_path, [])
                    line_iter = (l for l in lines)
                    close_after = False
                    fh = None
                
                for idx, line in enumerate(line_iter, 1):
                    line_stripped = line.rstrip('\n')
                    parsed_json = None
                    epoch = None
                    try:
                        parsed_json = json.loads(line_stripped)
                        epoch = self._parse_epoch_from_log_entry(parsed_json)
                    except Exception:
                        parsed_json = None
                        epoch = None

                    # Date-first filter
                    if start_epoch is not None or end_epoch is not None:
                        if epoch is None:
                            continue
                        if start_epoch is not None and epoch < start_epoch:
                            continue
                        if end_epoch is not None and epoch > end_epoch:
                            continue

                    # Conditions
                    if not self._line_matches_conditions(line_stripped, parsed_json, eff_conditions):
                        continue

                    total_found += 1
                    if len(results) < limit:
                        ts = datetime.datetime.fromtimestamp(epoch) if epoch is not None else None
                        results.append({
                            'file_path': file_path,
                            'line_number': idx,
                            'timestamp': ts,
                            'raw_line': line_stripped,
                        })

                if close_after and fh:
                    try:
                        fh.close()
                    except Exception:
                        pass
            except Exception:
                # Skip unreadable file
                continue

        return {'results': results, 'total_found': (total_found if compute_total else len(results))}
    
    def get_slow_queries_from_db(self, limit=None, database_filter=None):
        """Get slow queries from database with optional filters and caching"""
        if not hasattr(self, 'db_conn'):
            return []
        
        # Check cache first
        cache_key = self._get_cache_key('slow_queries', limit=limit, database_filter=database_filter)
        cached_result = self._get_cached_result(cache_key)
        if cached_result is not None:
            return cached_result
        
        query = '''
            SELECT timestamp, database, collection, duration, docs_examined, docs_returned,
                   keys_examined, query_hash, plan_summary, file_path, 
                   line_number, namespace, query_text, connection_id, username,
                   cpu_nanos, bytes_read, bytes_written, time_reading_micros, time_writing_micros
            FROM slow_queries
        '''
        params = []
        
        if database_filter:
            query += ' WHERE database NOT IN ({})'.format(','.join(['?' for _ in database_filter]))
            params.extend(database_filter)
        
        query += ' ORDER BY duration DESC'
        
        if limit:
            query += ' LIMIT ?'
            params.append(limit)
        
        cursor = self.db_conn.cursor()
        cursor.execute(query, params)
        
        # Convert database rows to dictionary format for compatibility
        slow_queries = []
        for row in cursor.fetchall():
            timestamp = self._normalize_timestamp(row[0])
            slow_queries.append({
                'timestamp': timestamp,
                'timestamp_formatted': self._format_timestamp_for_template(timestamp),
                'database': row[1],
                'collection': row[2],
                'duration': row[3],
                'docs_examined': row[4],
                'docs_returned': row[5],
                'keys_examined': row[6],
                # Add compatibility field names for JSON format
                'docsExamined': row[4],  # Map db field to JSON field
                'nReturned': row[5],     # Map db field to JSON field  
                'keysExamined': row[6],  # Map db field to JSON field
                'query_hash': row[7],    # Shifted from row[8]
                'plan_summary': row[8],  # Shifted from row[9]
                'file_path': row[9],     # Shifted from row[10]
                'line_number': row[10],  # Shifted from row[11]
                'namespace': row[11],    # Shifted from row[12]
                'query': row[12],        # Shifted from row[13]
                'connection_id': row[13] if len(row) > 13 else None,
                'username': row[14] if len(row) > 14 else None,
                # OS Resource Metrics
                'cpu_nanos': row[15] if len(row) > 15 else 0,
                'bytes_read': row[16] if len(row) > 16 else 0,
                'bytes_written': row[17] if len(row) > 17 else 0,
                'time_reading_micros': row[18] if len(row) > 18 else 0,
                'time_writing_micros': row[19] if len(row) > 19 else 0,
                'query_pattern': self._generate_query_pattern_from_text(row[12])  # Generate dynamically
            })
        
        # Cache the result before returning
        self._cache_result(cache_key, slow_queries)
        return slow_queries
    
    def get_slow_queries_filtered(self, threshold=100, database='all', plan_summary='all', 
                                start_date=None, end_date=None, page=1, per_page=100):
        """
        Optimized SQL-based filtering for slow queries with pagination
        Returns both filtered results and total count
        """
        if not hasattr(self, 'db_conn'):
            return {'items': [], 'total': 0, 'page': page, 'per_page': per_page, 'pages': 0}
        
        # Build WHERE clause
        where_conditions = ['duration >= ?']
        params = [threshold]
        
        # Database filter
        if database != 'all':
            where_conditions.append('database = ?')
            params.append(database)
        
        # Plan summary filter
        if plan_summary != 'all':
            if plan_summary == 'other':
                where_conditions.append('(plan_summary NOT IN (?, ?) OR plan_summary IS NULL)')
                params.extend(['COLLSCAN', 'IXSCAN'])
            else:
                where_conditions.append('plan_summary = ?')
                params.append(plan_summary)
        
        # Date filters - use numeric ts_epoch for better performance
        if start_date:
            try:
                from datetime import datetime
                start_epoch = int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp())
                where_conditions.append('ts_epoch >= ?')
                params.append(start_epoch)
            except (ValueError, AttributeError):
                # Fallback to string comparison
                where_conditions.append('timestamp >= ?')
                params.append(start_date)
        
        if end_date:
            try:
                from datetime import datetime
                end_epoch = int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp())
                where_conditions.append('ts_epoch <= ?')
                params.append(end_epoch)
            except (ValueError, AttributeError):
                # Fallback to string comparison
                where_conditions.append('timestamp <= ?')
                params.append(end_date)
        
        # Exclude system databases
        where_conditions.append('database NOT IN (?, ?, ?, ?, ?)')
        params.extend(['admin', 'local', 'config', '$external', 'unknown'])
        
        where_clause = 'WHERE ' + ' AND '.join(where_conditions) if where_conditions else ''
        
        # Get total count - SECURE: No string concatenation
        if where_conditions:
            count_query = "SELECT COUNT(*) FROM slow_queries WHERE " + " AND ".join(where_conditions)
        else:
            count_query = "SELECT COUNT(*) FROM slow_queries"
        
        cursor = self.db_conn.cursor()
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Calculate pagination - handle None per_page for unlimited results
        if per_page is None:
            total_pages = 1
            offset = 0
            per_page = total_count  # Return all results
        else:
            total_pages = (total_count + per_page - 1) // per_page
            offset = (page - 1) * per_page
        
        # Get filtered results with pagination - SECURE: No string concatenation
        base_query = '''
            SELECT timestamp, database, collection, duration, docs_examined, docs_returned,
                   keys_examined, query_hash, plan_summary, file_path, 
                   line_number, namespace, query_text, connection_id, username,
                   cpu_nanos, bytes_read, bytes_written, time_reading_micros, time_writing_micros
            FROM slow_queries'''
        
        if where_conditions:
            main_query = base_query + ' WHERE ' + ' AND '.join(where_conditions)
        else:
            main_query = base_query
            
        main_query += ' ORDER BY duration DESC, timestamp DESC'
        
        # Add pagination only if per_page is specified
        if per_page != total_count:  # per_page was set to total_count when None
            main_query += ' LIMIT ? OFFSET ?'
            cursor.execute(main_query, params + [per_page, offset])
        else:
            cursor.execute(main_query, params)
        
        # Convert to dictionary format
        items = []
        for row in cursor.fetchall():
            timestamp = self._normalize_timestamp(row[0])
            
            # Extract raw data (positions shifted after removing query_pattern column)
            database = row[1]
            collection = row[2] 
            duration = row[3]
            docs_examined = row[4]
            docs_returned = row[5]
            keys_examined = row[6]
            stored_hash = row[7]      # Shifted from row[8]
            plan_summary = row[8]     # Shifted from row[9]
            file_path = row[9]        # Shifted from row[10]
            line_number = row[10]     # Shifted from row[11]
            namespace = row[11]       # Shifted from row[12]
            query_text = row[12]      # Shifted from row[13]
            connection_id = row[13] if len(row) > 13 else None  # Shifted from row[14]
            username = row[14] if len(row) > 14 else None
            cpu_nanos = row[15] if len(row) > 15 else 0
            bytes_read = row[16] if len(row) > 16 else 0
            bytes_written = row[17] if len(row) > 17 else 0
            time_reading_micros = row[18] if len(row) > 18 else 0
            time_writing_micros = row[19] if len(row) > 19 else 0
            
            # Generate query pattern dynamically from query_text (since removed from DB)
            query_pattern = self._generate_query_pattern_from_text(query_text)
            
            # Fix missing query hash - generate from query data if needed  
            if not stored_hash:
                query_data = {
                    'database': database,
                    'collection': collection,
                    'query': query_text,
                    'plan_summary': plan_summary
                }
                query_hash = self._generate_synthetic_query_hash(query_data)
            else:
                query_hash = stored_hash
            
            # Use connection_id from database directly (already extracted from ctx during insertion)
            
            items.append({
                'timestamp': timestamp,
                'timestamp_formatted': self._format_timestamp_for_template(timestamp),
                'database': database,
                'collection': collection,
                'duration': duration,
                'docs_examined': docs_examined,
                'docs_returned': docs_returned,
                'keys_examined': keys_examined,
                'query_pattern': query_pattern,
                'query_hash': query_hash,
                'plan_summary': plan_summary,
                'file_path': file_path,
                'line_number': line_number,
                'namespace': namespace,
                'query_text': query_text,
                'connection_id': connection_id,
                'username': username,
                # OS Resource Metrics
                'cpu_nanos': cpu_nanos,
                'bytes_read': bytes_read,
                'bytes_written': bytes_written,
                'time_reading_micros': time_reading_micros,
                'time_writing_micros': time_writing_micros,
                # Add compatibility field names
                'docsExamined': docs_examined,
                'nReturned': docs_returned,
                'keysExamined': keys_examined,
            })
        
        return {
            'items': items,
            'total': total_count,
            'page': page,
            'per_page': per_page,
            'pages': total_pages,
            'has_prev': page > 1,
            'has_next': page < total_pages,
            'prev_num': page - 1 if page > 1 else None,
            'next_num': page + 1 if page < total_pages else None
        }
    
    def get_aggregated_patterns(self, threshold=100, database='all', plan_summary='all',
                              start_date=None, end_date=None, limit=1000):
        """
        SQL-based pre-aggregation for query patterns
        Much faster than Python grouping
        """
        if not hasattr(self, 'db_conn'):
            return {}
        
        # Build WHERE clause (same as filtering method)
        where_conditions = ['duration >= ?']
        params = [threshold]
        
        if database != 'all':
            where_conditions.append('database = ?')
            params.append(database)
        
        if plan_summary != 'all':
            if plan_summary == 'other':
                where_conditions.append('(plan_summary NOT IN (?, ?) OR plan_summary IS NULL)')
                params.extend(['COLLSCAN', 'IXSCAN'])
            else:
                where_conditions.append('plan_summary = ?')
                params.append(plan_summary)
        
        # Date filters - use numeric ts_epoch for better performance
        if start_date:
            try:
                from datetime import datetime
                start_epoch = int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp())
                where_conditions.append('ts_epoch >= ?')
                params.append(start_epoch)
            except (ValueError, AttributeError):
                # Fallback to string comparison
                where_conditions.append('timestamp >= ?')
                params.append(start_date)
        
        if end_date:
            try:
                from datetime import datetime
                end_epoch = int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp())
                where_conditions.append('ts_epoch <= ?')
                params.append(end_epoch)
            except (ValueError, AttributeError):
                # Fallback to string comparison
                where_conditions.append('timestamp <= ?')
                params.append(end_date)
        
        # Exclude system databases
        where_conditions.append('database NOT IN (?, ?, ?, ?, ?)')
        params.extend(['admin', 'local', 'config', '$external', 'unknown'])
        
        where_clause = 'WHERE ' + ' AND '.join(where_conditions)
        
        # Pre-aggregate using SQL with NULL handling to prevent NoneType errors
        # Use subquery to get query_text from the execution with highest duration
        aggregation_query = f'''
            SELECT 
                sq.query_hash,
                sq.database,
                sq.collection,
                sq.namespace,
                sq.plan_summary,
                COUNT(*) as execution_count,
                COALESCE(AVG(sq.duration), 0) as avg_duration,
                COALESCE(MIN(sq.duration), 0) as min_duration,
                COALESCE(MAX(sq.duration), 0) as max_duration,
                COALESCE(SUM(sq.duration), 0) as total_duration,
                COALESCE(AVG(sq.docs_examined), 0) as avg_docs_examined,
                COALESCE(AVG(sq.docs_returned), 0) as avg_docs_returned,
                COALESCE(AVG(sq.keys_examined), 0) as avg_keys_examined,
                MIN(sq.timestamp) as first_seen,
                MAX(sq.timestamp) as last_seen,
                (SELECT sq2.query_text FROM slow_queries sq2 
                 WHERE sq2.query_hash = sq.query_hash 
                 AND sq2.database = sq.database 
                 AND sq2.collection = sq.collection
                 ORDER BY sq2.duration DESC LIMIT 1) as query_text
            FROM slow_queries sq
            {where_clause}
            GROUP BY sq.query_hash, sq.database, sq.collection
            ORDER BY avg_duration DESC, execution_count DESC
            LIMIT ?
        '''
        
        cursor = self.db_conn.cursor()
        cursor.execute(aggregation_query, params + [limit])
        
        patterns = {}
        for row in cursor.fetchall():
            query_hash = row[0]
            patterns[query_hash] = {
                'query_hash': query_hash,
                'database': row[1],
                'collection': row[2],
                'namespace': row[3],
                'plan_summary': row[4],
                'execution_count': row[5],
                'avg_duration': row[6],
                'min_duration': row[7],
                'max_duration': row[8],
                'total_duration': row[9],
                'avg_docs_examined': row[10],
                'avg_docs_returned': row[11],
                'avg_keys_examined': row[12],
                'first_seen': self._normalize_timestamp(row[13]),
                'last_seen': self._normalize_timestamp(row[14]),
                'query_pattern': self._generate_query_pattern_from_text(row[15]),  # Generate from query_text
                'sample_query': row[15],  # Shifted from row[16]
                # For backward compatibility
                'executions': [{'duration': row[6]}] * min(row[5], 10)  # Synthetic data
            }
        
        return list(patterns.values())
    
    def get_aggregated_patterns_by_grouping(self, grouping='pattern_key', threshold=100, database='all',
                                          namespace='all', plan_summary='all', start_date=None, end_date=None,
                                          limit=1000):
        """
        SQL-based aggregation with different grouping strategies
        grouping options: 'pattern_key' (default), 'namespace', 'query_hash'
        """
        if not hasattr(self, 'db_conn'):
            return {}
        
        # Build WHERE clause (same as existing method)
        where_conditions = ['duration >= ?']
        params = [threshold]
        
        if database != 'all':
            where_conditions.append('database = ?')
            params.append(database)
        
        if plan_summary != 'all':
            if plan_summary == 'other':
                where_conditions.append('(plan_summary NOT IN (?, ?) OR plan_summary IS NULL)')
                params.extend(['COLLSCAN', 'IXSCAN'])
            else:
                where_conditions.append('plan_summary = ?')
                params.append(plan_summary)
        
        # Namespace filter
        if namespace and namespace != 'all':
            where_conditions.append('namespace = ?')
            params.append(namespace)

        # Date filters
        if start_date:
            try:
                from datetime import datetime
                start_epoch = int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp())
                where_conditions.append('ts_epoch >= ?')
                params.append(start_epoch)
            except (ValueError, AttributeError):
                where_conditions.append('timestamp >= ?')
                params.append(start_date)
        
        if end_date:
            try:
                from datetime import datetime
                end_epoch = int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp())
                where_conditions.append('ts_epoch <= ?')
                params.append(end_epoch)
            except (ValueError, AttributeError):
                where_conditions.append('timestamp <= ?')
                params.append(end_date)
        
        # Exclude system databases
        where_conditions.append('database NOT IN (?, ?, ?, ?, ?)')
        params.extend(['admin', 'local', 'config', '$external', 'unknown'])
        
        where_clause = 'WHERE ' + ' AND '.join(where_conditions)
        
        # Determine GROUP BY clause based on grouping strategy
        if grouping == 'namespace':
            group_by = 'sq.database, sq.collection'
            select_fields = '''
                'MIXED' as query_hash,
                sq.database,
                sq.collection,
                sq.namespace,
                'MIXED' as plan_summary,
            '''
        elif grouping == 'query_hash':
            group_by = 'sq.query_hash'
            select_fields = '''
                sq.query_hash,
                'MIXED' as database,
                'MIXED' as collection,
                'MIXED' as namespace,
                'MIXED' as plan_summary,
            '''
        else:  # pattern_key (default behavior)
            group_by = 'sq.query_hash, sq.database, sq.collection'
            select_fields = '''
                sq.query_hash,
                sq.database,
                sq.collection,
                sq.namespace,
                sq.plan_summary,
            '''
        
        # Build aggregation query
        aggregation_query = f'''
            SELECT 
                {select_fields}
                COUNT(*) as execution_count,
                COALESCE(AVG(sq.duration), 0) as avg_duration,
                COALESCE(MIN(sq.duration), 0) as min_duration,
                COALESCE(MAX(sq.duration), 0) as max_duration,
                COALESCE(SUM(sq.duration), 0) as total_duration,
                COALESCE(SUM(sq.docs_examined), 0) as sum_docs_examined,
                COALESCE(SUM(sq.docs_returned), 0) as sum_docs_returned,
                COALESCE(SUM(sq.keys_examined), 0) as sum_keys_examined,
                COALESCE(AVG(sq.docs_examined), 0) as avg_docs_examined,
                COALESCE(AVG(sq.docs_returned), 0) as avg_docs_returned,
                COALESCE(AVG(sq.keys_examined), 0) as avg_keys_examined,
                MIN(sq.timestamp) as first_seen,
                MAX(sq.timestamp) as last_seen,
                (SELECT sq2.query_text FROM slow_queries sq2 
                 WHERE {self._get_grouping_join_condition(grouping, 'sq2', 'sq')}
                 ORDER BY sq2.duration DESC LIMIT 1) as query_text
            FROM slow_queries sq
            {where_clause}
            GROUP BY {group_by}
            ORDER BY avg_duration DESC, execution_count DESC
            LIMIT ?
        '''
        
        cursor = self.db_conn.cursor()
        cursor.execute(aggregation_query, params + [limit])
        
        patterns = {}
        for row in cursor.fetchall():
            # Create unique key based on grouping strategy
            if grouping == 'namespace':
                key = f"{row[1]}.{row[2]}"  # database.collection
                display_key = f"Namespace: {key}"
            elif grouping == 'query_hash':
                key = row[0]  # query_hash
                display_key = f"Query Hash: {key}"
            else:  # pattern_key
                key = f"{row[0]}_{row[1]}_{row[2]}"  # query_hash_database_collection
                display_key = f"Pattern: {row[0]}"
                
            # Create proper namespace for display
            if grouping == 'namespace':
                computed_namespace = f"{row[1]}.{row[2]}" if row[1] and row[2] else row[3] or ''
            elif grouping == 'query_hash':
                computed_namespace = 'MIXED'  # Cleaner display for query_hash grouping
            else:
                computed_namespace = row[3] or f"{row[1]}.{row[2]}" if row[1] and row[2] else ''
            
            operation_type = self._extract_operation_type_from_query_text(row[16])

            patterns[key] = {
                'query_hash': row[0],
                'database': row[1],
                'collection': row[2],
                'namespace': computed_namespace,
                'plan_summary': row[4],
                'execution_count': row[5],
                'avg_duration': row[6],
                'min_duration': row[7],
                'max_duration': row[8],
                'total_duration': row[9],
                'sum_docs_examined': row[10],
                'sum_docs_returned': row[11],
                'sum_keys_examined': row[12],
                'avg_docs_examined': row[13],
                'avg_docs_returned': row[14],
                'avg_keys_examined': row[15],
                'first_seen': self._normalize_timestamp(row[16]),
                'last_seen': self._normalize_timestamp(row[17]),
                'query_pattern': self._generate_query_pattern_from_text(row[18]),
                'sample_query': row[18],
                'grouping_type': grouping,
                'display_key': display_key,
                'executions': [],
                'operation_type': operation_type,
                'group_key': key
            }

        return list(patterns.values())
    
    def _get_grouping_join_condition(self, grouping, table1, table2):
        """Helper to get JOIN condition for subquery based on grouping"""
        if grouping == 'namespace':
            return f"{table1}.database = {table2}.database AND {table1}.collection = {table2}.collection"
        elif grouping == 'query_hash':
            return f"{table1}.query_hash = {table2}.query_hash"
        else:  # pattern_key
            return f"{table1}.query_hash = {table2}.query_hash AND {table1}.database = {table2}.database AND {table1}.collection = {table2}.collection"

    def _extract_operation_type_from_query_text(self, query_text):
        """Infer operation type from stored query text"""
        if not query_text:
            return 'unknown'

        try:
            if isinstance(query_text, str):
                parsed = json.loads(query_text)
            elif isinstance(query_text, dict):
                parsed = query_text
            else:
                return 'unknown'

            if not isinstance(parsed, dict):
                return 'unknown'

            # Direct command keys
            operation_candidates = ['findAndModify', 'aggregate', 'find', 'update', 'delete', 'insert', 'distinct', 'count', 'explain']
            for candidate in operation_candidates:
                if candidate in parsed:
                    return 'update' if candidate == 'findAndModify' else candidate

            # Some slow queries store structure under "command"
            command_obj = parsed.get('command') if isinstance(parsed.get('command'), dict) else None
            if command_obj:
                for candidate in operation_candidates:
                    if candidate in command_obj:
                        return 'update' if candidate == 'findAndModify' else candidate

                # Generic command name fallback
                op_name = command_obj.get('commandName') or command_obj.get('operation')
                if isinstance(op_name, str) and op_name:
                    return op_name.lower()

            # Mongosh auth commands often expose "saslStart"
            if 'saslStart' in parsed or (command_obj and 'saslStart' in command_obj):
                return 'saslStart'

            return 'unknown'

        except Exception:
            return 'unknown'

    def _get_trend_executions_sql(self, grouping='pattern_key', database='all', namespace='all', limit=2000):
        """Fetch execution samples for trend visualisations using SQL backend"""
        if not self.use_database or not hasattr(self, 'db_conn'):
            return []

        where_conditions = ['duration > 0']
        params = []

        if database and database != 'all':
            where_conditions.append('database = ?')
            params.append(database)

        if namespace and namespace != 'all':
            where_conditions.append('namespace = ?')
            params.append(namespace)

        # Avoid system databases unless specifically requested
        if not (database and database not in ('all', None)):
            where_conditions.append('database NOT IN (?, ?, ?, ?, ?)')
            params.extend(['admin', 'local', 'config', '$external', 'unknown'])

        where_clause = 'WHERE ' + ' AND '.join(where_conditions)

        query = f'''
            SELECT timestamp, duration, query_hash, database, collection, namespace,
                   plan_summary, query_text
            FROM slow_queries
            {where_clause}
            ORDER BY timestamp
            LIMIT ?
        '''

        params.append(limit)

        cursor = self.db_conn.cursor()
        cursor.execute(query, params)

        executions = []
        for row in cursor.fetchall():
            timestamp = self._normalize_timestamp(row[0])
            operation_type = self._extract_operation_type_from_query_text(row[7])

            if grouping == 'namespace':
                group_key = row[5] or f"{row[3]}.{row[4]}"
            elif grouping == 'query_hash':
                group_key = row[2]
            else:
                group_key = f"{row[2]}_{row[3]}_{row[4]}"

            executions.append({
                'timestamp': timestamp.isoformat() if timestamp else row[0],
                'duration': row[1],
                'query_hash': row[2],
                'database': row[3],
                'collection': row[4],
                'namespace': row[5] or f"{row[3]}.{row[4]}",
                'plan_summary': row[6],
                'operation_type': operation_type,
                'query_text': row[7],
                'group_key': group_key
            })

        return executions

    def _summarise_pattern_from_executions(self, group_key, executions, operation_type='unknown'):
        """Build aggregated metrics from a list of execution documents"""
        if not executions:
            return None

        durations = [e.get('duration', 0) for e in executions if e.get('duration') is not None]
        total_duration = sum(durations)
        min_duration = min(durations) if durations else 0
        max_duration = max(durations) if durations else 0
        avg_duration = total_duration / len(durations) if durations else 0

        database = executions[0].get('database', 'unknown')
        collection = executions[0].get('collection', 'unknown')
        namespace = executions[0].get('namespace', f"{database}.{collection}")
        plan_summary = executions[0].get('plan_summary', 'None')
        query_hash = executions[0].get('query_hash', 'unknown')

        sum_docs_examined = 0
        sum_docs_returned = 0
        sum_keys_examined = 0

        for execution in executions:
            sum_docs_examined += execution.get('docsExamined') or execution.get('docs_examined') or 0
            sum_docs_returned += execution.get('nReturned') or execution.get('docs_returned') or 0
            sum_keys_examined += execution.get('keysExamined') or execution.get('keys_examined') or 0

        avg_docs_examined = sum_docs_examined / len(executions) if executions else 0
        avg_docs_returned = sum_docs_returned / len(executions) if executions else 0
        avg_keys_examined = sum_keys_examined / len(executions) if executions else 0

        timestamps = [execution.get('timestamp') for execution in executions if execution.get('timestamp')]
        first_seen = min(timestamps) if timestamps else None
        last_seen = max(timestamps) if timestamps else None

        sample_query = None
        for execution in executions:
            candidate = execution.get('query') or execution.get('query_text')
            if candidate:
                sample_query = candidate
                break

        return {
            'group_key': group_key,
            'query_hash': query_hash,
            'database': database,
            'collection': collection,
            'namespace': namespace,
            'plan_summary': plan_summary,
            'execution_count': len(executions),
            'avg_duration': avg_duration,
            'min_duration': min_duration,
            'max_duration': max_duration,
            'total_duration': total_duration,
            'sum_docs_examined': sum_docs_examined,
            'sum_docs_returned': sum_docs_returned,
            'sum_keys_examined': sum_keys_examined,
            'avg_docs_examined': avg_docs_examined,
            'avg_docs_returned': avg_docs_returned,
            'avg_keys_examined': avg_keys_examined,
            'first_seen': first_seen,
            'last_seen': last_seen,
            'sample_query': sample_query,
            'operation_type': operation_type,
            'display_key': group_key,
            'grouping_type': 'pattern_key',
            'executions': []
        }

    def _get_query_trend_dataset_fallback(self, grouping='pattern_key', database='all', namespace='all',
                                          limit=1000, scatter_limit=2000):
        """Compute query trend data without SQL backend"""
        patterns = self.analyze_query_patterns()

        filtered = {}
        for pattern_key, pattern in patterns.items():
            db_name = pattern.get('database', 'unknown')
            coll = pattern.get('collection', 'unknown')
            ns = f"{db_name}.{coll}"

            if database != 'all' and db_name != database:
                continue
            if namespace != 'all' and ns != namespace:
                continue

            filtered[pattern_key] = pattern

        grouped = {}

        def add_to_group(key, pattern_data):
            execs = pattern_data.get('executions', [])
            if not execs:
                return

            operation = pattern_data.get('operation_type') or self._extract_operation_type_from_query_text(
                pattern_data.get('sample_query')
            )

            if key not in grouped:
                grouped[key] = self._summarise_pattern_from_executions(
                    key,
                    execs,
                    operation_type=operation
                )
            else:
                existing_execs = grouped[key].get('_all_execs', [])
                if not existing_execs:
                    existing_execs = []
                existing_execs.extend(execs)
                summary = self._summarise_pattern_from_executions(key, existing_execs, operation)
                if summary:
                    summary['_all_execs'] = existing_execs
                    grouped[key] = summary

        if grouping == 'namespace':
            for pattern_data in filtered.values():
                ns = f"{pattern_data.get('database', 'unknown')}.{pattern_data.get('collection', 'unknown')}"
                add_to_group(ns, pattern_data)
        elif grouping == 'query_hash':
            for pattern_data in filtered.values():
                add_to_group(pattern_data.get('query_hash', 'unknown'), pattern_data)
        else:
            for key, pattern_data in filtered.items():
                add_to_group(key, pattern_data)

        patterns_list = []
        for key, summary in grouped.items():
            if summary:
                if '_all_execs' in summary:
                    summary.pop('_all_execs')
                summary['group_key'] = key
                summary['grouping_type'] = grouping
                patterns_list.append(summary)

        patterns_list.sort(key=lambda x: x.get('total_duration', 0), reverse=True)

        all_executions = []
        for pattern_data in filtered.values():
            all_executions.extend(pattern_data.get('executions', []))

        return {
            'patterns': patterns_list[:limit],
            'executions': all_executions[:scatter_limit]
        }

    def get_query_trend_dataset(self, grouping='pattern_key', database='all', namespace='all',
                                 limit=1000, scatter_limit=2000):
        """Provide aggregated and execution-level data for query trend visualisations"""
        cache_key = self._get_cache_key(
            'query_trend_dataset',
            grouping=grouping,
            database=database,
            namespace=namespace,
            limit=limit,
            scatter_limit=scatter_limit
        )

        cached = self._get_cached_result(cache_key)
        if cached:
            return cached

        if self.use_database and hasattr(self, 'db_conn'):
            patterns = self.get_aggregated_patterns_by_grouping(
                grouping=grouping,
                database=database,
                namespace=namespace,
                limit=limit
            )
            executions = self._get_trend_executions_sql(
                grouping=grouping,
                database=database,
                namespace=namespace,
                limit=scatter_limit
            )

            dataset = {
                'patterns': patterns,
                'executions': executions
            }
        else:
            dataset = self._get_query_trend_dataset_fallback(
                grouping=grouping,
                database=database,
                namespace=namespace,
                limit=limit,
                scatter_limit=scatter_limit
            )

        self._cache_result(cache_key, dataset)
        return dataset

    def populate_fts_index(self):
        """Populate FTS index with existing query data"""
        if hasattr(self, 'db_conn'):
            with self.db_lock:
                # Insert data into FTS table from existing slow_queries
                self.db_conn.execute('''
                    INSERT INTO query_fts(query_fts) VALUES('rebuild');
                ''')
                self.db_conn.commit()
                
    def search_queries_fts(self, search_term, threshold=100, page=1, per_page=100):
        """
        Full-text search in query text using FTS5
        Returns results matching search term with pagination
        """
        if not hasattr(self, 'db_conn') or not search_term.strip():
            return [], 0
        
        # Sanitize search term for FTS5
        search_term = search_term.replace('"', '""').strip()
        
        # Build FTS query - search in query_text and namespace (query_pattern removed)
        fts_query = f'query_text:"{search_term}" OR namespace:"{search_term}"'
        
        # Get total count
        count_query = '''
            SELECT COUNT(*)
            FROM query_fts 
            JOIN slow_queries ON query_fts.rowid = slow_queries.id
            WHERE query_fts MATCH ? 
            AND slow_queries.duration >= ?
            AND slow_queries.database NOT IN ('admin', 'local', 'config', '$external', 'unknown')
        '''
        
        cursor = self.db_conn.cursor()
        cursor.execute(count_query, (fts_query, threshold))
        total_count = cursor.fetchone()[0]
        
        # Get paginated results
        offset = (page - 1) * per_page
        search_query = '''
            SELECT slow_queries.*, rank
            FROM query_fts 
            JOIN slow_queries ON query_fts.rowid = slow_queries.id
            WHERE query_fts MATCH ? 
            AND slow_queries.duration >= ?
            AND slow_queries.database NOT IN ('admin', 'local', 'config', '$external', 'unknown')
            ORDER BY rank, slow_queries.duration DESC
            LIMIT ? OFFSET ?
        '''
        
        cursor.execute(search_query, (fts_query, threshold, per_page, offset))
        rows = cursor.fetchall()
        
        # Convert to dictionary format
        columns = [description[0] for description in cursor.description]
        results = []
        for row in rows:
            query_dict = dict(zip(columns, row))
            # Convert timestamp back to datetime if needed
            if query_dict.get('timestamp'):
                try:
                    from datetime import datetime
                    query_dict['timestamp'] = datetime.fromisoformat(query_dict['timestamp'])
                except:
                    pass
            results.append(query_dict)
        
        return results, total_count
    
    def clear_database_data(self):
        """Clear all data and indexes from database tables using DROP+CREATE for zero-index optimization"""
        if hasattr(self, 'db_conn'):
            with self.db_lock:
                print("🗑️  Dropping tables and indexes for zero-index optimization...")
                
                # Drop all tables (this removes all data AND all indexes)
                self.db_conn.execute('DROP TABLE IF EXISTS slow_queries')
                self.db_conn.execute('DROP TABLE IF EXISTS connections')
                self.db_conn.execute('DROP TABLE IF EXISTS authentications')
                
                # Recreate tables with clean schema (no indexes)
                self.db_conn.execute('''
                    CREATE TABLE slow_queries (
                        id INTEGER PRIMARY KEY,
                        timestamp TEXT,
                        ts_epoch INTEGER DEFAULT NULL,
                        database TEXT,
                        collection TEXT,
                        duration INTEGER,
                        docs_examined INTEGER,
                        docs_returned INTEGER,
                        keys_examined INTEGER,
                        query_hash TEXT,
                        plan_summary TEXT,
                        file_path TEXT,
                        line_number INTEGER,
                        namespace TEXT,
                        query_text TEXT,
                        connection_id TEXT,
                        username TEXT,
                        cpu_nanos INTEGER DEFAULT NULL,
                        bytes_read INTEGER DEFAULT NULL,
                        bytes_written INTEGER DEFAULT NULL,
                        time_reading_micros INTEGER DEFAULT NULL,
                        time_writing_micros INTEGER DEFAULT NULL
                    )
                ''')
                
                self.db_conn.execute('''
                    CREATE TABLE connections (
                        id INTEGER PRIMARY KEY,
                        conn_id TEXT,
                        timestamp INTEGER NOT NULL,
                        timestamp_str TEXT,
                        action TEXT,
                        details TEXT
                    )
                ''')
                
                self.db_conn.execute('''
                    CREATE TABLE authentications (
                        id INTEGER PRIMARY KEY,
                        timestamp INTEGER NOT NULL,
                        timestamp_str TEXT,
                        user TEXT,
                        database TEXT,
                        result TEXT,
                        connection_id TEXT,
                        remote TEXT,
                        mechanism TEXT
                    )
                ''')
                
                self.db_conn.commit()
                print("✅ Database cleared with zero indexes - ready for optimal bulk loading")
    
    def parse_log_file_optimized(self, filepath, skip_bulk_session=False):
        """Main entry point - chooses best parsing strategy based on file size
        
        Args:
            filepath: Path to log file to parse
            skip_bulk_session: If True, skips bulk session management (for multi-file uploads)
        """
        file_size = os.path.getsize(filepath)
        
        # PHASE 4: Gate chatty logging - only show file size for verbose mode
        if self.config.get('verbose_logging', False):
            print(f"📊 File size: {file_size / (1024*1024*1024):.1f}GB")
        
        try:
            if file_size > 1024*1024*1024:  # > 1GB: Use all optimizations
                result = self.parse_large_file_phase3(filepath, skip_bulk_session)
            elif file_size > 100*1024*1024:  # > 100MB: Use Phase 2
                result = self.parse_medium_file_phase2(filepath, skip_bulk_session)
            else:  # < 100MB: Use Phase 1 only
                result = self.parse_small_file_phase1(filepath, skip_bulk_session)
        finally:
            # Always flush remaining batches when done
            if hasattr(self, 'flush_batches'):
                self.flush_batches(final_flush=True)
            
            # PHASE 4: Gate chatty logging - only show FTS message for verbose mode
            if self.config.get('verbose_logging', False):
                print("⚡ Skipping FTS index creation for faster UI response")
                
        return result
    
    def parse_small_file_phase1(self, filepath, skip_bulk_session=False):
        """Phase 1: Optimized parsing for small files"""
        # PHASE 4: Gate chatty logging - only show for verbose mode
        if self.config.get('verbose_logging', False):
            print("🚀 Using Phase 1 optimization (JSON + Memory)")
        
        # Calculate file size for bulk load optimization
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        
        # PHASE 3: START BULK LOAD OPTIMIZATION - Skip if part of multi-file session
        if not skip_bulk_session:
            self.start_bulk_load(file_size_mb)
        
        start_time = time.time()
        filename = os.path.basename(filepath)
        
        try:
            # Phase 1: Pre-allocate lists with estimated size
            estimated_lines = self.estimate_file_lines(filepath)
            raw_lines = [] if self.config.get('store_raw_lines', True) else None
            
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()
                    if not line:
                        self.parsing_summary['skipped_lines'] += 1
                        continue
                    
                    self.parsing_summary['total_lines'] += 1
                    
                    # Phase 1: Store raw line efficiently
                    if raw_lines is not None:
                        raw_lines.append(line)
                    
                    # Phase 1: Optimized JSON parsing
                    if line.startswith('{') and line.endswith('}'):
                        self.parsing_summary['json_lines'] += 1
                        self.parse_json_line_fast(line, filepath, line_num)
                    else:
                        self.parsing_summary['text_lines'] += 1
                        self.parse_text_line_fast(line, filepath, line_num)
                    
                    self.parsing_summary['parsed_lines'] += 1
            
            if raw_lines is not None:
                self.raw_log_data[filepath] = raw_lines
                
            elapsed = time.time() - start_time
            print(f"✅ Phase 1 completed in {elapsed:.2f}s")
            return {'parsing_time': elapsed, 'method': 'phase1'}
        
        finally:
            # FINISH BULK LOAD OPTIMIZATION - Always restore database state
            self.finish_bulk_load()
    
    def parse_medium_file_phase2(self, filepath, skip_bulk_session=False):
        """Phase 2: Chunked processing with streaming analysis
        
        Args:
            filepath: Path to log file to parse
            skip_bulk_session: If True, skips bulk session management (for multi-file uploads)
        """
        # PHASE 4: Gate chatty logging - only show for verbose mode
        if self.config.get('verbose_logging', False):
            print("🚀 Using Phase 2 optimization (Chunked + Streaming)")
        
        # Calculate file size for bulk load optimization
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        
        # PHASE 3: START BULK LOAD OPTIMIZATION - Skip if part of multi-file session
        if not skip_bulk_session:
            self.start_bulk_load(file_size_mb)
        
        start_time = time.time()
        chunk_size = self.config['chunk_size']
        
        try:
            # Phase 2: Streaming analysis structures
            streaming_stats = {
                'collection_counts': Counter(),
                'hourly_patterns': defaultdict(list),
                'duration_histogram': [0] * 10  # 0-100ms, 100-200ms, etc.
            }
            
            processed_lines = 0
            chunk = []
            
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()
                    if not line:
                        continue
                        
                    chunk.append((line_num, line))
                    
                    # Process chunk when full
                    if len(chunk) >= chunk_size:
                        self.process_chunk_streaming(chunk, filepath, streaming_stats)
                        processed_lines += len(chunk)
                        
                        # Phase 2: Progress reporting
                        if self.config.get('progress_callback'):
                            progress = (processed_lines / self.estimate_file_lines(filepath)) * 100
                            self.config['progress_callback'](progress, streaming_stats)
                        
                        chunk = []
                
                # Process final chunk
                if chunk:
                    self.process_chunk_streaming(chunk, filepath, streaming_stats)
            
            elapsed = time.time() - start_time
            # PHASE 4: Gate chatty logging - only show for verbose mode
            if self.config.get('verbose_logging', False):
                print(f"✅ Phase 2 completed in {elapsed:.2f}s")
                print(f"📊 Streaming stats: {len(streaming_stats['collection_counts'])} collections")
            return {'parsing_time': elapsed, 'method': 'phase2', 'streaming_stats': streaming_stats}
        
        finally:
            # PHASE 3: FINISH BULK LOAD OPTIMIZATION - Only if we started a session
            if not skip_bulk_session:
                self.finish_bulk_load()
    
    def parse_large_file_phase3(self, filepath, skip_bulk_session=False):
        """Phase 3: Multi-threading + Database backend + Bulk Load Optimizations
        
        Args:
            filepath: Path to log file to parse
            skip_bulk_session: If True, skips bulk session management (for multi-file uploads)
        """
        # PHASE 4: Gate chatty logging - only show for verbose mode
        if self.config.get('verbose_logging', False):
            print("🚀 Using Phase 3 optimization (Multi-threading + Database)")
        
        # Calculate file size for bulk load optimization
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        
        # Phase 3: Check if file already processed (incremental)
        file_hash = self.get_file_hash(filepath)
        if file_hash in self.incremental_stats['processed_files']:
            print("✅ File already processed (incremental analysis)")
            return {'parsing_time': 0, 'method': 'phase3_cached'}
        
        start_time = time.time()
        
        # PHASE 3: START BULK LOAD OPTIMIZATION - Skip if part of multi-file session
        if not skip_bulk_session:
            self.start_bulk_load(file_size_mb)
        
        try:
            # Phase 3: Split file into chunks for multi-threading
            chunks = self.split_file_into_chunks(filepath, self.config['max_workers'])
            
            # Phase 3: Process chunks in parallel
            with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
                futures = []
                for chunk_info in chunks:
                    future = executor.submit(self.process_chunk_parallel, chunk_info)
                    futures.append(future)
                
                # Collect results
                for future in as_completed(futures):
                    chunk_result = future.result()
                    self.merge_chunk_results(chunk_result)
            
            # Final flush of any remaining batches with commit
            self.flush_batches(final_flush=True)
            
        finally:
            # PHASE 3: FINISH BULK LOAD OPTIMIZATION - Only if we started a session
            if not skip_bulk_session:
                self.finish_bulk_load()
        
        # Phase 3: Mark file as processed
        self.incremental_stats['processed_files'].add(file_hash)
        
        elapsed = time.time() - start_time
        print(f"✅ Phase 3 completed in {elapsed:.2f}s with {self.config['max_workers']} threads")
        return {'parsing_time': elapsed, 'method': 'phase3'}
    
    def parse_json_line_fast(self, line, filepath, line_num):
        """Phase 1: Optimized JSON parsing with minimal allocations"""
        try:
            # Phase 1: Fast JSON parsing - only parse what we need
            if '"msg":"Slow query"' in line or '"Slow query"' in line:
                log_entry = json.loads(line)
                self.extract_slow_query_fast(log_entry, filepath, line_num)
                self.parsing_summary['slow_query_events'] += 1
                
            elif '"connection accepted"' in line or '"Connection accepted"' in line:
                log_entry = json.loads(line)  
                self.extract_connection_fast(log_entry, 'accepted')
                self.parsing_summary['connection_events'] += 1
                
            elif '"connection ended"' in line or '"Connection ended"' in line:
                log_entry = json.loads(line)
                self.extract_connection_fast(log_entry, 'ended') 
                self.parsing_summary['connection_events'] += 1
                
            elif '"c":"ACCESS"' in line:
                log_entry = json.loads(line)
                component = log_entry.get('c', '')
                msg = log_entry.get('msg', '') or ''
                if component == 'ACCESS' and (msg == "Successfully authenticated" or msg == "Authentication succeeded" or msg == "Authentication failed"):
                    self.extract_auth_fast(log_entry)
                    self.parsing_summary['auth_events'] += 1
                
        except json.JSONDecodeError:
            self.parsing_summary['error_lines'] += 1
    
    def extract_slow_query_fast(self, log_entry, filepath, line_num):
        """Phase 1: Fast slow query extraction with minimal object creation"""
        import json
        attr = log_entry.get('attr', {})
        
        # Phase 1: Extract only essential fields
        duration = attr.get('durationMillis', 0)
        ns = attr.get('ns', '')
        
        # Fast namespace parsing
        if '.' in ns:
            database, collection = ns.split('.', 1)
        else:
            database = 'unknown'
            collection = 'unknown'
        
        # Extract command info for query text (Flask app compatibility)
        command_info = attr.get('command', {})
        if not command_info and 'command' in attr:
            command_info = attr['command']
        elif not command_info:
            # Create a minimal command representation
            command_info = {
                'find': collection if database != 'unknown' else 'unknown',
                'filter': {}
            }
        
        # Extract storage metrics (handle different log formats)
        storage = attr.get('storage', {}).get('data', {})
        
        # Phase 1: Create minimal query object with OS resource metrics
        query = {
            'timestamp': self.extract_timestamp_fast(log_entry),
            'connection_id': self.extract_connection_id_from_ctx(log_entry.get('ctx', '')),
            'duration': duration,
            'database': database, 
            'collection': collection,
            'query': json.dumps(command_info, indent=None),  # Flask app compatibility
            'docsExamined': attr.get('docsExamined', 0),
            'keysExamined': attr.get('keysExamined', 0),
            'nReturned': attr.get('nReturned') or attr.get('nreturned') or attr.get('numReturned') or 0,
            'plan_summary': attr.get('planSummary', ''),
            'query_hash': attr.get('queryHash', None),
            'planCacheKey': attr.get('planCacheKey', ''),
            'username': None,  # Will be filled by correlation
            'file_path': filepath,
            'line_number': line_num,
            'namespace': f"{database}.{collection}",
            # OS Resource Metrics (check both storage.data and attr for different log formats)
            'bytes_read': storage.get('bytesRead', 0) or attr.get('bytesRead', 0),
            'bytes_written': storage.get('bytesWritten', 0) or attr.get('bytesWritten', 0),
            'time_reading_micros': storage.get('timeReadingMicros', 0) or attr.get('timeReadingMicros', 0),
            'time_writing_micros': storage.get('timeWritingMicros', 0) or attr.get('timeWritingMicros', 0),
            'cpu_nanos': attr.get('cpuNanos', 0)
        }
        
        # Phase 3: Store in database if configured
        if self.config.get('use_database', False):
            self.store_query_in_db(query)
        else:
            self.slow_queries.append(query)
    
    def extract_connection_fast(self, log_entry, conn_type):
        """Phase 1: Fast connection extraction"""
        attr = log_entry.get('attr', {})
        remote = attr.get('remote', '')
        
        if ':' in remote:
            ip, port = remote.rsplit(':', 1)
            conn = {
                'timestamp': self.extract_timestamp_fast(log_entry),
                'ip': ip,
                'port': port,
                'connection_id': self.extract_connection_id_from_ctx(log_entry.get('ctx', '')),
                'type': f'connection_{conn_type}'
            }
            # Store in database if configured (with batch processing)
            if self.config.get('use_database', False):
                # Map to database schema: (conn_id, timestamp, timestamp_str, action, details)
                if conn.get('timestamp') and hasattr(conn['timestamp'], 'timestamp'):
                    ts_epoch = int(conn['timestamp'].timestamp())
                else:
                    print(f"⚠️  Missing timestamp in connection event, using epoch 0 (1970)")
                    ts_epoch = 0
                ts_str = conn['timestamp'].isoformat() if conn.get('timestamp') and hasattr(conn['timestamp'], 'isoformat') else None

                # Create consistent JSON details format
                details_dict = {
                    'ip': conn['ip'],
                    'port': conn['port']
                }

                conn_data = (
                    conn['connection_id'],
                    ts_epoch,      # epoch integer for timestamp column
                    ts_str,        # ISO string for timestamp_str column
                    conn['type'],
                    json.dumps(details_dict)  # Consistent JSON format like phase-3
                )
                self.add_connection_to_batch(conn_data)
            else:
                self.connections.append(conn)
    
    def extract_timestamp_fast(self, log_entry):
        """Phase 1: Fast timestamp extraction preserving MongoDB timezone"""
        t = log_entry.get('t', {})
        if isinstance(t, dict) and '$date' in t:
            date_str = t['$date']
            # Preserve original MongoDB timezone information
            try:
                # Handle MongoDB's $date format - preserve timezone as-is
                return datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except Exception as e:
                # Fallback to unix timestamp parsing if ISO parsing fails
                unix_timestamp = self.parse_timestamp_to_unix(date_str)
                if unix_timestamp:
                    return datetime.datetime.fromtimestamp(unix_timestamp, tz=datetime.timezone.utc)
                print(f"⚠️  Timestamp parsing failed for '{date_str}', using current time. Error: {e}")
                return datetime.datetime.now(tz=datetime.timezone.utc)
        print(f"⚠️  No timestamp found in log entry, using current time")
        return datetime.datetime.now(tz=datetime.timezone.utc)
    
    def extract_connection_id_fast(self, log_entry):
        """Phase 1: Fast connection ID extraction"""
        ctx = log_entry.get('ctx', '')
        if ctx.startswith('conn'):
            return ctx.replace('conn', '')
        return 'unknown'
    
    def extract_connection_id_from_ctx(self, ctx):
        """Extract connection ID from ctx field - return the whole value as-is"""
        if not ctx:
            return 'unknown'
        
        # Return the whole ctx value without any modification
        return ctx
    
    def extract_ip_from_remote(self, remote):
        """Extract IP address only from remote field (remove port)"""
        if not remote:
            return ''
        
        # Extract IP from IP:port format
        if ':' in remote:
            return remote.rsplit(':', 1)[0]
        else:
            return remote
    
    def determine_auth_result(self, msg):
        """Determine authentication result from msg field - only check specific messages"""
        if not msg:
            return 'unknown'
            
        # Only check for specific messages
        if msg == "Successfully authenticated" or msg == "Authentication succeeded":
            return 'auth_success'
        elif msg == "Authentication failed":
            return 'auth_failed'
        else:
            # Return None to indicate this should not be processed as auth event
            return None
    
    def process_chunk_streaming(self, chunk, filepath, streaming_stats):
        """Phase 2: Process chunk with streaming analysis"""
        for line_num, line in chunk:
            if not line.startswith('{'):
                continue
                
            try:
                # Phase 2: Process all event types, not just slow queries
                if '"Slow query"' in line or '"msg":"Slow query"' in line:
                    # Extract collection without full JSON parse
                    ns_match = NAMESPACE_PATTERN.search(line)
                    duration_match = DURATION_PATTERN.search(line)
                    
                    if ns_match and duration_match:
                        ns = ns_match.group(1)
                        duration = int(duration_match.group(1))
                        
                        collection = ns.split('.')[-1] if '.' in ns else 'unknown'
                        streaming_stats['collection_counts'][collection] += 1
                        
                        # Duration histogram
                        bucket = min(duration // 100, 9)  # 0-100ms, 100-200ms, etc.
                        streaming_stats['duration_histogram'][bucket] += 1
                        
                        # Full parse for storage
                        log_entry = json.loads(line)
                        self.extract_slow_query_fast(log_entry, filepath, line_num)
                        self.parsing_summary['slow_query_events'] += 1
                
                elif '"connection accepted"' in line or '"Connection accepted"' in line:
                    log_entry = json.loads(line)
                    self.extract_connection_fast(log_entry, 'accepted')
                    self.parsing_summary['connection_events'] += 1
                    
                elif '"connection ended"' in line or '"Connection ended"' in line:
                    log_entry = json.loads(line)
                    self.extract_connection_fast(log_entry, 'ended')
                    self.parsing_summary['connection_events'] += 1
                    
                elif '"c":"ACCESS"' in line:
                    log_entry = json.loads(line)
                    component = log_entry.get('c', '')
                    msg = log_entry.get('msg', '') or ''
                    if component == 'ACCESS' and (msg == "Successfully authenticated" or msg == "Authentication succeeded" or msg == "Authentication failed"):
                        self.extract_auth_fast(log_entry)
                        self.parsing_summary['auth_events'] += 1
                    
            except (json.JSONDecodeError, ValueError):
                self.parsing_summary['error_lines'] += 1
                continue
    
    def split_file_into_chunks(self, filepath, num_chunks):
        """Phase 3: Calculate file offset chunks for streaming parallel processing (no memory loading)"""
        file_size = os.path.getsize(filepath)
        chunk_size = file_size // num_chunks
        chunks = []
        
        print(f"🔄 Streaming chunk calculation for {file_size / (1024*1024):.1f}MB file")
        
        # Calculate byte offset ranges instead of loading lines into memory
        for i in range(num_chunks):
            start_offset = i * chunk_size
            
            if i == num_chunks - 1:
                # Last chunk gets remainder
                end_offset = file_size
            else:
                # Find next line boundary to avoid splitting lines
                end_offset = (i + 1) * chunk_size
                
                # Adjust end_offset to next newline to avoid splitting lines
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
                    file.seek(end_offset)
                    # Read ahead to find next line boundary
                    remainder = file.read(1024)  # Read small buffer
                    if remainder:
                        newline_pos = remainder.find('\n')
                        if newline_pos != -1:
                            end_offset += newline_pos + 1
                        else:
                            # If no newline found in buffer, move to start of next chunk
                            end_offset = (i + 1) * chunk_size
            
            # Calculate actual starting line number for this chunk
            start_line_num = self._calculate_line_number_at_offset(filepath, start_offset)

            chunk_info = {
                'chunk_id': i,
                'start_offset': start_offset,
                'end_offset': end_offset,
                'filepath': filepath,
                'estimated_size': end_offset - start_offset,
                'start_line_num': start_line_num
            }
            chunks.append(chunk_info)
            
        print(f"✅ Created {len(chunks)} streaming chunks (avg {chunk_size/(1024*1024):.1f}MB each)")
        return chunks

    def _calculate_line_number_at_offset(self, filepath, target_offset):
        """Calculate the actual line number at the given byte offset"""
        if target_offset == 0:
            return 1

        line_num = 1
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
            current_pos = 0
            while current_pos < target_offset:
                char = file.read(1)
                if not char:
                    break
                if char == '\n':
                    line_num += 1
                current_pos += 1
        return line_num

    def _validate_tuple_schema(self, tuple_data, table_name):
        """Validate that tuple matches expected schema"""
        expected_columns = {
            'slow_queries': 21,
            'connections': 7,  # Adjust based on actual schema
            'authentications': 6  # Adjust based on actual schema
        }

        expected_count = expected_columns.get(table_name)
        if expected_count and len(tuple_data) != expected_count:
            print(f"❌ Schema validation failed for {table_name}: expected {expected_count} columns, got {len(tuple_data)}")
            print(f"   Tuple: {tuple_data}")
            raise ValueError(f"Tuple schema mismatch for {table_name}: expected {expected_count} columns, got {len(tuple_data)}")

    def process_chunk_parallel(self, chunk_info):
        """Phase 3: Process chunk by streaming from file offsets (no memory loading)"""
        chunk_results = {
            'slow_queries': [],
            'connections': [],
            'authentications': [],
            'chunk_id': chunk_info['chunk_id'],
            'processed_lines': 0,
            'stats': {
                'slow_query_events': 0,
                'connection_events': 0, 
                'auth_events': 0,
                'error_lines': 0
            }
        }
        
        # Stream lines from file offset range instead of loading into memory
        filepath = chunk_info['filepath']
        start_offset = chunk_info['start_offset']
        end_offset = chunk_info['end_offset']
        start_line_num = chunk_info.get('start_line_num', 1)
        
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                # Seek to start position
                file.seek(start_offset)
                current_pos = start_offset
                line_num = 0
                
                while current_pos < end_offset:
                    line = file.readline()
                    if not line:  # EOF
                        break
                        
                    line_num += 1
                    current_pos = file.tell()
                    
                    # Process line (same logic as before)
                    if not line.strip() or not line.strip().startswith('{'):
                        continue
                        
                    line = line.strip()
                    try:
                        if '"Slow query"' in line or '"msg":"Slow query"' in line:
                            log_entry = json.loads(line)
                            actual_line_num = start_line_num + line_num - 1
                            query = self.extract_slow_query_to_dict(log_entry, filepath, actual_line_num)
                            chunk_results['slow_queries'].append(query)
                            chunk_results['stats']['slow_query_events'] += 1
                            
                        elif '"connection accepted"' in line or '"Connection accepted"' in line:
                            log_entry = json.loads(line)
                            conn = self.extract_connection_to_dict(log_entry, 'accepted')
                            if conn:
                                chunk_results['connections'].append(conn)
                                chunk_results['stats']['connection_events'] += 1
                                
                        elif '"connection ended"' in line or '"Connection ended"' in line:
                            log_entry = json.loads(line)
                            conn = self.extract_connection_to_dict(log_entry, 'ended')
                            if conn:
                                chunk_results['connections'].append(conn)
                                chunk_results['stats']['connection_events'] += 1
                                
                        elif '"c":"ACCESS"' in line:
                            log_entry = json.loads(line)
                            component = log_entry.get('c', '')
                            msg = log_entry.get('msg', '') or ''
                            if component == 'ACCESS' and (msg == "Successfully authenticated" or msg == "Authentication succeeded" or msg == "Authentication failed"):
                                auth = self.extract_auth_to_dict(log_entry)
                                if auth:
                                    chunk_results['authentications'].append(auth)
                                    chunk_results['stats']['auth_events'] += 1
                            
                    except json.JSONDecodeError:
                        chunk_results['stats']['error_lines'] += 1
                        continue
                        
                chunk_results['processed_lines'] = line_num
                
        except Exception as e:
            print(f"⚠️  Error processing chunk {chunk_info['chunk_id']}: {e}")
            
        return chunk_results
    
    def extract_slow_query_to_dict(self, log_entry, filepath, line_num):
        """Phase 3: Extract slow query to dictionary (for parallel processing)"""
        attr = log_entry.get('attr', {})
        ns = attr.get('ns', '')
        
        if '.' in ns:
            database, collection = ns.split('.', 1)
        else:
            database, collection = 'unknown', 'unknown'
        
        # Extract connection ID from ctx field
        ctx = log_entry.get('ctx', '')
        connection_id = self.extract_connection_id_from_ctx(ctx)
            
        # Robust handling of nReturned/nreturned/numReturned variations
        nReturned = attr.get('nReturned') or attr.get('nreturned') or attr.get('numReturned') or 0
        
        # Extract command for query_pattern/query_text - consistent with phase-1 handling
        command = attr.get('command', {})
        if not command:
            # Create a minimal command representation consistent with phase-1
            command = {
                'find': collection if database != 'unknown' else 'unknown',
                'filter': {}
            }
        
        # Check for username in the slow query log (some setups might include it)
        username = None
        if 'user' in attr:
            username = attr.get('user', '')
        elif 'principalName' in attr:
            username = attr.get('principalName', '')
        # Otherwise username remains None and will be filled through authentication correlation
        
        # Extract OS/storage metrics when available (robust across formats)
        storage = attr.get('storage', {}).get('data', {}) if isinstance(attr.get('storage'), dict) else {}

        return {
            'timestamp': self.extract_timestamp_fast(log_entry),
            'duration': attr.get('durationMillis', 0),
            'database': database,
            'collection': collection,
            'docsExamined': attr.get('docsExamined', 0),
            'nReturned': nReturned,  # Robust casing handling
            'keysExamined': attr.get('keysExamined', 0),  # Add missing keysExamined
            'plan_summary': attr.get('planSummary', ''),
            'queryHash': attr.get('queryHash'),  # Don't default to empty string - leave as None if missing
            'command': command,  # Add missing command for query_pattern/query_text
            'connection_id': connection_id,
            'username': username,  # Add username field (if available, otherwise None)
            'file_path': filepath,
            'line_number': line_num,
            # OS resource metrics (fallback to attr-level if nested storage missing)
            'bytes_read': storage.get('bytesRead', 0) or attr.get('bytesRead', 0),
            'bytes_written': storage.get('bytesWritten', 0) or attr.get('bytesWritten', 0),
            'time_reading_micros': storage.get('timeReadingMicros', 0) or attr.get('timeReadingMicros', 0),
            'time_writing_micros': storage.get('timeWritingMicros', 0) or attr.get('timeWritingMicros', 0),
            'cpu_nanos': attr.get('cpuNanos', 0)
        }
    
    def extract_connection_to_dict(self, log_entry, conn_type):
        """Phase 3: Extract connection to dictionary"""
        attr = log_entry.get('attr', {})
        remote = attr.get('remote', '')
        
        # Extract connection ID from ctx field consistently
        ctx = log_entry.get('ctx', '')
        connection_id = self.extract_connection_id_from_ctx(ctx)
        
        if ':' in remote:
            ip, port = remote.rsplit(':', 1)
            return {
                'timestamp': self.extract_timestamp_fast(log_entry),
                'ip': ip,
                'port': port,
                'connection_id': connection_id,
                'type': f'connection_{conn_type}'
            }
        return None
    
    def extract_auth_to_dict(self, log_entry):
        """Phase 3: Extract authentication to dictionary"""
        attr = log_entry.get('attr', {})
        
        # Extract connection ID from ctx field consistently
        ctx = log_entry.get('ctx', '')
        connection_id = self.extract_connection_id_from_ctx(ctx)
        
        # Extract authentication result from msg field
        msg = log_entry.get('msg', '')
        auth_result = self.determine_auth_result(msg)
        
        # Skip if not a valid auth event
        if auth_result is None:
            print(f"⚠️  Skipping unknown auth message: '{msg}' - not a recognized auth event")
            return None
        
        # Extract username safely (handle both string and dict formats)
        username = attr.get('user', attr.get('principalName', ''))
        if isinstance(username, dict):
            username = username.get('user', username.get('name', str(username)))
        username = str(username) if username else ''
        
        # Extract database safely
        database = attr.get('db', attr.get('authenticationDatabase', ''))
        database = str(database) if database else ''
        
        return {
            'timestamp': self.extract_timestamp_fast(log_entry),
            'connection_id': connection_id,
            'username': username,
            'database': database,
            'mechanism': attr.get('mechanism', 'SCRAM-SHA-256'),
            'remote': self.extract_ip_from_remote(attr.get('remote', '') or attr.get('client', '')),
            'type': auth_result
        }
    
    def merge_chunk_results(self, chunk_result):
        """Phase 3: Merge results from parallel processing and insert into database"""
        # Insert data into database if database is enabled
        if self.config.get('use_database', False):
            # Insert slow queries into database
            for query in chunk_result['slow_queries']:
                if query:
                    # Convert query dict to database format
                    query_data = self.convert_query_to_db_format(query)
                    self.add_slow_query_to_batch(query_data)
            
            # Insert connections into database
            for conn in chunk_result['connections']:
                if conn:
                    # Convert connection dict to database format
                    conn_data = self.convert_connection_to_db_format(conn)
                    self.add_connection_to_batch(conn_data)
            
            # Insert authentications into database
            for auth in chunk_result['authentications']:
                if auth:
                    # Convert auth dict to database format
                    auth_data = self.convert_auth_to_db_format(auth)
                    self.add_auth_to_batch(auth_data)
        else:
            # PERFORMANCE OPTIMIZATION: Only maintain in-memory lists when database is disabled
            # Skip redundant memory operations during bulk database loading
            self.slow_queries.extend(chunk_result['slow_queries'])
            self.connections.extend([c for c in chunk_result['connections'] if c is not None])
            self.authentications.extend([a for a in chunk_result['authentications'] if a is not None])
        
        # Merge statistics
        chunk_stats = chunk_result['stats']
        self.parsing_summary['slow_query_events'] += chunk_stats['slow_query_events']
        self.parsing_summary['connection_events'] += chunk_stats['connection_events'] 
        self.parsing_summary['auth_events'] += chunk_stats['auth_events']
        self.parsing_summary['error_lines'] += chunk_stats['error_lines']
    
    def convert_query_to_db_format(self, query):
        """Convert query dictionary to database format tuple for main slow_queries table"""
        # Convert timestamp to epoch seconds for numeric indexing
        ts_epoch = None
        if query.get('timestamp'):
            try:
                ts_epoch = int(query['timestamp'].timestamp())
            except (AttributeError, ValueError):
                ts_epoch = None
        
        # PERFORMANCE OPTIMIZATION: Cache JSON serialization once per record
        command_json = json.dumps(query.get('command', {})) if query.get('command') else '{}'
        
        # Handle both field name variations for compatibility
        query_hash = query.get('query_hash') or query.get('queryHash', '')
        plan_summary = query.get('plan_summary') or query.get('planSummary', '')
        
        # Generate synthetic query hash if missing
        if not query_hash:
            query_hash = self._generate_synthetic_query_hash(query)
        
        # Match the main slow_queries table schema (21 columns, with OS metrics)
        tuple_data = (
            query['timestamp'].isoformat() if query.get('timestamp') else None,  # timestamp
            ts_epoch,                                                           # ts_epoch
            query.get('database', ''),                                         # database
            query.get('collection', ''),                                       # collection
            query.get('duration', 0),                                         # duration
            query.get('docsExamined', 0),                                     # docs_examined
            query.get('nReturned', 0),                                        # docs_returned
            query.get('keysExamined', 0),                                     # keys_examined
            query_hash,                                                       # query_hash
            plan_summary,                                                     # plan_summary
            query.get('file_path', ''),                                       # file_path
            query.get('line_number', 0),                                      # line_number
            f"{query.get('database', '')}.{query.get('collection', '')}",     # namespace
            command_json,                                                     # query_text
            query.get('connection_id', 'unknown'),                            # connection_id
            query.get('username', None),                                      # username
            # OS Resource Metrics
            query.get('cpu_nanos', 0),                                        # cpu_nanos
            query.get('bytes_read', 0),                                       # bytes_read
            query.get('bytes_written', 0),                                    # bytes_written
            query.get('time_reading_micros', 0),                             # time_reading_micros
            query.get('time_writing_micros', 0),                             # time_writing_micros
        )

        # Validation: Ensure tuple has exactly 21 columns
        self._validate_tuple_schema(tuple_data, 'slow_queries')
        return tuple_data
    
    def convert_connection_to_db_format(self, conn):
        """Convert connection dictionary to database format tuple"""
        # PERFORMANCE OPTIMIZATION: Direct timestamp handling without full dict copying
        # Convert timestamp to both epoch integer and ISO string
        if conn.get('timestamp') and hasattr(conn['timestamp'], 'timestamp'):
            ts_epoch = int(conn['timestamp'].timestamp())
        else:
            print(f"⚠️  Missing timestamp in connection (chunk processing), using epoch 0 (1970)")
            ts_epoch = 0
        timestamp_str = conn['timestamp'].isoformat() if conn.get('timestamp') and hasattr(conn['timestamp'], 'isoformat') else None
        
        # Build minimal JSON for details field - only convert datetime fields as needed
        details_dict = {}
        for key, value in conn.items():
            if key in ('connection_id', 'timestamp', 'action'):  # Skip fields that are stored separately
                continue
            if hasattr(value, 'isoformat'):  # datetime object
                details_dict[key] = value.isoformat()
            else:
                details_dict[key] = value
        
        return (
            conn.get('connection_id', ''),  # conn_id
            ts_epoch,                       # timestamp (INTEGER epoch)
            timestamp_str,                  # timestamp_str (TEXT ISO string)
            conn.get('type', ''),           # action (use 'type' field from extract_connection_to_dict)
            json.dumps(details_dict) if details_dict else '{}'  # details
        )
    
    def convert_auth_to_db_format(self, auth):
        """Convert authentication dictionary to database format tuple"""
        # Convert timestamp to both epoch integer and ISO string
        if auth.get('timestamp') and hasattr(auth['timestamp'], 'timestamp'):
            ts_epoch = int(auth['timestamp'].timestamp())
        else:
            print(f"⚠️  Missing timestamp in authentication event, using epoch 0 (1970)")
            ts_epoch = 0
        timestamp_str = auth['timestamp'].isoformat() if auth.get('timestamp') and hasattr(auth['timestamp'], 'isoformat') else None
        
        return (
            ts_epoch,                                # timestamp (INTEGER epoch)
            timestamp_str,                           # timestamp_str (TEXT ISO string)
            auth.get('username', ''),                # user
            auth.get('database', ''),                # database
            auth.get('type', 'unknown'),             # result (auth_success/auth_failed)
            auth.get('connection_id', 'unknown'),    # connection_id
            auth.get('remote', ''),                  # remote (IP:port)
            auth.get('mechanism', 'SCRAM-SHA-256')   # mechanism
        )
    
    def store_query_in_db(self, query):
        """Phase 3: Store query in database backend with enhanced schema - using batches"""
        # Convert timestamp to epoch seconds for numeric indexing
        ts_epoch = None
        if query.get('timestamp'):
            try:
                ts_epoch = int(query['timestamp'].timestamp())
            except (AttributeError, ValueError):
                ts_epoch = None
        
        # Generate synthetic query hash if missing
        query_hash = query.get('query_hash')
        if not query_hash:
            query_hash = self._generate_synthetic_query_hash(query)
        
        query_data = (
            query['timestamp'].isoformat() if query.get('timestamp') else None,
            ts_epoch,
            query.get('database', ''),
            query.get('collection', ''), 
            query.get('duration', 0),
            query.get('docsExamined', 0),
            query.get('nReturned', 0),
            query.get('keysExamined', 0),
            query_hash,
            query.get('plan_summary', ''),
            query.get('file_path', ''),
            query.get('line_number', 0),
            query.get('namespace', ''),
            query.get('query', ''),
            query.get('connection_id', 'unknown'),
            query.get('username', None),
            # OS Resource Metrics
            query.get('cpu_nanos', 0),
            query.get('bytes_read', 0),
            query.get('bytes_written', 0),
            query.get('time_reading_micros', 0),
            query.get('time_writing_micros', 0)
        )
        self.add_slow_query_to_batch(query_data)
    
    def get_file_hash(self, filepath):
        """Phase 3: Get file hash for incremental processing"""
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            # Hash first and last 1KB for speed
            hasher.update(f.read(1024))
            f.seek(-1024, 2)
            hasher.update(f.read(1024))
        return hasher.hexdigest()
    
    def estimate_file_lines(self, filepath):
        """Phase 1: Estimate number of lines for pre-allocation"""
        with open(filepath, 'r') as f:
            # Sample first 1000 lines to estimate
            sample = [f.readline() for _ in range(1000)]
            avg_line_length = sum(len(line) for line in sample if line) / len(sample)
            
        file_size = os.path.getsize(filepath)
        return int(file_size / avg_line_length) if avg_line_length > 0 else 1000000
    
    def parse_text_line_fast(self, line, filepath, line_num):
        """Phase 1: Fast text line parsing (legacy format)"""
        # Simplified text parsing - focus on slow queries
        if 'slow query' in line.lower():
            self.parsing_summary['slow_query_events'] += 1
            # Basic text parsing could be added here
    
    def extract_auth_fast(self, log_entry, auth_type=None):
        """Phase 1: Fast authentication extraction"""
        try:
            attr = log_entry.get('attr', {})
            msg = log_entry.get('msg', '')
            
            # Auto-determine auth type if not provided
            if auth_type is None:
                auth_type = self.determine_auth_result(msg)
            
            # Skip if not a valid auth event
            if auth_type is None:
                return
            
            # Extract username safely (handle both string and dict formats)
            username = attr.get('user', attr.get('principalName', ''))
            if isinstance(username, dict):
                username = username.get('user', username.get('name', str(username)))
            username = str(username) if username else ''
            
            # Extract database safely
            database = attr.get('db', attr.get('authenticationDatabase', ''))
            database = str(database) if database else ''
            
            auth = {
                'timestamp': self.extract_timestamp_fast(log_entry),
                'connection_id': self.extract_connection_id_from_ctx(log_entry.get('ctx', '')),
                'username': username,
                'database': database,
                'mechanism': attr.get('mechanism', 'SCRAM-SHA-256'),
                'remote': self.extract_ip_from_remote(attr.get('remote', '') or attr.get('client', '')),
                'type': auth_type
            }
            # Store in database if configured (with batch processing)
            if self.config.get('use_database', False):
                # Map to database schema: (timestamp, timestamp_str, user, database, result, connection_id)
                # Handle timestamp conversion safely to both epoch and ISO string
                ts_epoch = 0
                timestamp_str = None
                if auth['timestamp']:
                    try:
                        ts_epoch = int(auth['timestamp'].timestamp()) if hasattr(auth['timestamp'], 'timestamp') else 0
                        timestamp_str = auth['timestamp'].isoformat() if hasattr(auth['timestamp'], 'isoformat') else None
                    except (AttributeError, TypeError):
                        ts_epoch = 0
                        timestamp_str = str(auth['timestamp'])
                
                auth_data = (
                    ts_epoch,             # timestamp (INTEGER epoch)
                    timestamp_str,        # timestamp_str (TEXT ISO string)
                    auth['username'],     # user
                    auth['database'],     # database
                    auth['type'],         # result (auth_success/auth_failed)
                    auth['connection_id'], # connection_id
                    auth.get('remote', ''),  # remote (IP:port)
                    auth.get('mechanism', 'SCRAM-SHA-256')  # mechanism
                )
                self.add_auth_to_batch(auth_data)
            else:
                self.authentications.append(auth)
        except Exception as e:
            # Log the error for debugging but don't stop processing
            print(f"Warning: Failed to process authentication event: {e}")
            pass
    
    def get_analysis_results(self):
        """Get analysis results - works with all phases"""
        if self.config.get('use_database', False):
            return self.get_results_from_db()
        else:
            return {
                'slow_queries': self.slow_queries,
                'connections': self.connections,
                'authentications': self.authentications,
                'parsing_summary': self.parsing_summary
            }
    
    # Compatibility methods for Flask app integration
    def get_parsing_summary_message(self):
        """Compatibility method for Flask app"""
        messages = []
        summary = self.parsing_summary
        
        if summary['slow_query_events'] > 0:
            messages.append(f"✅ Found {summary['slow_query_events']:,} slow queries")
        
        if summary['connection_events'] > 0:
            messages.append(f"🔗 Found {summary['connection_events']:,} connection events")
            
        if summary['auth_events'] > 0:
            messages.append(f"🔐 Found {summary['auth_events']:,} authentication events")
        
        if summary['error_lines'] > 0:
            messages.append(f"⚠️  {summary['error_lines']:,} lines had parsing errors")
            
        messages.append(f"📊 Processed {summary['total_lines']:,} total lines")
        
        return messages
    
    def parse_log_file(self, filepath):
        """Compatibility method - delegates to optimized parser"""
        return self.parse_log_file_optimized(filepath)
    
    # Additional compatibility methods for Flask app integration
    def get_available_date_range(self):
        """Get the date range of available log data - handles mixed timezone timestamps"""
        if not self.slow_queries and not self.connections:
            return None, None
        
        normalized_timestamps = []
        
        # Collect and normalize timestamps from slow queries
        for query in self.slow_queries:
            if 'timestamp' in query and query['timestamp']:
                normalized_ts = self._normalize_timestamp_for_comparison(query['timestamp'])
                if normalized_ts:
                    normalized_timestamps.append(normalized_ts)
        
        # Collect and normalize timestamps from connections
        for conn in self.connections:
            if 'timestamp' in conn and conn['timestamp']:
                normalized_ts = self._normalize_timestamp_for_comparison(conn['timestamp'])
                if normalized_ts:
                    normalized_timestamps.append(normalized_ts)
        
        # Collect and normalize timestamps from authentications
        for auth in self.authentications:
            if 'timestamp' in auth and auth['timestamp']:
                normalized_ts = self._normalize_timestamp_for_comparison(auth['timestamp'])
                if normalized_ts:
                    normalized_timestamps.append(normalized_ts)
        
        if not normalized_timestamps:
            return None, None
        
        min_date = min(normalized_timestamps)
        max_date = max(normalized_timestamps)
        return min_date, max_date
    
    def _normalize_timestamp_for_comparison(self, timestamp):
        """Normalize timestamp for safe comparison - handles both datetime and string"""
        if timestamp is None:
            return None
            
        try:
            # If it's already a datetime object
            if isinstance(timestamp, datetime.datetime):
                # Convert timezone-aware to naive datetime
                if timestamp.tzinfo is not None:
                    return timestamp.replace(tzinfo=None)
                return timestamp
            
            # If it's a string, parse it
            if isinstance(timestamp, str):
                # Try parsing as ISO format
                if 'T' in timestamp:
                    timestamp = timestamp.replace('Z', '+00:00')
                    dt = datetime.datetime.fromisoformat(timestamp)
                    if dt.tzinfo is not None:
                        return dt.replace(tzinfo=None)
                    return dt
                else:
                    # Try other common formats
                    return datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            
            return None
        except (ValueError, TypeError, AttributeError):
            return None
    
    def _is_timestamp_in_range(self, timestamp, start_date, end_date):
        """Check if timestamp is within range - handles timezone normalization"""
        if timestamp is None:
            return False
            
        normalized_ts = self._normalize_timestamp_for_comparison(timestamp)
        if normalized_ts is None:
            return False
        
        if start_date and normalized_ts < start_date:
            return False
        if end_date and normalized_ts > end_date:
            return False
            
        return True
    
    def get_slow_queries_paginated(self, page=1, per_page=100, filters=None):
        """Get paginated slow queries from database using SQL LIMIT/OFFSET"""
        offset = (page - 1) * per_page
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('start_date'):
                where_conditions.append('timestamp >= ?')
                params.append(filters['start_date'])
            if filters.get('end_date'):
                where_conditions.append('timestamp <= ?')
                params.append(filters['end_date'])
            if filters.get('database'):
                where_conditions.append('database = ?')
                params.append(filters['database'])
            if filters.get('collection'):
                where_conditions.append('collection = ?')
                params.append(filters['collection'])
        
        where_clause = 'WHERE ' + ' AND '.join(where_conditions) if where_conditions else ''
        
        # Get total count - SECURE: No string concatenation
        if where_conditions:
            count_query = "SELECT COUNT(*) FROM slow_queries WHERE " + " AND ".join(where_conditions)
        else:
            count_query = "SELECT COUNT(*) FROM slow_queries"
        
        total = self.db_conn.execute(count_query, params).fetchone()[0]
        
        # Get paginated data - SECURE: No string concatenation
        if where_conditions:
            data_query = """
                SELECT * FROM slow_queries WHERE """ + " AND ".join(where_conditions) + """
                ORDER BY duration DESC 
                LIMIT ? OFFSET ?
            """
        else:
            data_query = """
                SELECT * FROM slow_queries
                ORDER BY duration DESC 
                LIMIT ? OFFSET ?
        """
        params.extend([per_page, offset])
        
        cursor = self.db_conn.execute(data_query, params)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        items = [dict(zip(columns, row)) for row in rows]
        
        return {
            'items': items,
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page,
            'has_prev': page > 1,
            'has_next': page * per_page < total,
            'prev_num': page - 1 if page > 1 else None,
            'next_num': page + 1 if page * per_page < total else None
        }
    
    
    def get_connection_stats(self, start_date=None, end_date=None, ip_filter=None, user_filter=None):
        """Get connection statistics with optional filters - Flask app compatible structure"""
        from collections import Counter, defaultdict
        
        if not self.connections:
            return {
                'total_connections': 0,
                'unique_ips': 0,
                'connections_by_ip': {},
                'slow_queries_count': 0,
                'connections_timeline': [],
                'auth_success_count': 0,
                'auth_failure_count': 0,
                'unique_users': [],
                'unique_databases': [],
                'user_activity': {},
                'database_access': [],
                'authentications': []
            }
        
        # Apply filters to data
        filtered_connections = self.connections
        filtered_authentications = self.authentications
        filtered_database_access = self.database_access
        
        if start_date or end_date or ip_filter or user_filter:
            if start_date or end_date:
                # Normalize start/end dates for comparison
                normalized_start = self._normalize_timestamp_for_comparison(start_date) if start_date else None
                normalized_end = self._normalize_timestamp_for_comparison(end_date) if end_date else None
                
                filtered_connections = [
                    conn for conn in self.connections
                    if self._is_timestamp_in_range(conn.get('timestamp'), normalized_start, normalized_end)
                ]
                filtered_authentications = [
                    auth for auth in self.authentications
                    if self._is_timestamp_in_range(auth.get('timestamp'), normalized_start, normalized_end)
                ]
                filtered_database_access = [
                    access for access in self.database_access
                    if self._is_timestamp_in_range(access.get('timestamp'), normalized_start, normalized_end)
                ]
            
            if ip_filter:
                filtered_connections = [
                    conn for conn in filtered_connections
                    if ip_filter.lower() in conn.get('ip', '').lower()
                ]
                
            if user_filter:
                filtered_authentications = [
                    auth for auth in filtered_authentications
                    if auth.get('username') and user_filter.lower() in auth['username'].lower()
                ]
                filtered_database_access = [
                    access for access in filtered_database_access
                    if access.get('username') and user_filter.lower() in access['username'].lower()
                ]
        
        # Ensure user correlation is done
        self.correlate_users_with_access()
        
        # Count connections by IP  
        ip_counts = Counter([conn['ip'] for conn in filtered_connections if conn['type'] == 'connection_accepted'])
        
        # Get authentication statistics
        auth_success = [auth for auth in filtered_authentications if auth.get('type') == 'auth_success']
        auth_failures = [auth for auth in filtered_authentications if auth.get('type') == 'auth_failure']
        
        # Get unique users and databases
        unique_users = set([auth['username'] for auth in filtered_authentications if auth.get('username')])
        unique_databases = set([db.get('database') for db in filtered_database_access if db.get('database') and db['database'] != 'unknown'])
        
        # Combine authentication and database access data
        user_db_activity = defaultdict(lambda: {
            'databases': set(), 
            'connections': set(), 
            'last_seen': None,
            'auth_success': False,
            'auth_failures': 0,
            'auth_mechanism': None
        })
        
        for auth in auth_success:
            if auth.get('username') and auth.get('database'):
                user_db_activity[auth['username']]['databases'].add(auth['database'])
                user_db_activity[auth['username']]['connections'].add(auth.get('connection_id'))
                user_db_activity[auth['username']]['auth_success'] = True
                user_db_activity[auth['username']]['auth_mechanism'] = auth.get('mechanism', 'Unknown')
                if not user_db_activity[auth['username']]['last_seen'] or auth.get('timestamp', datetime.datetime.min) > user_db_activity[auth['username']]['last_seen']:
                    user_db_activity[auth['username']]['last_seen'] = auth.get('timestamp')
        
        # Count authentication failures per user
        for auth in auth_failures:
            if auth.get('username'):
                user_db_activity[auth['username']]['auth_failures'] += 1
                if not user_db_activity[auth['username']]['auth_mechanism']:
                    user_db_activity[auth['username']]['auth_mechanism'] = auth.get('mechanism', 'Unknown')
        
        for db_access in filtered_database_access:
            # Try to find username for this connection
            conn_auths = [auth for auth in auth_success if auth.get('connection_id') == db_access.get('connection_id')]
            if conn_auths:
                username = conn_auths[0]['username']
                user_db_activity[username]['databases'].add(db_access.get('database', 'unknown'))
                user_db_activity[username]['connections'].add(db_access.get('connection_id'))
                if not user_db_activity[username]['last_seen'] or db_access.get('timestamp', datetime.datetime.min) > user_db_activity[username]['last_seen']:
                    user_db_activity[username]['last_seen'] = db_access.get('timestamp')
        
        # Convert sets to lists for JSON serialization
        user_activity = {}
        for username, activity in user_db_activity.items():
            user_activity[username] = {
                'databases': list(activity['databases']),
                'connection_count': len(activity['connections']),
                'last_seen': activity['last_seen'],
                'auth_success': activity['auth_success'],
                'auth_failures': activity['auth_failures'],
                'auth_mechanism': activity['auth_mechanism']
            }
        
        # Build stats object with expected structure for template
        stats = {
            'total_connections': len([c for c in filtered_connections if c.get('type') == 'connection_accepted']),
            'unique_ips': len(ip_counts),
            'connections_by_ip': dict(ip_counts),
            'slow_queries_count': len(self.slow_queries),
            'connections_timeline': sorted(filtered_connections, key=lambda x: x.get('timestamp', datetime.datetime.min)),
            'auth_success_count': len(auth_success),
            'auth_failure_count': len(auth_failures),
            'unique_users': list(unique_users),
            'unique_databases': list(unique_databases),
            'user_activity': user_activity,
            'database_access': filtered_database_access,
            'authentications': filtered_authentications
        }
        
        return stats
    
    def get_dashboard_summary_stats(self, start_date=None, end_date=None, ip_filter=None, user_filter=None):
        """Get dashboard summary statistics focused on performance and usage intelligence"""
        if not self.config.get('use_database', False) or not self.db_conn:
            # Fallback to connection stats for non-database mode
            return self.get_connection_stats(start_date, end_date, ip_filter, user_filter)
        
        # Build date filters for SQL queries
        date_conditions = []
        date_params = []
        
        if start_date:
            try:
                start_epoch = int(start_date.timestamp()) if hasattr(start_date, 'timestamp') else int(datetime.fromisoformat(str(start_date)).timestamp())
                date_conditions.append('ts_epoch >= ?')
                date_params.append(start_epoch)
            except:
                pass
                
        if end_date:
            try:
                end_epoch = int(end_date.timestamp()) if hasattr(end_date, 'timestamp') else int(datetime.fromisoformat(str(end_date)).timestamp())
                date_conditions.append('ts_epoch <= ?')
                date_params.append(end_epoch)
            except:
                pass
        
        date_where = ' AND ' + ' AND '.join(date_conditions) if date_conditions else ''
        
        cursor = self.db_conn.cursor()
        
        # 1. Basic slow query metrics
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total_queries,
                COUNT(DISTINCT database||'.'||collection) as unique_namespaces,
                COUNT(DISTINCT database) as unique_databases,
                COUNT(DISTINCT query_hash) as unique_patterns,
                COALESCE(AVG(duration), 0) as avg_duration,
                COUNT(CASE WHEN plan_summary = 'COLLSCAN' THEN 1 END) as collscan_count
            FROM slow_queries 
            WHERE database NOT IN ('admin', 'local', 'config', '$external', 'unknown')
            {date_where}
        ''', date_params)
        
        basic_stats = cursor.fetchone()
        
        # 2. Top IP addresses by query count (from authentications + slow queries correlation)
        cursor.execute(f'''
            SELECT 
                a.remote as ip,
                COUNT(DISTINCT a.connection_id) as connection_count,
                COUNT(DISTINCT a.user) as unique_users,
                COUNT(*) as auth_count
            FROM authentications a
            WHERE a.remote IS NOT NULL AND a.remote != ''
            {date_where.replace('ts_epoch', 'datetime(a.timestamp)')}
            GROUP BY a.remote
            ORDER BY auth_count DESC
            LIMIT 10
        ''', date_params)
        
        top_ips = [{'ip': row[0], 'connection_count': row[1], 'unique_users': row[2], 'auth_count': row[3]} 
                   for row in cursor.fetchall()]
        
        # 3. Top namespaces by query volume and performance
        cursor.execute(f'''
            SELECT 
                database||'.'||collection as namespace,
                COUNT(*) as query_count,
                COALESCE(AVG(duration), 0) as avg_duration,
                COUNT(CASE WHEN plan_summary = 'COLLSCAN' THEN 1 END) as collscan_count,
                COUNT(CASE WHEN plan_summary LIKE '%IXSCAN%' THEN 1 END) as ixscan_count
            FROM slow_queries 
            WHERE database NOT IN ('admin', 'local', 'config', '$external', 'unknown')
            {date_where}
            GROUP BY database, collection
            ORDER BY query_count DESC
            LIMIT 10
        ''', date_params)
        
        top_namespaces = [{'namespace': row[0], 'query_count': row[1], 'avg_duration': row[2], 
                          'collscan_count': row[3], 'ixscan_count': row[4]} 
                         for row in cursor.fetchall()]
        
        # 4. Top application users (excluding system users)
        cursor.execute(f'''
            SELECT 
                a.user,
                COUNT(DISTINCT a.connection_id) as sessions,
                COUNT(CASE WHEN a.result = 'auth_success' THEN 1 END) as success_logins,
                COUNT(CASE WHEN a.result = 'auth_failed' THEN 1 END) as failed_logins,
                MAX(a.timestamp) as last_seen,
                (SELECT remote FROM authentications a2 
                 WHERE a2.user = a.user 
                 ORDER BY a2.timestamp DESC 
                 LIMIT 1) as last_login_ip,
                (SELECT timestamp_str FROM authentications a3 
                 WHERE a3.user = a.user 
                 ORDER BY a3.timestamp DESC 
                 LIMIT 1) as last_seen_str,
                (SELECT connection_id FROM authentications a4 
                 WHERE a4.user = a.user 
                 ORDER BY a4.timestamp DESC 
                 LIMIT 1) as last_login_conn_id
            FROM authentications a
            WHERE a.user IS NOT NULL 
              AND a.user NOT IN ('__system', 'admin', 'root', 'mongodb', 'system')
              AND a.user != ''
            {date_where.replace('ts_epoch', 'datetime(a.timestamp)')}
            GROUP BY a.user
            ORDER BY (success_logins + failed_logins) DESC
            LIMIT 10
        ''', date_params)
        
        # Convert timestamp to datetime preserving original MongoDB log timezone
        from datetime import datetime
        top_users = []
        for row in cursor.fetchall():
            last_seen_timestamp = row[4]
            last_seen_str = row[6]  # Original timestamp string with timezone
            last_login_conn_id = row[7]  # Connection ID for last login
            last_seen_datetime = None
            
            # Try to parse the original timestamp string first (preserves timezone)
            if last_seen_str:
                try:
                    # Handle MongoDB timestamp format with timezone
                    timestamp_str = last_seen_str
                    if isinstance(timestamp_str, str):
                        # Replace 'Z' with '+00:00' for UTC
                        timestamp_str = timestamp_str.replace('Z', '+00:00')
                        
                        # Parse with timezone info if available
                        if ('T' in timestamp_str and 
                            ('+' in timestamp_str[-6:] or '-' in timestamp_str[-6:])):
                            # This preserves the original timezone from MongoDB logs
                            last_seen_datetime = datetime.fromisoformat(timestamp_str)
                        else:
                            # Fallback to basic ISO format
                            last_seen_datetime = datetime.fromisoformat(timestamp_str)
                except:
                    # Fallback to Unix timestamp conversion if string parsing fails
                    if last_seen_timestamp:
                        try:
                            last_seen_datetime = datetime.fromtimestamp(last_seen_timestamp)
                        except:
                            last_seen_datetime = last_seen_timestamp
            elif last_seen_timestamp:
                # Fallback if no string timestamp available
                try:
                    last_seen_datetime = datetime.fromtimestamp(last_seen_timestamp)
                except:
                    last_seen_datetime = last_seen_timestamp
            
            top_users.append({
                'user': row[0], 
                'sessions': row[1],
                'success_logins': row[2],
                'failed_logins': row[3],
                'last_seen': last_seen_datetime, 
                'last_login_ip': row[5],
                'last_login_conn_id': last_login_conn_id
            })
        
        # 5. Authentication statistics
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total_auths,
                COUNT(DISTINCT remote) as unique_ips,
                COUNT(DISTINCT user) as unique_users,
                COUNT(DISTINCT mechanism) as auth_methods
            FROM authentications
            WHERE user NOT IN ('__system', 'admin', 'root')
            {date_where.replace('ts_epoch', 'datetime(timestamp)')}
        ''', date_params)
        
        auth_stats = cursor.fetchone()
        
        return {
            # Enhanced basic metrics
            'slow_queries_count': basic_stats[0],
            'avg_duration': round(basic_stats[4], 2),
            'unique_namespaces': basic_stats[1],
            'unique_databases': basic_stats[2],
            'unique_patterns': basic_stats[3],
            'performance_issues': basic_stats[5],  # COLLSCAN count
            
            # Top lists
            'top_ip': top_ips[0] if top_ips else {'ip': 'N/A', 'auth_count': 0},
            'top_ips': top_ips,
            'top_namespace': top_namespaces[0] if top_namespaces else {'namespace': 'N/A', 'query_count': 0},
            'top_namespaces': top_namespaces,
            'top_user': top_users[0] if top_users else {'user': 'N/A', 'total_activity': 0},
            'top_users': top_users,
            
            # Auth metrics
            'total_authentications': auth_stats[0] if auth_stats else 0,
            'unique_ips': auth_stats[1] if auth_stats else 0,
            'unique_users': auth_stats[2] if auth_stats else 0,
            'auth_methods': auth_stats[3] if auth_stats else 0,
            
            # For template compatibility
            'auth_success_count': 0,  # Will be calculated separately if needed
            'auth_failure_count': 0,
            'total_connections': 0,
            'connections_by_ip': {},
            'connections_timeline': [],
            'database_access': [],
            'authentications': []
        }
    
    def get_authentication_data(self, start_date=None, end_date=None, ip_filter=None, username_filter=None, mechanism_filter=None, auth_status_filter=None, use_regex=False, page=1, per_page=50):
        """Get authentication data from database with filtering and pagination"""
        if not self.config.get('use_database', False) or not self.db_conn:
            return {
                'items': [],
                'total': 0,
                'page': page,
                'per_page': per_page,
                'pages': 0,
                'has_prev': False,
                'has_next': False,
                'prev_num': None,
                'next_num': None
            }
        
        try:
            cursor = self.db_conn.cursor()
            
            # Validate regex patterns if regex is enabled
            if use_regex:
                import re
                for filter_val in [ip_filter, username_filter, mechanism_filter, auth_status_filter]:
                    if filter_val:
                        try:
                            re.compile(filter_val)
                        except re.error as e:
                            raise ValueError(f"Invalid regex pattern '{filter_val}': {str(e)}")
            
            # Build WHERE conditions
            where_conditions = []
            params = []
            
            # Date filtering using timestamp (epoch)
            if start_date:
                start_epoch = int(start_date.timestamp())
                where_conditions.append("timestamp >= ?")
                params.append(start_epoch)
            
            if end_date:
                end_epoch = int(end_date.timestamp())
                where_conditions.append("timestamp <= ?")
                params.append(end_epoch)
            
            # IP filtering (remote field already contains IP-only)
            if ip_filter:
                if use_regex:
                    where_conditions.append("remote REGEXP ?")
                    params.append(ip_filter)
                else:
                    where_conditions.append("remote LIKE ?")
                    params.append(f"%{ip_filter}%")
            
            # Username filtering
            if username_filter:
                if use_regex:
                    where_conditions.append("user REGEXP ?")
                    params.append(username_filter)
                else:
                    where_conditions.append("user LIKE ?")
                    params.append(f"%{username_filter}%")
            
            # Mechanism filtering
            if mechanism_filter:
                if use_regex:
                    where_conditions.append("mechanism REGEXP ?")
                    params.append(mechanism_filter)
                else:
                    where_conditions.append("mechanism LIKE ?")
                    params.append(f"%{mechanism_filter}%")
            
            # Auth status filtering
            if auth_status_filter:
                if use_regex:
                    where_conditions.append("result REGEXP ?")
                    params.append(auth_status_filter)
                else:
                    where_conditions.append("result = ?")
                    params.append(auth_status_filter)
            
            # Build WHERE clause
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Get total count for pagination
            count_query = f"SELECT COUNT(*) FROM authentications {where_clause}"
            cursor.execute(count_query, params)
            total = cursor.fetchone()[0]
            
            # Calculate pagination
            pages = (total + per_page - 1) // per_page  # Ceiling division
            has_prev = page > 1
            has_next = page < pages
            prev_num = page - 1 if has_prev else None
            next_num = page + 1 if has_next else None
            
            # Get paginated data
            offset = (page - 1) * per_page
            data_query = f"""
                SELECT user, database, result, connection_id, remote, mechanism, timestamp_str 
                FROM authentications 
                {where_clause}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
            """
            cursor.execute(data_query, params + [per_page, offset])
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries with proper field mapping
            items = []
            for row in rows:
                user, database, result, connection_id, remote, mechanism, timestamp_str = row
                
                # Parse timestamp string for template
                last_seen = None
                if timestamp_str:
                    try:
                        from datetime import datetime
                        last_seen = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    except:
                        last_seen = timestamp_str
                
                items.append({
                    'connection_id': connection_id or 'unknown',
                    'username': user or 'unknown',
                    'ip_address': remote or 'unknown',
                    'auth_mechanism': mechanism or 'unknown',
                    'result': result or 'unknown',
                    'last_seen': last_seen
                })
            
            return {
                'items': items,
                'total': total,
                'page': page,
                'per_page': per_page,
                'pages': pages,
                'has_prev': has_prev,
                'has_next': has_next,
                'prev_num': prev_num,
                'next_num': next_num
            }
            
        except ValueError as e:
            # Handle regex validation errors specifically
            print(f"Regex validation error: {e}")
            raise e
        except Exception as e:
            print(f"Error querying authentication data: {e}")
            return {
                'items': [],
                'total': 0,
                'page': page,
                'per_page': per_page,
                'pages': 0,
                'has_prev': False,
                'has_next': False,
                'prev_num': None,
                'next_num': None
            }
    
    def get_available_databases_from_auth(self):
        """Get list of databases from authentication table"""
        if not self.config.get('use_database', False) or not self.db_conn:
            return []
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT DISTINCT database FROM authentications WHERE database IS NOT NULL AND database != '' ORDER BY database")
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []
    
    def get_available_mechanisms_from_auth(self):
        """Get list of mechanisms from authentication table"""
        if not self.config.get('use_database', False) or not self.db_conn:
            return []
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT DISTINCT mechanism FROM authentications WHERE mechanism IS NOT NULL AND mechanism != '' ORDER BY mechanism")
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []
    
    def get_available_ips_from_auth(self):
        """Get list of IP addresses from authentication table"""
        if not self.config.get('use_database', False) or not self.db_conn:
            return []
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT DISTINCT remote FROM authentications WHERE remote IS NOT NULL ORDER BY remote")
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []
    
    def get_available_users_from_auth(self):
        """Get list of users from authentication table"""
        if not self.config.get('use_database', False) or not self.db_conn:
            return []
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT DISTINCT user FROM authentications WHERE user IS NOT NULL ORDER BY user")
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []
    
    def get_available_auth_statuses(self):
        """Get list of authentication statuses"""
        if not self.config.get('use_database', False) or not self.db_conn:
            return []
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT DISTINCT result FROM authentications WHERE result IS NOT NULL ORDER BY result")
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []
    
    def get_available_databases(self):
        """Get available databases from slow_queries table"""
        if not self.config.get('use_database', False) or not self.db_conn:
            return []
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute('''
                SELECT DISTINCT database 
                FROM slow_queries 
                WHERE database IS NOT NULL 
                  AND database NOT IN ('admin', 'local', 'config', '$external', 'unknown')
                ORDER BY database
            ''')
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting databases: {e}")
            return []
    
    def get_available_namespaces(self):
        """Get available namespaces (database.collection) from slow_queries table"""
        if not self.config.get('use_database', False) or not self.db_conn:
            return []
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute('''
                SELECT DISTINCT database || '.' || collection as namespace
                FROM slow_queries 
                WHERE database IS NOT NULL 
                  AND collection IS NOT NULL
                  AND database NOT IN ('admin', 'local', 'config', '$external', 'unknown')
                ORDER BY namespace
            ''')
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting namespaces: {e}")
            return []
    
    def correlate_users_with_access(self):
        """Correlate authentication data with database access"""
        # Create a mapping of connection_id to user info
        conn_user_map = {}
        for auth in self.authentications:
            conn_id = auth.get('connection_id')
            if conn_id:
                conn_user_map[conn_id] = {
                    'username': auth.get('username'),
                    'database': auth.get('database'),
                    'timestamp': auth.get('timestamp')
                }
        
        # Update slow queries with user information
        for query in self.slow_queries:
            conn_id = query.get('connection_id')
            if conn_id and conn_id in conn_user_map:
                user_info = conn_user_map[conn_id]
                query['username'] = user_info['username']
                # Don't overwrite if database is already set from query
                if not query.get('database') or query['database'] == 'unknown':
                    query['database'] = user_info['database']
    
    def _generate_query_pattern_from_text(self, query_text):
        """
        Generate a query pattern from the query_text field
        """
        if not query_text:
            return "Unknown Query Pattern"
        
        try:
            # Try to parse as JSON first
            if query_text.strip().startswith('{') and query_text.strip().endswith('}'):
                import json
                query_obj = json.loads(query_text)
                
                # Extract operation type and main keys
                if 'find' in query_obj:
                    operation = 'find'
                    filter_obj = query_obj.get('filter', {})
                elif 'aggregate' in query_obj:
                    operation = 'aggregate'
                    pipeline = query_obj.get('pipeline', [])
                    if pipeline and isinstance(pipeline, list) and len(pipeline) > 0:
                        first_stage = pipeline[0]
                        filter_obj = first_stage.get('$match', {}) if '$match' in first_stage else {}
                    else:
                        filter_obj = {}
                elif 'findAndModify' in query_obj:
                    operation = 'findAndModify'
                    filter_obj = query_obj.get('query', {})
                elif 'update' in query_obj:
                    operation = 'update'
                    updates = query_obj.get('updates', [])
                    filter_obj = updates[0].get('q', {}) if updates and len(updates) > 0 else {}
                elif 'delete' in query_obj:
                    operation = 'delete'
                    deletes = query_obj.get('deletes', [])
                    filter_obj = deletes[0].get('q', {}) if deletes and len(deletes) > 0 else {}
                else:
                    operation = 'unknown'
                    filter_obj = {}
                
                # Extract key field names from filter
                field_names = []
                if isinstance(filter_obj, dict):
                    field_names = list(filter_obj.keys())
                
                # Build pattern
                if field_names:
                    pattern = f"{operation}({', '.join(sorted(field_names))})"
                else:
                    pattern = f"{operation}()"
                
                return pattern
            else:
                # Handle text-based queries
                query_lower = query_text.lower()
                if 'find' in query_lower:
                    return "find(text_query)"
                elif 'update' in query_lower:
                    return "update(text_query)"
                elif 'delete' in query_lower:
                    return "delete(text_query)"
                elif 'aggregate' in query_lower:
                    return "aggregate(text_query)"
                else:
                    return "unknown(text_query)"
                    
        except (json.JSONDecodeError, KeyError, TypeError):
            # Fallback for malformed JSON or other errors
            return "malformed_query"
    
    def _extract_connection_id_from_text(self, query_text):
        """
        Extract connection ID from query_text or related data
        """
        if not query_text:
            return "N/A"
        
        try:
            # Try to parse as JSON and look for connection-related fields
            if query_text.strip().startswith('{') and query_text.strip().endswith('}'):
                import json
                query_obj = json.loads(query_text)
                
                # Look for lsid (session ID) which can serve as connection identifier
                if 'lsid' in query_obj:
                    lsid = query_obj['lsid']
                    if isinstance(lsid, dict) and 'id' in lsid:
                        # Extract a short version of the UUID
                        lsid_id = lsid['id']
                        if isinstance(lsid_id, dict) and '$uuid' in lsid_id:
                            uuid_str = lsid_id['$uuid']
                            # Return first 8 chars of UUID as connection identifier
                            return uuid_str[:8] if len(uuid_str) >= 8 else uuid_str
                
                # Look for other identifying fields
                if 'txnNumber' in query_obj:
                    return f"txn_{query_obj['txnNumber']}"
                
            return "N/A"
            
        except (json.JSONDecodeError, KeyError, TypeError):
            return "N/A"

    def _generate_synthetic_query_hash(self, query):
        """Generate a synthetic query hash for queries without queryHash"""
        try:
            # Create a normalized version of the query for hashing
            query_text = query.get('query', '')
            database = query.get('database', 'unknown')
            collection = query.get('collection', 'unknown')
            
            # Try to parse the query if it's JSON to normalize it
            try:
                if query_text.startswith('{'):
                    # Parse and extract key query components
                    query_obj = json.loads(query_text)
                    
                    # For complex queries, extract key components for uniqueness
                    normalized_parts = []
                    
                    # Add database.collection
                    normalized_parts.append(f"{database}.{collection}")
                    
                    # Extract operation type from command structure
                    if isinstance(query_obj, dict):
                        # Get the main operation (find, aggregate, update, etc.)
                        op_found = False
                        for key in ['find', 'aggregate', 'update', 'delete', 'insert', 'command']:
                            if key in query_obj:
                                normalized_parts.append(f"op:{key}")
                                op_found = True
                                break
                        
                        # If no explicit operation, treat as direct filter query
                        if not op_found:
                            normalized_parts.append("op:filter")
                            # Extract field names from the direct filter
                            filter_keys = self._extract_query_structure(query_obj)
                            if filter_keys:
                                normalized_parts.append(f"filter:{','.join(sorted(filter_keys))}")
                        
                        # Extract filter/match conditions (without values) for explicit operations
                        if 'filter' in query_obj:
                            filter_keys = self._extract_query_structure(query_obj['filter'])
                            if filter_keys:
                                normalized_parts.append(f"filter:{','.join(sorted(filter_keys))}")
                        
                        # Extract pipeline structure for aggregation
                        if 'pipeline' in query_obj and isinstance(query_obj['pipeline'], list):
                            pipeline_ops = []
                            sort_parts = []
                            match_fields = set()
                            for stage in query_obj['pipeline']:
                                if isinstance(stage, dict):
                                    for op in stage.keys():
                                        if op.startswith('$'):
                                            pipeline_ops.append(op)
                                    # Capture $sort keys/directions when present
                                    if '$sort' in stage and isinstance(stage['$sort'], dict):
                                        for sf, sd in stage['$sort'].items():
                                            try:
                                                d = int(sd)
                                            except Exception:
                                                d = 1
                                            sort_parts.append(f"{sf}:{d}")
                                    # Capture $match field names and values for uniqueness
                                    if '$match' in stage and isinstance(stage['$match'], dict):
                                        # Include both field names and a hash of the match conditions
                                        match_keys = self._extract_query_structure(stage['$match'])
                                        match_fields.update(match_keys)
                                        
                                        # Create a hash of the match condition values for uniqueness
                                        match_hash = hashlib.md5(json.dumps(stage['$match'], sort_keys=True).encode()).hexdigest()[:8]
                                        match_fields.add(f"match_values_{match_hash}")
                            if pipeline_ops:
                                normalized_parts.append(f"pipeline:{','.join(pipeline_ops)}")
                            if sort_parts:
                                normalized_parts.append(f"pipeline_sort:{','.join(sort_parts)}")
                            if match_fields:
                                normalized_parts.append(f"pipeline_match:{','.join(sorted(match_fields))}")
                        
                        # Extract updates[].q filters for update operations
                        if 'updates' in query_obj and isinstance(query_obj['updates'], list):
                            update_filters = set()
                            for update in query_obj['updates']:
                                if isinstance(update, dict) and 'q' in update:
                                    update_filter_keys = self._extract_query_structure(update['q'])
                                    if update_filter_keys:
                                        update_filters.update(update_filter_keys)
                            if update_filters:
                                normalized_parts.append(f"updates_filter:{','.join(sorted(update_filters))}")
                        
                        # Extract deletes[].q filters for delete operations
                        if 'deletes' in query_obj and isinstance(query_obj['deletes'], list):
                            delete_filters = set()
                            for delete in query_obj['deletes']:
                                if isinstance(delete, dict) and 'q' in delete:
                                    delete_filter_keys = self._extract_query_structure(delete['q'])
                                    if delete_filter_keys:
                                        delete_filters.update(delete_filter_keys)
                            if delete_filters:
                                normalized_parts.append(f"deletes_filter:{','.join(sorted(delete_filters))}")
                    
                    # Include sort keys if present
                    sort_spec = None
                    if isinstance(query_obj, dict) and 'sort' in query_obj and isinstance(query_obj['sort'], dict):
                        sort_parts = []
                        for sf, sd in query_obj['sort'].items():
                            try:
                                d = int(sd)
                            except Exception:
                                d = 1
                            sort_parts.append(f"{sf}:{d}")
                        if sort_parts:
                            normalized_parts.append(f"sort:{','.join(sort_parts)}")

                    # Create normalized query string
                    normalized_query = '|'.join(normalized_parts)
                else:
                    # For text-based queries, extract pattern
                    normalized_query = f"{database}.{collection}|{self._normalize_text_query(query_text)}"
            
            except (json.JSONDecodeError, Exception):
                # Fallback for unparseable queries
                normalized_query = f"{database}.{collection}|{self._normalize_text_query(query_text)}"
            
            # Generate hash from normalized query
            return hashlib.md5(normalized_query.encode('utf-8')).hexdigest()
            
        except Exception:
            # Ultimate fallback - hash the raw query
            fallback_key = f"{query.get('database', 'unknown')}.{query.get('collection', 'unknown')}|{query.get('query', '')[:100]}"
            return hashlib.md5(fallback_key.encode('utf-8')).hexdigest()
    
    def _extract_query_structure(self, filter_obj, max_depth=2, current_depth=0):
        """Extract field names from query filter for structure matching"""
        if current_depth >= max_depth or not isinstance(filter_obj, dict):
            return set()
        
        field_names = set()
        for key, value in filter_obj.items():
            if not key.startswith('$'):  # Field name
                field_names.add(key)
                
                # For regex patterns, include a hash of the pattern for uniqueness
                if isinstance(value, dict) and '$regex' in value:
                    pattern_info = str(value.get('$regex', ''))
                    if isinstance(value['$regex'], dict) and '$regularExpression' in value['$regex']:
                        pattern_info = str(value['$regex'].get('$regularExpression', {}).get('pattern', ''))
                    # Create a short hash of the pattern for uniqueness
                    import hashlib
                    pattern_hash = hashlib.md5(pattern_info.encode()).hexdigest()[:8]
                    field_names.add(f"{key}_regex_{pattern_hash}")
                    
            elif isinstance(value, dict):
                field_names.update(self._extract_query_structure(value, max_depth, current_depth + 1))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        field_names.update(self._extract_query_structure(item, max_depth, current_depth + 1))
        
        return field_names
    
    def _normalize_text_query(self, query_text):
        """Normalize text-based query for pattern matching"""
        # Extract operation pattern from text queries
        if 'command' in query_text.lower():
            # Extract command type
            command_match = re.search(r'command\s+(\w+)', query_text, re.IGNORECASE)
            if command_match:
                return f"command:{command_match.group(1)}"
        
        if 'slow query' in query_text.lower():
            return "slow_query"
        
        # Return first 50 chars as fallback pattern
        return re.sub(r'\s+', ' ', query_text[:50]).strip()
    
    def _normalize_datetime_for_comparison(self, dt):
        """Normalize datetime objects for safe comparison (remove timezone info)"""
        if dt is None:
            return None
        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
            # Convert to naive datetime by removing timezone info
            return dt.replace(tzinfo=None)
        return dt
    
    def _group_queries_by_pattern(self, queries):
        """Group queries by pattern for unique queries view (Flask app compatibility)"""
        from collections import defaultdict
        
        patterns = defaultdict(lambda: {
            'executions': [],
            'total_count': 0,
            'durations': [],
            'query_hash': '',
            'database': '',
            'collection': '',
            'plan_summary': '',
            'first_seen': None,
            'last_seen': None,
            'sample_query': '',
            'slowest_query_full': '',
            'slowest_execution_timestamp': None,
            'total_duration': 0,
            'avg_duration': 0,
            'max_duration': 0,
            'min_duration': 0
        })
        
        for query in queries:
            # Create pattern key (match app.py logic)
            query_hash = query.get('query_hash', 'unknown')
            pattern_key = f"{query.get('database', 'unknown')}.{query.get('collection', 'unknown')}_{query_hash}_{query.get('plan_summary', 'None')}"
            
            pattern = patterns[pattern_key]
            
            # Initialize pattern data on first occurrence
            if pattern['total_count'] == 0:
                pattern['query_hash'] = query_hash
                pattern['database'] = query.get('database', 'unknown')
                pattern['collection'] = query.get('collection', 'unknown')
                pattern['plan_summary'] = query.get('plan_summary', 'None')
                pattern['sample_query'] = query.get('query', '')[:200] + ('...' if len(query.get('query', '')) > 200 else '')
                pattern['slowest_query_full'] = query.get('query', '')
                pattern['first_seen'] = query.get('timestamp')
                pattern['slowest_execution_timestamp'] = query.get('timestamp')
                pattern['min_duration'] = query.get('duration', 0)
                pattern['max_duration'] = query.get('duration', 0)
            
            # Add execution data
            pattern['executions'].append(query)
            pattern['total_count'] += 1
            pattern['durations'].append(query.get('duration', 0))
            pattern['total_duration'] += query.get('duration', 0)
            
            # Update timestamp tracking
            current_timestamp = query.get('timestamp')
            if current_timestamp:
                if not pattern['last_seen'] or current_timestamp > pattern['last_seen']:
                    pattern['last_seen'] = current_timestamp
                if not pattern['first_seen'] or current_timestamp < pattern['first_seen']:
                    pattern['first_seen'] = current_timestamp
            
            # Track slowest query in pattern
            if query.get('duration', 0) > pattern['max_duration']:
                pattern['max_duration'] = query.get('duration', 0)
                pattern['slowest_query_full'] = query.get('query', '')
                pattern['slowest_execution_timestamp'] = query.get('timestamp')
            
            if query.get('duration', 0) < pattern['min_duration']:
                pattern['min_duration'] = query.get('duration', 0)
        
        # Convert to list format and calculate final statistics
        result = []
        for pattern_key, pattern_data in patterns.items():
            if pattern_data['total_count'] > 0:
                pattern_data['avg_duration'] = pattern_data['total_duration'] / pattern_data['total_count']
                
                # Add required template fields
                pattern_data['query'] = pattern_data['sample_query']  # Template expects 'query' field
                pattern_data['duration'] = pattern_data['max_duration']  # Template expects 'duration' field
                
                # Format timestamps for template compatibility (prevent subscript errors)
                pattern_data['first_seen_formatted'] = self._format_timestamp_for_template(pattern_data['first_seen'])
                pattern_data['last_seen_formatted'] = self._format_timestamp_for_template(pattern_data['last_seen'])
                pattern_data['slowest_execution_timestamp_formatted'] = self._format_timestamp_for_template(pattern_data['slowest_execution_timestamp'])
                
                # Also add formatted timestamp field for each execution
                for execution in pattern_data['executions']:
                    if 'timestamp' in execution:
                        execution['timestamp_formatted'] = self._format_timestamp_for_template(execution['timestamp'])
                
                result.append(pattern_data)
        
        # Sort by total duration (impact = avg duration × count)
        return sorted(result, key=lambda x: x['total_duration'], reverse=True)
    
    def get_original_log_line(self, file_path, line_number):
        """Get the original log line from raw data"""
        if not file_path or not line_number:
            return None
        
        # Try to get from raw log data
        if file_path in self.raw_log_data:
            try:
                line_index = int(line_number) - 1  # Convert to 0-based index
                if 0 <= line_index < len(self.raw_log_data[file_path]):
                    return self.raw_log_data[file_path][line_index]
            except (ValueError, IndexError):
                pass
        
        # Fallback: try to read from file if it still exists
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f, 1):
                    if i == int(line_number):
                        return line.strip()
        except (IOError, ValueError):
            pass
        
        return None
    
    def search_logs(self, keyword=None, field_name=None, field_value=None, use_regex=False,
                    start_date=None, end_date=None, limit=100, conditions=None):
        """Search through log entries with various criteria (Flask app compatibility)"""
        import json
        import re

        results = []
        total_found = 0

        normalized_start = self._normalize_datetime_for_comparison(start_date) if start_date else None
        normalized_end = self._normalize_datetime_for_comparison(end_date) if end_date else None

        effective_conditions = []
        if keyword:
            effective_conditions.append({
                'type': 'keyword',
                'value': keyword,
                'regex': use_regex,
                'case_sensitive': False,
                'negate': False,
            })
        if field_name and field_value:
            effective_conditions.append({
                'type': 'field',
                'name': field_name,
                'value': field_value,
                'regex': use_regex,
                'case_sensitive': False,
                'negate': False,
            })
        if conditions:
            effective_conditions.extend(conditions)

        if use_regex:
            try:
                if keyword:
                    re.compile(keyword)
                if field_name and field_value:
                    re.compile(str(field_value))
            except re.error:
                return {'results': [], 'total_found': 0}

        for file_path, lines in (self.raw_log_data or {}).items():
            for line_num, line in enumerate(lines, 1):
                line_stripped = line.rstrip('\n')
                parsed_json = None
                timestamp = None

                if line_stripped.startswith('{'):
                    try:
                        parsed_json = json.loads(line_stripped)
                    except Exception:
                        parsed_json = None

                if parsed_json:
                    t = parsed_json.get('t', {})
                    if isinstance(t, dict) and '$date' in t:
                        try:
                            timestamp = datetime.datetime.fromisoformat(str(t['$date']).replace('Z', '+00:00'))
                        except Exception:
                            timestamp = None
                if not timestamp:
                    timestamp = self._extract_timestamp_from_line(line_stripped)

                if normalized_start and timestamp:
                    ts_norm = self._normalize_datetime_for_comparison(timestamp)
                    if ts_norm and ts_norm < normalized_start:
                        continue
                if normalized_end and timestamp:
                    ts_norm = self._normalize_datetime_for_comparison(timestamp)
                    if ts_norm and ts_norm > normalized_end:
                        continue

                if effective_conditions:
                    if not self._line_matches_conditions(line_stripped, parsed_json, effective_conditions):
                        continue

                total_found += 1
                if len(results) < limit:
                    results.append({
                        'file_path': file_path,
                        'line_number': line_num,
                        'line_content': line_stripped,
                        'raw_line': line_stripped,
                        'timestamp': timestamp,
                        'formatted_timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S') if timestamp else None,
                        'parsed_json': parsed_json
                    })

        return {'results': results, 'total_found': total_found}

    def _get_nested_field(self, obj, field_path):
        """Get nested field from object using dot notation"""
        if not field_path:
            return None
        
        parts = field_path.split('.')
        current = obj
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        
        return current
    
    def _extract_timestamp_from_line(self, line):
        """Extract timestamp from a log line"""
        import re
        import datetime
        
        # Try JSON format first
        if line.startswith('{'):
            try:
                import json
                log_entry = json.loads(line)
                t = log_entry.get('t', {})
                if isinstance(t, dict) and '$date' in t:
                    date_str = t['$date']
                    # Use the new timestamp parsing utility
                    unix_timestamp = self.parse_timestamp_to_unix(date_str)
                    if unix_timestamp:
                        return datetime.datetime.fromtimestamp(unix_timestamp)
                    # Fallback to original parsing
                    return datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                pass
        
        # Try common timestamp patterns
        timestamp_patterns = [
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})',
        ]
        
        for pattern in timestamp_patterns:
            match = re.search(pattern, line)
            if match:
                try:
                    return datetime.datetime.fromisoformat(match.group(1))
                except:
                    pass
        
        return None
    
    def analyze_index_suggestions(self):
        """Analyze slow and inefficient queries to suggest high-accuracy indexes.

        - Considers COLLSCAN and IXSCAN with poor efficiency
        - Builds compound indexes (equality -> range -> sort direction)
        - Ranks suggestions by impact score (duration x inefficiency)
        - Deduplicates covered single-field suggestions
        - Returns per-collection structured suggestions with confidence and impact
        """
        from collections import defaultdict
        import json

        def parse_query(query_text):
            if not query_text or not isinstance(query_text, str):
                return None
            query_text = query_text.strip()
            if not (query_text.startswith('{') and query_text.endswith('}')):
                return None
            try:
                return json.loads(query_text)
            except Exception:
                return None

        def is_anchored_regex(v):
            if isinstance(v, dict) and '$regex' in v:
                pat = v.get('$regex', '')
                return isinstance(pat, str) and pat.startswith('^')
            return False

        def collect_filters_and_sort(qobj):
            eq_fields = []
            range_fields = []
            sort_items = []
            if not qobj:
                return eq_fields, range_fields, sort_items

            # find
            if 'find' in qobj:
                fobj = qobj.get('filter', {}) or {}
                sobj = qobj.get('sort', {}) or {}
            # aggregate
            elif 'aggregate' in qobj:
                fobj, sobj = {}, {}
                for stage in qobj.get('pipeline', []) or []:
                    if '$match' in stage and isinstance(stage['$match'], dict):
                        fobj = {**fobj, **stage['$match']}
                    if '$sort' in stage and isinstance(stage['$sort'], dict):
                        sobj = {**sobj, **stage['$sort']}
            else:
                fobj, sobj = {}, {}

            # Flatten simple equality and $in, skip complex ops
            for k, v in (fobj or {}).items():
                if k in ('$and', '$or', '$nor', '$expr'):
                    continue
                # equality
                if not isinstance(v, dict):
                    eq_fields.append(k)
                else:
                    ops = set(v.keys())
                    if ops <= {'$eq'}:
                        eq_fields.append(k)
                    elif '$in' in ops and isinstance(v.get('$in'), list):
                        eq_fields.append(k)
                    elif any(op in ops for op in ('$gt', '$lt', '$gte', '$lte')):
                        range_fields.append(k)
                    elif is_anchored_regex(v):
                        eq_fields.append(k)

            for k, d in (sobj or {}).items():
                try:
                    dirn = int(d)
                except Exception:
                    dirn = 1
                sort_items.append((k, 1 if dirn >= 0 else -1))

            return eq_fields, range_fields, sort_items

        def normalized_spec(eq_fields, range_fields, sort_items):
            spec = [(f, 1) for f in eq_fields]
            spec += [(f, 1) for f in range_fields]
            spec += sort_items
            return tuple(spec)

        def is_covered(shorter, longer):
            return len(shorter) <= len(longer) and shorter == longer[:len(shorter)]

        suggestions = defaultdict(lambda: {
            'collection_name': '',
            'suggestions': [],
            'collscan_queries': 0,
            'ixscan_ineff_queries': 0,
            'total_docs_examined': 0,
            'total_returned': 0,
            'total_duration': 0,
            'avg_duration': 0,
            'sample_queries': []
        })

        # Aggregate per suggestion spec
        per_spec = defaultdict(lambda: {
            'collection': '',
            'spec': tuple(),
            'occurrences': 0,
            'total_duration': 0,
            'docs_examined': 0,
            'returned': 0,
            'confidence': 'high',
            'reason': '',
            'weighted_execs': 0,
            'patterned_duration': 0,
            'source': set(),  # {'COLLSCAN','IXSCAN'}
        })

        # Use patterns to weight by total executions across similar shapes
        try:
            patterns = self.analyze_query_patterns()
        except Exception:
            patterns = {}

        for query in self.slow_queries:
            plan = (query.get('plan_summary') or '').upper()
            is_collscan = plan == 'COLLSCAN'
            is_ixscan = 'IXSCAN' in plan

            if not (is_collscan or is_ixscan):
                continue

            db_coll = f"{query.get('database', 'unknown')}.{query.get('collection', 'unknown')}"
            s = suggestions[db_coll]
            s['collection_name'] = db_coll
            if is_collscan:
                s['collscan_queries'] += 1
            if is_ixscan:
                s['ixscan_ineff_queries'] += 1

            docs = int(query.get('docsExamined', 0) or 0)
            ret = int(query.get('nReturned', 0) or 0)
            dur = int(query.get('duration', 0) or 0)
            s['total_docs_examined'] += docs
            s['total_returned'] += ret
            s['total_duration'] += dur

            if len(s['sample_queries']) < 3:
                s['sample_queries'].append({
                    'query': query.get('query', ''),
                    'duration': dur,
                    'timestamp': query.get('timestamp')
                })

            qobj = parse_query(query.get('query', ''))
            eq_fields, range_fields, sort_items = collect_filters_and_sort(qobj)
            if not eq_fields and not range_fields:
                continue  # only high-accuracy suggestions

            spec = normalized_spec(eq_fields, range_fields, sort_items)
            if not spec:
                continue

            key = (db_coll, spec)
            rec = per_spec[key]
            rec['collection'] = db_coll
            rec['spec'] = spec
            rec['occurrences'] += 1
            rec['total_duration'] += dur
            rec['docs_examined'] += docs
            rec['returned'] += ret
            rec['reason'] = 'COLLSCAN filter/sort coverage' if is_collscan else 'IXSCAN inefficiency improvement'
            rec['source'].add('COLLSCAN' if is_collscan else 'IXSCAN')

            # Patterns weighting
            try:
                qh = query.get('query_hash') or ''
                if not qh:
                    # fallback: synthetic hash similar to analyze_query_patterns
                    qh = self._generate_synthetic_query_hash(query)
                pk = f"{query.get('database','unknown')}.{query.get('collection','unknown')}_{qh}_{query.get('plan_summary','None')}"
                pat = patterns.get(pk)
                if pat:
                    rec['weighted_execs'] += int(pat.get('total_count', 0) or 0)
                    # approximate duration load
                    rec['patterned_duration'] += float(pat.get('avg_duration', 0) or 0) * int(pat.get('total_count', 0) or 0)
            except Exception:
                pass

        # Build ranked suggestions per collection
        collection_to_specs = defaultdict(list)
        for (coll, spec), rec in per_spec.items():
            # inefficiency ratio
            ineff = (rec['docs_examined'] / max(1, rec['returned'])) if rec['returned'] is not None else 1.0
            base_load = rec['patterned_duration'] if rec['patterned_duration'] else rec['total_duration']
            impact = base_load * max(1.0, ineff)
            rec['impact_score'] = int(impact)
            rec['inefficiency_ratio'] = round(ineff, 2)
            rec['avg_duration'] = int(rec['total_duration'] / max(1, rec['occurrences']))
            collection_to_specs[coll].append(rec)

        # Deduplicate covered specs and keep top-N
        results = {}
        for coll, items in collection_to_specs.items():
            # remove covered specs
            specs_sorted = sorted(items, key=lambda r: (-r['impact_score'], -len(r['spec'])))
            kept = []
            for r in specs_sorted:
                if any(is_covered(r['spec'], k['spec']) for k in kept):
                    continue
                kept.append(r)
            # finalize list with commands
            final = []
            for r in kept[:10]:
                index_fields = ', '.join([f"{f}: {d}" for f, d in r['spec']])
                final.append({
                    'type': 'compound',
                    'index': f'{{{index_fields}}}',
                    'fields': [{'field': f, 'direction': d} for f, d in r['spec']],
                    'reason': r['reason'],
                    'priority': 'high',
                    'confidence': 'high',
                    'impact_score': r['impact_score'],
                    'occurrences': r['occurrences'],
                    'avg_duration_ms': r['avg_duration'],
                    'inefficiency_ratio': r['inefficiency_ratio'],
                    'command': f"db.{coll.split('.',1)[1]}.createIndex({{{index_fields}}})"
                })
            # assemble collection entry
            s = suggestions[coll]
            if s['collscan_queries'] + s['ixscan_ineff_queries'] > 0:
                s['avg_duration'] = s['total_duration'] / max(1, (s['collscan_queries'] + s['ixscan_ineff_queries']))
            s['suggestions'] = final
            s['avg_docs_per_query'] = s['total_docs_examined'] / max(1, (s['collscan_queries'] + s['ixscan_ineff_queries']))
            results[coll] = s

        return results
    
    def get_legacy_compatible_queries(self):
        """Convert optimized query format to legacy format for compatibility with analyzer classes"""
        import json
        compatible_queries = []
        
        for query in self.slow_queries:
            # Convert field names to match what the original analyzer classes expect
            compatible_query = {
                'timestamp': query.get('timestamp'),
                'duration_ms': query.get('duration', 0),  # Convert duration to duration_ms
                'docs_examined': query.get('docsExamined', 0),  # Convert docsExamined to docs_examined  
                'docs_returned': query.get('nReturned', 0),  # Convert nReturned to docs_returned
                'keys_examined': query.get('keysExamined', 0),  # Convert keysExamined to keys_examined
                'database': query.get('database', 'unknown'),
                'collection': query.get('collection', 'unknown'),
                'namespace': f"{query.get('database', 'unknown')}.{query.get('collection', 'unknown')}",
                'plan_summary': query.get('plan_summary', ''),
                'query': query.get('query', ''),
                'command_info': self._parse_command_info(query.get('query', '')),
                'connection_id': query.get('connection_id', ''),
                'username': query.get('username'),
                'file_path': query.get('file_path', ''),
                'line_number': query.get('line_number', 0)
            }
            compatible_queries.append(compatible_query)
        
        return compatible_queries
    
    def _parse_command_info(self, query_str):
        """Parse query string to extract command info for compatibility"""
        try:
            if query_str and query_str.startswith('{'):
                return json.loads(query_str)
        except:
            pass
        return {}
    
    def analyze_query_patterns(self, queries=None):
        """Analyze query patterns for insights (Flask app compatibility)"""
        from collections import defaultdict
        import statistics
        
        patterns = defaultdict(lambda: {
            'query_hash': '',
            'plan_cache_key': '',
            'collection': '',
            'database': '',
            'query_type': '',
            'plan_summary': '',
            'executions': [],
            'total_count': 0,
            'avg_duration': 0,
            'min_duration': 0,
            'max_duration': 0,
            'median_duration': 0,
            'total_docs_examined': 0,
            'total_keys_examined': 0,
            'total_returned': 0,
            'avg_selectivity': 0,
            'avg_index_efficiency': 0,
            'sample_query': '',
            'slowest_query_full': '',
            'slowest_execution_timestamp': None,
            'first_seen': None,
            'last_seen': None,
            'complexity_score': 0,
            'optimization_potential': 'low'
        })
        
        # Determine source data
        source_queries = queries if queries is not None else self.slow_queries

        # Group queries by pattern and calculate statistics
        for query in source_queries:
            query_hash = self._generate_synthetic_query_hash(query)
            pattern_key = f"{query.get('database', 'unknown')}.{query.get('collection', 'unknown')}_{query_hash}_{query.get('plan_summary', 'None')}"
            pattern = patterns[pattern_key]
            
            # Initialize pattern data if first time
            if pattern['total_count'] == 0:
                pattern['query_hash'] = query_hash
                pattern['collection'] = query.get('collection', 'unknown')
                pattern['database'] = query.get('database', 'unknown')
                pattern['plan_summary'] = query.get('plan_summary', 'unknown')
                pattern['first_seen'] = query.get('timestamp')
                pattern['last_seen'] = query.get('timestamp')
                pattern['sample_query'] = str(query)
                pattern['slowest_query_full'] = str(query)
                pattern['slowest_execution_timestamp'] = query.get('timestamp')
            
            # Update counts and totals
            pattern['total_count'] += 1
            pattern['executions'].append(query)
            
            # Update timestamp range (first_seen = earliest, last_seen = latest)
            current_timestamp = query.get('timestamp')
            if current_timestamp:
                if not pattern['first_seen'] or (pattern['first_seen'] and current_timestamp < pattern['first_seen']):
                    pattern['first_seen'] = current_timestamp
                if not pattern['last_seen'] or (pattern['last_seen'] and current_timestamp > pattern['last_seen']):
                    pattern['last_seen'] = current_timestamp
            
            # Update duration statistics
            duration = query.get('duration', 0)
            if pattern['total_count'] == 1:
                pattern['min_duration'] = duration
                pattern['max_duration'] = duration
            else:
                pattern['min_duration'] = min(pattern['min_duration'], duration)
                pattern['max_duration'] = max(pattern['max_duration'], duration)
            
            # Update totals (handle both old and new field names)
            pattern['total_docs_examined'] += (
                query.get('docsExamined', 0) if query.get('docsExamined') is not None else
                query.get('docs_examined', 0) if query.get('docs_examined') is not None else 0
            )
            pattern['total_keys_examined'] += (
                query.get('keysExamined', 0) if query.get('keysExamined') is not None else
                query.get('keys_examined', 0) if query.get('keys_examined') is not None else 0
            )
            pattern['total_returned'] += (
                query.get('nReturned', 0) if query.get('nReturned') is not None else
                query.get('docs_returned', 0) if query.get('docs_returned') is not None else 0
            )
            
            # Track slowest query
            if duration > pattern.get('max_duration', 0):
                pattern['slowest_query_full'] = str(query)
                pattern['slowest_execution_timestamp'] = query.get('timestamp')
        
        # Calculate final statistics for each pattern
        for pattern_key, pattern in patterns.items():
            if pattern['total_count'] > 0:
                durations = [q.get('duration', 0) for q in pattern['executions']]
                pattern['avg_duration'] = sum(durations) / len(durations)
                
                if len(durations) > 1:
                    pattern['median_duration'] = statistics.median(durations)
                else:
                    pattern['median_duration'] = pattern['avg_duration']
                
                pattern['avg_docs_examined'] = pattern['total_docs_examined'] / pattern['total_count'] if pattern['total_count'] else 0
                pattern['avg_docs_returned'] = pattern['total_returned'] / pattern['total_count'] if pattern['total_count'] else 0
                pattern['avg_keys_examined'] = pattern['total_keys_examined'] / pattern['total_count'] if pattern['total_count'] else 0

                # Calculate selectivity / index efficiency
                if pattern['total_docs_examined'] > 0:
                    pattern['avg_selectivity'] = (pattern['total_returned'] / pattern['total_docs_examined']) * 100
                    pattern['avg_index_efficiency'] = (pattern['total_keys_examined'] / pattern['total_docs_examined']) * 100
                else:
                    pattern['avg_selectivity'] = 0
                    pattern['avg_index_efficiency'] = 0

                # Calculate optimization potential based on duration and selectivity
                if pattern['avg_duration'] > 1000 or pattern['avg_selectivity'] < 10:
                    pattern['optimization_potential'] = 'high'
                elif pattern['avg_duration'] > 100 or pattern['avg_selectivity'] < 50:
                    pattern['optimization_potential'] = 'medium'
                else:
                    pattern['optimization_potential'] = 'low'
                
                # Calculate complexity score
                pattern['complexity_score'] = min(100, 
                    (pattern['avg_duration'] / 10) + 
                    (100 - pattern['avg_selectivity']) + 
                    (pattern['total_count'] / 10)
                )
        
        return patterns
    
    def get_results_from_db(self):
        """Phase 3: Get results from database"""
        cursor = self.db_conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM slow_queries')
        query_count = cursor.fetchone()[0]
        
        # Get queries as dictionaries for consistency
        cursor.execute('''
            SELECT timestamp, database, collection, duration, docs_examined, docs_returned,
                   plan_summary, file_path, line_number 
            FROM slow_queries ORDER BY duration DESC LIMIT 10000
        ''')
        
        db_queries = []
        for row in cursor.fetchall():
            db_queries.append({
                'timestamp': datetime.datetime.fromisoformat(row[0]),
                'database': row[1],
                'collection': row[2], 
                'duration': row[3],
                'docsExamined': row[4],
                'nReturned': row[5],
                'plan_summary': row[6],
                'file_path': row[7],
                'line_number': row[8]
            })
        
        return {
            'slow_queries': db_queries,  # Return as list for consistency
            'slow_queries_count': query_count,
            'connections': self.connections,
            'authentications': self.authentications,
            'parsing_summary': self.parsing_summary
        }
    
    # =============================================================================
    # PHASE 1 OPTIMIZATION: SQL-Based Metadata Queries for UI Performance
    # =============================================================================
    
    def get_available_date_range_sql(self):
        """Get date range using efficient SQL instead of Python iteration"""
        if not self.use_database or not hasattr(self, 'db_conn'):
            # Fallback to original method
            return self.get_available_date_range()
        
        try:
            cursor = self.db_conn.cursor()
            
            # Get min/max from slow_queries table
            cursor.execute('''
                SELECT MIN(ts_epoch), MAX(ts_epoch) 
                FROM slow_queries 
                WHERE ts_epoch IS NOT NULL AND ts_epoch > 0
            ''')
            slow_query_range = cursor.fetchone()
            
            # Get min/max from connections table
            cursor.execute('''
                SELECT MIN(timestamp), MAX(timestamp) 
                FROM connections 
                WHERE timestamp IS NOT NULL AND timestamp > 0
            ''')
            conn_range = cursor.fetchone()

            # Get min/max from authentications table
            cursor.execute('''
                SELECT MIN(timestamp), MAX(timestamp)
                FROM authentications
                WHERE timestamp IS NOT NULL AND timestamp > 0
            ''')
            auth_range = cursor.fetchone()
            
            # Combine ranges and find overall min/max
            min_timestamps = []
            max_timestamps = []
            
            if slow_query_range[0] and slow_query_range[1]:
                min_timestamps.append(slow_query_range[0])
                max_timestamps.append(slow_query_range[1])
            
            if conn_range[0] and conn_range[1]:
                min_timestamps.append(conn_range[0])
                max_timestamps.append(conn_range[1])

            if auth_range[0] and auth_range[1]:
                min_timestamps.append(auth_range[0])
                max_timestamps.append(auth_range[1])
            
            if not min_timestamps:
                return None, None
            
            min_epoch = min(min_timestamps)
            max_epoch = max(max_timestamps)
            
            # Convert epochs to datetime objects preserving MongoDB timezone (UTC)
            min_date = datetime.datetime.fromtimestamp(min_epoch, tz=datetime.timezone.utc)
            max_date = datetime.datetime.fromtimestamp(max_epoch, tz=datetime.timezone.utc)
            
            return min_date, max_date
            
        except Exception as e:
            print(f"⚠️  SQL date range query failed, using fallback: {e}")
            return self.get_available_date_range()
    
    def get_distinct_databases_sql(self):
        """Get distinct database names using efficient SQL"""
        if not self.use_database or not hasattr(self, 'db_conn'):
            # Fallback to Python iteration
            databases = set()
            for query in self.slow_queries:
                if query.get('database') and query['database'] not in ('admin', 'local', 'config', '$external', 'unknown', ''):
                    databases.add(query['database'])
            return sorted(list(databases))
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute('''
                SELECT DISTINCT database 
                FROM slow_queries 
                WHERE database IS NOT NULL 
                  AND database != ''
                  AND database NOT IN ('admin', 'local', 'config', '$external', 'unknown')
                ORDER BY database
            ''')
            
            return [row[0] for row in cursor.fetchall()]
            
        except Exception as e:
            print(f"⚠️  SQL distinct databases query failed, using fallback: {e}")
            databases = set()
            for query in self.slow_queries:
                if query.get('database') and query['database'] not in ('admin', 'local', 'config', '$external', 'unknown', ''):
                    databases.add(query['database'])
            return sorted(list(databases))
    
    def get_dashboard_stats_sql(self):
        """Get dashboard statistics using efficient SQL queries"""
        if not self.use_database or not hasattr(self, 'db_conn'):
            # Fallback to Python calculations
            return self._get_dashboard_stats_fallback()
        
        try:
            cursor = self.db_conn.cursor()
            
            # Slow queries stats
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_count,
                    AVG(duration) as avg_duration,
                    MAX(duration) as max_duration,
                    AVG(docs_examined) as avg_docs_examined,
                    COUNT(DISTINCT database) as unique_databases,
                    COUNT(DISTINCT collection) as unique_collections
                FROM slow_queries 
                WHERE duration > 0
            ''')
            slow_stats = cursor.fetchone()
            
            # Authentication stats
            cursor.execute('SELECT COUNT(*) FROM authentications')
            auth_count = cursor.fetchone()[0]
            
            # Connection stats  
            cursor.execute('SELECT COUNT(*) FROM connections')
            conn_count = cursor.fetchone()[0]
            
            # Top slow operations
            cursor.execute('''
                SELECT database, collection, AVG(duration) as avg_dur, COUNT(*) as count
                FROM slow_queries 
                WHERE database IS NOT NULL AND collection IS NOT NULL
                GROUP BY database, collection
                ORDER BY avg_dur DESC
                LIMIT 5
            ''')
            top_operations = cursor.fetchall()
            
            return {
                'total_slow_queries': slow_stats[0] or 0,
                'avg_duration': slow_stats[1] or 0,
                'max_duration': slow_stats[2] or 0,
                'avg_docs_examined': slow_stats[3] or 0,
                'unique_databases': slow_stats[4] or 0,
                'unique_collections': slow_stats[5] or 0,
                'total_authentications': auth_count,
                'total_connections': conn_count,
                'top_operations': [
                    {
                        'database': op[0], 
                        'collection': op[1], 
                        'avg_duration': op[2], 
                        'count': op[3]
                    } for op in top_operations
                ]
            }
            
        except Exception as e:
            print(f"⚠️  SQL dashboard stats query failed, using fallback: {e}")
            return self._get_dashboard_stats_fallback()
    
    def _get_dashboard_stats_fallback(self):
        """Fallback dashboard stats calculation using Python"""
        if not self.slow_queries:
            return {
                'total_slow_queries': 0,
                'avg_duration': 0,
                'max_duration': 0,
                'avg_docs_examined': 0,
                'unique_databases': 0,
                'unique_collections': 0,
                'total_authentications': len(self.authentications),
                'total_connections': len(self.connections),
                'top_operations': []
            }
        
        durations = [q.get('duration', 0) for q in self.slow_queries if q.get('duration', 0) > 0]
        docs_examined = [q.get('docsExamined', 0) for q in self.slow_queries if q.get('docsExamined', 0) > 0]
        databases = set(q.get('database') for q in self.slow_queries if q.get('database'))
        collections = set(q.get('collection') for q in self.slow_queries if q.get('collection'))
        
        return {
            'total_slow_queries': len(self.slow_queries),
            'avg_duration': sum(durations) / len(durations) if durations else 0,
            'max_duration': max(durations) if durations else 0,
            'avg_docs_examined': sum(docs_examined) / len(docs_examined) if docs_examined else 0,
            'unique_databases': len(databases),
            'unique_collections': len(collections),
            'total_authentications': len(self.authentications),
            'total_connections': len(self.connections),
            'top_operations': []
        }
    
    def get_os_resource_workload_summary(self):
        """Get OS resource workload summary with actual storage/CPU metrics from database"""
        if not self.use_database or not hasattr(self, 'db_conn'):
            return {
                'top_bytes_read': [],
                'top_bytes_written': [],
                'top_io_time': [],
                'top_cpu_usage': [],
                'stats': {'total_queries': 0, 'queries_with_bytes_read': 0, 'queries_with_cpu_data': 0, 'total_bytes_read': 0}
            }
        
        try:
            cursor = self.db_conn.cursor()
            
            # Get top disk I/O intensive operations (by bytes_read)
            cursor.execute('''
                SELECT database, collection, plan_summary,
                       COUNT(*) as query_count,
                       SUM(bytes_read) as total_bytes_read,
                       AVG(bytes_read) as avg_bytes_read,
                       AVG(duration) as avg_duration,
                       SUM(time_reading_micros) as total_read_time_micros
                FROM slow_queries 
                WHERE bytes_read > 0
                GROUP BY database, collection, plan_summary
                ORDER BY total_bytes_read DESC
                LIMIT 10
            ''')
            top_bytes_read = [
                {
                    'database': row[0] or 'unknown',
                    'collection': row[1] or 'unknown',
                    'plan_summary': row[2] or 'unknown',
                    'query_count': row[3],
                    'total_bytes_read': row[4],
                    'avg_bytes_read': int(row[5]) if row[5] else 0,
                    'avg_duration': int(row[6]) if row[6] else 0,
                    'total_read_time_micros': row[7] or 0
                }
                for row in cursor.fetchall()
            ]
            
            # Get top write operations (by bytes_written)  
            cursor.execute('''
                SELECT database, collection, plan_summary,
                       COUNT(*) as query_count,
                       SUM(bytes_written) as total_bytes_written,
                       AVG(bytes_written) as avg_bytes_written,
                       AVG(duration) as avg_duration
                FROM slow_queries 
                WHERE bytes_written > 0
                GROUP BY database, collection, plan_summary
                ORDER BY total_bytes_written DESC
                LIMIT 10
            ''')
            top_bytes_written = [
                {
                    'database': row[0] or 'unknown',
                    'collection': row[1] or 'unknown',
                    'plan_summary': row[2] or 'unknown',
                    'query_count': row[3],
                    'total_bytes_written': row[4],
                    'avg_bytes_written': int(row[5]) if row[5] else 0,
                    'avg_duration': int(row[6]) if row[6] else 0
                }
                for row in cursor.fetchall()
            ]
            
            # Get top I/O time operations
            cursor.execute('''
                SELECT database, collection, plan_summary,
                       COUNT(*) as query_count,
                       SUM(time_reading_micros) as total_read_time,
                       AVG(time_reading_micros) as avg_read_time,
                       AVG(duration) as avg_duration,
                       SUM(bytes_read) as total_bytes_read
                FROM slow_queries 
                WHERE time_reading_micros > 0
                GROUP BY database, collection, plan_summary
                ORDER BY total_read_time DESC
                LIMIT 10
            ''')
            top_io_time = [
                {
                    'database': row[0] or 'unknown',
                    'collection': row[1] or 'unknown',
                    'plan_summary': row[2] or 'unknown',
                    'query_count': row[3],
                    'total_read_time_micros': row[4] or 0,
                    'avg_read_time_micros': int(row[5]) if row[5] else 0,
                    'avg_duration': int(row[6]) if row[6] else 0,
                    'total_bytes_read': row[7] or 0
                }
                for row in cursor.fetchall()
            ]
            
            # Get top CPU usage operations
            cursor.execute('''
                SELECT database, collection, plan_summary,
                       COUNT(*) as query_count,
                       SUM(cpu_nanos) as total_cpu_nanos,
                       AVG(cpu_nanos) as avg_cpu_nanos,
                       AVG(duration) as avg_duration
                FROM slow_queries 
                WHERE cpu_nanos > 0
                GROUP BY database, collection, plan_summary
                ORDER BY total_cpu_nanos DESC
                LIMIT 10
            ''')
            top_cpu_usage = [
                {
                    'database': row[0] or 'unknown',
                    'collection': row[1] or 'unknown',
                    'plan_summary': row[2] or 'unknown',
                    'query_count': row[3],
                    'total_cpu_nanos': row[4] or 0,
                    'avg_cpu_nanos': int(row[5]) if row[5] else 0,
                    'avg_duration': int(row[6]) if row[6] else 0
                }
                for row in cursor.fetchall()
            ]
            
            # Get overall statistics
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_queries,
                    COUNT(CASE WHEN bytes_read > 0 THEN 1 END) as queries_with_bytes_read,
                    COUNT(CASE WHEN bytes_written > 0 THEN 1 END) as queries_with_bytes_written,
                    COUNT(CASE WHEN cpu_nanos > 0 THEN 1 END) as queries_with_cpu_data,
                    SUM(bytes_read) as total_bytes_read,
                    SUM(bytes_written) as total_bytes_written,
                    SUM(cpu_nanos) as total_cpu_nanos
                FROM slow_queries
            ''')
            stats_row = cursor.fetchone()
            stats = {
                'total_queries': stats_row[0],
                'queries_with_bytes_read': stats_row[1],
                'queries_with_bytes_written': stats_row[2], 
                'queries_with_cpu_data': stats_row[3],
                'total_bytes_read': stats_row[4] or 0,
                'total_bytes_written': stats_row[5] or 0,
                'total_cpu_nanos': stats_row[6] or 0
            }
            
            return {
                'top_bytes_read': top_bytes_read,
                'top_bytes_written': top_bytes_written,
                'top_io_time': top_io_time,
                'top_cpu_usage': top_cpu_usage,
                'stats': stats
            }
            
        except Exception as e:
            print(f"❌ Error getting OS resource workload: {e}")
            return {
                'top_bytes_read': [],
                'top_bytes_written': [],
                'top_io_time': [],
                'top_cpu_usage': [],
                'stats': {'total_queries': 0, 'queries_with_bytes_read': 0, 'queries_with_cpu_data': 0, 'total_bytes_read': 0}
            }

    # Final overrides to ensure correct, regex-less search methods are bound last
    def search_logs_ephemeral(self, raw_map, keyword=None, field_name=None, field_value=None,
                               start_date=None, end_date=None, limit=100, conditions=None, use_regex=None):
        results = []
        start_epoch = int(start_date.timestamp()) if start_date else None
        end_epoch = int(end_date.timestamp()) if end_date else None

        eff_conditions = []
        if keyword:
            eff_conditions.append({'type': 'keyword', 'value': keyword, 'regex': False, 'case_sensitive': False, 'negate': False})
        if field_name and field_value:
            eff_conditions.append({'type': 'field', 'name': field_name, 'value': field_value, 'regex': False, 'case_sensitive': False, 'negate': False})
        if conditions:
            eff_conditions.extend(conditions)

        for file_path, lines in (raw_map or {}).items():
            for idx, line in enumerate(lines, 1):
                line_stripped = line.rstrip('\n')
                parsed_json = None
                epoch = None
                try:
                    parsed_json = json.loads(line_stripped)
                    epoch = self._parse_epoch_from_log_entry(parsed_json)
                except Exception:
                    parsed_json = None
                    epoch = None

                if start_epoch is not None or end_epoch is not None:
                    if epoch is None:
                        continue
                    if start_epoch is not None and epoch < start_epoch:
                        continue
                    if end_epoch is not None and epoch > end_epoch:
                        continue

                if not self._line_matches_conditions(line_stripped, parsed_json, eff_conditions):
                    continue

                if len(results) < limit:
                    ts = datetime.datetime.fromtimestamp(epoch) if epoch is not None else None
                    results.append({
                        'file_path': file_path,
                        'line_number': idx,
                        'timestamp': ts,
                        'raw_line': line_stripped,
                    })
        return results

    def search_logs_streaming(self, keyword=None, field_name=None, field_value=None,
                               start_date=None, end_date=None, limit=100, compute_total=False, conditions=None, use_regex=None, files=None):
        results = []
        total_found = 0
        start_epoch = int(start_date.timestamp()) if start_date else None
        end_epoch = int(end_date.timestamp()) if end_date else None

        eff_conditions = []
        if keyword:
            eff_conditions.append({'type': 'keyword', 'value': keyword, 'regex': False, 'case_sensitive': False, 'negate': False})
        if field_name and field_value:
            eff_conditions.append({'type': 'field', 'name': field_name, 'value': field_value, 'regex': False, 'case_sensitive': False, 'negate': False})
        if conditions:
            eff_conditions.extend(conditions)

        files_list = getattr(self, 'source_log_files', []) or []
        used_files = files_list if files_list else list((getattr(self, 'raw_log_data', {}) or {}).keys())

        for file_path in used_files:
            try:
                if files_list:
                    fh = open(file_path, 'r', encoding='utf-8', errors='ignore')
                    close_after = True
                    line_iter = fh
                else:
                    lines = getattr(self, 'raw_log_data', {}).get(file_path, [])
                    line_iter = (l for l in lines)
                    close_after = False
                    fh = None

                for idx, line in enumerate(line_iter, 1):
                    line_stripped = line.rstrip('\n')
                    parsed_json = None
                    epoch = None
                    try:
                        parsed_json = json.loads(line_stripped)
                        epoch = self._parse_epoch_from_log_entry(parsed_json)
                    except Exception:
                        parsed_json = None
                        epoch = None

                    if start_epoch is not None or end_epoch is not None:
                        if epoch is None:
                            continue
                        if start_epoch is not None and epoch < start_epoch:
                            continue
                        if end_epoch is not None and epoch > end_epoch:
                            continue

                    if not self._line_matches_conditions(line_stripped, parsed_json, eff_conditions):
                        continue

                    total_found += 1
                    if len(results) < limit:
                        ts = datetime.datetime.fromtimestamp(epoch) if epoch is not None else None
                        results.append({
                            'file_path': file_path,
                            'line_number': idx,
                            'timestamp': ts,
                            'raw_line': line_stripped,
                        })

                if close_after and fh:
                    try:
                        fh.close()
                    except Exception:
                        pass
            except Exception as e:
                try:
                    print(f"⚠️  Streaming search: failed to read {file_path}: {e}")
                except Exception:
                    pass
                continue

        return {'results': results, 'total_found': (total_found if compute_total else len(results))}
