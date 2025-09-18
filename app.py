from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, Response
from werkzeug.utils import secure_filename
import os
import re
import json
import hashlib
from datetime import datetime, timezone

def parse_date_filter(date_str, is_end_date=False):
    """Parse date filter preserving MongoDB log timezone information"""
    if not date_str:
        return None

    try:
        # Try fromisoformat first, then fall back to strptime
        if 'T' in date_str:
            # Handle ISO format - preserve timezone information from MongoDB logs
            if not date_str.endswith(('Z', '+00:00', '-00:00')) and '+' not in date_str[-6:] and '-' not in date_str[-6:]:
                # If no timezone specified, assume local time as entered by user
                parsed_date = datetime.fromisoformat(date_str)
            else:
                # Has timezone info - preserve it
                date_str = date_str.replace('Z', '+00:00')
                parsed_date = datetime.fromisoformat(date_str)
        else:
            # Handle date-only format - assume local time
            parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
            if is_end_date:
                # For end date, set to end of day
                parsed_date = parsed_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Normalize: default to UTC when no timezone is provided
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=timezone.utc)
        else:
            parsed_date = parsed_date.astimezone(timezone.utc)

        return parsed_date
    except (ValueError, TypeError):
        return None

def normalize_datetime_for_comparison(dt):
    """Normalize datetime object for safe comparison - preserve timezone information"""
    if dt is None:
        return None

    # If timezone-naive, assume it's in the same timezone as MongoDB logs
    if not hasattr(dt, 'tzinfo') or dt.tzinfo is None:
        return dt

    # Convert to UTC for consistent comparisons
    return dt.astimezone(timezone.utc)

def format_datetime_for_input(dt):
    """Format datetime for HTML datetime-local input preserving timezone"""
    if dt is None:
        return None
    
    # For timezone-aware datetime, format appropriately
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        # For datetime-local inputs, show the time as-is (preserving the MongoDB log timezone)
        return dt.strftime('%Y-%m-%dT%H:%M')
    else:
        # Timezone-naive - format normally
        return dt.strftime('%Y-%m-%dT%H:%M')

def is_system_database(database_name):
    """Check if a database is a MongoDB system database that should be excluded from analysis"""
    if not database_name:
        return False
    system_databases = {'config', 'admin', 'local', 'system', '$external'}
    return database_name in system_databases or database_name.startswith('$')

def _passes_date_filter(query, start_date, end_date):
    """Helper function to check if query passes date filters"""
    if not start_date and not end_date:
        return True
    
    query_timestamp = normalize_datetime_for_comparison(query.get('timestamp'))
    if not query_timestamp:
        return True
        
    if start_date and query_timestamp < start_date:
        return False
    if end_date and query_timestamp > end_date:
        return False
    return True

def _passes_plan_filter(query, selected_plan):
    """Helper function to check if query passes plan filter"""
    if selected_plan == 'all':
        return True
        
    query_plan = query.get('plan_summary', 'None')
    if selected_plan == 'COLLSCAN' and query_plan != 'COLLSCAN':
        return False
    elif selected_plan == 'IXSCAN' and query_plan != 'IXSCAN':
        return False
    elif selected_plan == 'other' and query_plan in ['COLLSCAN', 'IXSCAN']:
        return False
    return True


def _aggregate_patterns_by_group(patterns, grouping_type='pattern_key'):
    """Aggregate slow query patterns according to grouping strategy."""
    allowed = {'pattern_key', 'namespace', 'query_hash'}
    if grouping_type not in allowed:
        grouping_type = 'pattern_key'

    if grouping_type == 'pattern_key':
        return patterns

    priority_rank = {'low': 0, 'medium': 1, 'high': 2}
    aggregated = {}

    for original_key, pattern in patterns.items():
        database = pattern.get('database', 'unknown')
        collection = pattern.get('collection', 'unknown')
        query_hash = pattern.get('query_hash', 'unknown')

        if grouping_type == 'namespace':
            group_key = f"{database}.{collection}"
        else:  # query_hash
            group_key = query_hash

        agg = aggregated.get(group_key)
        if agg is None:
            agg = pattern.copy()
            agg['pattern_key'] = group_key
            agg['grouping_type'] = grouping_type
            agg['_total_duration'] = agg.get('avg_duration', 0) * agg.get('total_count', 0)
            agg['_total_index_eff'] = agg.get('avg_index_efficiency', 0) * agg.get('total_count', 0)
            agg['_priority_rank'] = priority_rank.get(agg.get('optimization_potential', 'low'), 0)
            agg['_max_duration'] = agg.get('max_duration', 0)

            if grouping_type == 'namespace':
                agg['database'] = database
                agg['collection'] = collection
                agg['query_hash'] = query_hash
            else:  # query_hash grouping
                agg['database'] = database
                agg['collection'] = collection
                agg['query_hash'] = group_key

            aggregated[group_key] = agg
            continue

        # Update aggregate metrics
        executions = pattern.get('total_count', 0)
        agg['_total_duration'] += pattern.get('avg_duration', 0) * executions
        agg['_total_index_eff'] += pattern.get('avg_index_efficiency', 0) * executions
        agg['total_count'] += executions
        agg['min_duration'] = min(agg.get('min_duration', pattern.get('min_duration', 0)), pattern.get('min_duration', 0))
        agg['max_duration'] = max(agg.get('max_duration', pattern.get('max_duration', 0)), pattern.get('max_duration', 0))
        agg['total_docs_examined'] = agg.get('total_docs_examined', 0) + pattern.get('total_docs_examined', 0)
        agg['total_returned'] = agg.get('total_returned', 0) + pattern.get('total_returned', 0)
        agg['total_keys_examined'] = agg.get('total_keys_examined', 0) + pattern.get('total_keys_examined', 0)
        agg['complexity_score'] = max(agg.get('complexity_score', 0), pattern.get('complexity_score', 0))
        agg['median_duration'] = max(agg.get('median_duration', 0), pattern.get('median_duration', 0))

        first_seen = pattern.get('first_seen')
        last_seen = pattern.get('last_seen')
        if first_seen and (not agg.get('first_seen') or first_seen < agg['first_seen']):
            agg['first_seen'] = first_seen
        if last_seen and (not agg.get('last_seen') or last_seen > agg['last_seen']):
            agg['last_seen'] = last_seen

        rank = priority_rank.get(pattern.get('optimization_potential', 'low'), 0)
        if rank > agg['_priority_rank']:
            agg['optimization_potential'] = pattern.get('optimization_potential', 'low')
            agg['_priority_rank'] = rank

        if grouping_type == 'namespace':
            if agg.get('query_hash') != pattern.get('query_hash'):
                agg['query_hash'] = 'MIXED'
            if agg.get('plan_summary') != pattern.get('plan_summary'):
                agg['plan_summary'] = 'MIXED'
        else:  # query_hash grouping
            if agg.get('database') != database:
                agg['database'] = 'MIXED'
            if agg.get('collection') != collection:
                agg['collection'] = 'MIXED'
            if agg.get('plan_summary') != pattern.get('plan_summary'):
                agg['plan_summary'] = 'MIXED'

        if agg.get('query_type') != pattern.get('query_type'):
            agg['query_type'] = 'mixed'

        if pattern.get('max_duration', 0) > agg.get('_max_duration', 0):
            agg['_max_duration'] = pattern.get('max_duration', 0)
            agg['sample_query'] = pattern.get('sample_query')
            agg['slowest_query_full'] = pattern.get('slowest_query_full')
            agg['slowest_execution_timestamp'] = pattern.get('slowest_execution_timestamp')

        if pattern.get('is_estimated'):
            agg['is_estimated'] = True

    # Finalise aggregates
    for agg in aggregated.values():
        total_count = max(agg.get('total_count', 0), 1)
        agg['avg_duration'] = agg['_total_duration'] / total_count
        agg['avg_index_efficiency'] = agg['_total_index_eff'] / total_count if total_count else 0

        total_docs_examined = agg.get('total_docs_examined', 0)
        total_returned = agg.get('total_returned', 0)
        if total_docs_examined > 0:
            agg['avg_selectivity'] = (total_returned / total_docs_examined) * 100
        else:
            agg['avg_selectivity'] = 0

        agg['avg_docs_examined'] = total_docs_examined / total_count if total_count else 0
        agg['avg_docs_returned'] = total_returned / total_count if total_count else 0
        agg['avg_keys_examined'] = agg.get('total_keys_examined', 0) / total_count if total_count else 0
        agg['median_duration'] = agg.get('median_duration', agg['avg_duration'])

        agg.pop('_total_duration', None)
        agg.pop('_total_index_eff', None)
        agg.pop('_priority_rank', None)
        agg.pop('_max_duration', None)

    sorted_items = sorted(
        aggregated.items(),
        key=lambda item: item[1].get('avg_duration', 0) * item[1].get('total_count', 0),
        reverse=True
    )
    return dict(sorted_items)

def _clean_query_text_for_export(query_text):
    """Convert query text to parsed JSON object for clean export"""
    if not query_text or query_text == 'N/A':
        return 'N/A'
    
    try:
        # Try to parse as JSON and return the actual JSON object
        import json
        if query_text.startswith('{') and query_text.endswith('}'):
            # Parse JSON and return the object directly (not a string)
            parsed_json = json.loads(query_text)
            return parsed_json
        else:
            # Not JSON - clean basic escape sequences and return as string
            return query_text.replace('\\"', '"').replace('\\\\', '\\')
    except (json.JSONDecodeError, Exception):
        # If JSON parsing fails, fall back to simple replacement
        return query_text.replace('\\"', '"').replace('\\\\', '\\')

def _batch_get_original_log_lines(queries):
    """Efficiently batch process original log line retrieval"""
    # Group queries by file path to minimize file operations
    file_groups = {}
    for i, query in enumerate(queries):
        file_path = query.get('file_path')
        if file_path not in file_groups:
            file_groups[file_path] = []
        file_groups[file_path].append((i, query))
    
    # Create results array
    results = [None] * len(queries)
    
    # Process each file once
    for file_path, file_queries in file_groups.items():
        if not file_path:
            continue
            
        # Read file lines needed in one go
        line_numbers = [q.get('line_number') for _, q in file_queries if q.get('line_number')]
        if not line_numbers:
            continue
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for idx, query in file_queries:
                    line_num = query.get('line_number')
                    if line_num and 1 <= line_num <= len(lines):
                        results[idx] = lines[line_num - 1].strip()
        except (IOError, OSError):
            continue
    
    return results
from collections import defaultdict, Counter
import zipfile
import tarfile
import gzip
import shutil

# Import optimized analyzer
try:
    from optimized_analyzer import OptimizedMongoLogAnalyzer
    OPTIMIZED_ANALYZER_AVAILABLE = True
    print("✅ Optimized analyzer loaded successfully!")
except ImportError as e:
    print(f"⚠️  Optimized analyzer not available: {e}")
    OPTIMIZED_ANALYZER_AVAILABLE = False

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-change-this'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['TEMP_FOLDER'] = 'temp'

# Enable Jinja2 template caching for better performance
app.jinja_env.cache = {}
app.jinja_env.auto_reload = False  # Disable auto-reload in production
app.config['TEMPLATES_AUTO_RELOAD'] = False

# Performance limits for query results
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
DEFAULT_QUERY_LIMIT = 10000  # Maximum records to query without pagination

# Add custom Jinja2 filter for index information
@app.template_filter('extract_index_info')
def extract_index_info_filter(plan_summary):
    """Template filter to extract index information from plan summary"""
    return _extract_index_info(plan_summary)
# Removed file size limit - can now upload files of any size

# Ensure folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['TEMP_FOLDER'], exist_ok=True)

# Legacy: Previously restricted file extensions - now accepts any extension
# ALLOWED_EXTENSIONS = {'log', 'txt', 'zip', 'tar', 'gz'}  # No longer used

def paginate_data(data, page=1, per_page=DEFAULT_PAGE_SIZE):
    """
    Paginate a list of data with performance limits
    
    Args:
        data: List of items to paginate
        page: Current page number (1-based)
        per_page: Number of items per page (with limits)
    
    Returns:
        dict with paginated data and pagination info
    """
    # Apply performance limits
    if per_page == 'all' or per_page == -1:
        per_page = min(len(data), DEFAULT_QUERY_LIMIT)
    else:
        per_page = min(int(per_page), MAX_PAGE_SIZE)
    
    if per_page == len(data) and len(data) <= DEFAULT_QUERY_LIMIT:
        return {
            'items': data,
            'page': 1,
            'per_page': len(data),
            'total': len(data),
            'pages': 1,
            'has_prev': False,
            'has_next': False,
            'prev_num': None,
            'next_num': None
        }
    
    try:
        per_page = int(per_page)
        page = int(page)
    except (ValueError, TypeError):
        per_page = 100
        page = 1
    
    if page < 1:
        page = 1
    
    total = len(data)
    pages = max(1, (total + per_page - 1) // per_page)  # Ceiling division
    
    if page > pages:
        page = pages
    
    start = (page - 1) * per_page
    end = start + per_page
    
    return {
        'items': data[start:end],
        'page': page,
        'per_page': per_page,
        'total': total,
        'pages': pages,
        'has_prev': page > 1,
        'has_next': page < pages,
        'prev_num': page - 1 if page > 1 else None,
        'next_num': page + 1 if page < pages else None
    }

def get_pagination_params(request):
    """Extract pagination parameters from request"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100)
    
    # Handle 'all' option
    if per_page == 'all':
        per_page = 'all'
    else:
        try:
            per_page = int(per_page)
            if per_page not in [100, 250, 500, 1000]:
                per_page = 100
        except (ValueError, TypeError):
            per_page = 100
    
    return page, per_page

def allowed_file(filename):
    # Accept any file extension - let users upload any file type
    # The log parsing logic will determine if it's valid MongoDB log content
    if filename and filename.strip():
        return True
    
    return False

def is_archive_file(filename):
    """Check if file is a compressed archive"""
    filename_lower = filename.lower()
    return (
        filename_lower.endswith('.zip') or 
        filename_lower.endswith('.tar') or 
        filename_lower.endswith('.tar.gz') or
        filename_lower.endswith('.tgz') or
        filename_lower.endswith('.tar.bz2') or
        filename_lower.endswith('.tbz2') or
        filename_lower.endswith('.tbz') or
        (filename_lower.endswith('.gz') and not filename_lower.endswith('.tar.gz'))
    )

def extract_archive(archive_path, extract_to_dir):
    """Extract archive and return list of extracted log files"""
    extracted_files = []

    try:
        # Create extraction directory
        os.makedirs(extract_to_dir, exist_ok=True)

        def unique_path(directory, filename):
            base_name, ext = os.path.splitext(filename)
            candidate = filename
            counter = 1
            while os.path.exists(os.path.join(directory, candidate)):
                candidate = f"{base_name}_{counter}{ext}"
                counter += 1
            return os.path.join(directory, candidate)

        if archive_path.lower().endswith('.zip'):
            # Handle ZIP files
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                for info in zip_ref.infolist():
                    if info.is_dir():
                        continue
                    member_name = os.path.basename(info.filename)
                    if not member_name:
                        continue
                    if not is_log_file(member_name):
                        continue
                    target_path = unique_path(extract_to_dir, member_name)
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    with zip_ref.open(info) as src, open(target_path, 'wb') as dst:
                        shutil.copyfileobj(src, dst)
                    extracted_files.append(target_path)

        elif archive_path.lower().endswith(('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tbz')):
            # Handle TAR variants
            if archive_path.lower().endswith(('.tar.gz', '.tgz')):
                mode = 'r:gz'
            elif archive_path.lower().endswith(('.tar.bz2', '.tbz2', '.tbz')):
                mode = 'r:bz2'
            else:
                mode = 'r'
            with tarfile.open(archive_path, mode) as tar_ref:
                for member in tar_ref.getmembers():
                    member_name = member.name.split('/')[-1]  # Get filename without path
                    if (member.isfile() and 
                        is_log_file(member_name) and
                        member_name):  # Skip directories and empty names
                        target_path = unique_path(extract_to_dir, member_name)
                        extracted_file = tar_ref.extractfile(member)
                        if extracted_file:
                            with open(target_path, 'wb') as dst:
                                shutil.copyfileobj(extracted_file, dst)
                            extracted_files.append(target_path)


        elif archive_path.lower().endswith('.gz') and not archive_path.lower().endswith('.tar.gz'):
            # Handle standalone .gz files
            with gzip.open(archive_path, 'rb') as gz_file:
                # Create output filename by removing .gz extension
                base_name = os.path.basename(archive_path)
                if base_name.lower().endswith('.gz'):
                    output_name = base_name[:-3]  # Remove .gz extension
                else:
                    output_name = base_name + '.extracted'
                
                output_path = unique_path(extract_to_dir, output_name)

                # Write decompressed content
                with open(output_path, 'wb') as output_file:
                    shutil.copyfileobj(gz_file, output_file)

                # Only add if it looks like a log file
                if is_log_file(output_name):
                    extracted_files.append(output_path)
        
        return extracted_files
        
    except Exception as e:
        print(f"Error extracting archive {archive_path}: {e}")
        return []

def is_log_file(filename):
    """Check if file is likely a log file - now very permissive since we accept any extension"""
    filename_lower = filename.lower()
    
    # Standard log file extensions
    if filename_lower.endswith(('.log', '.logs', '.txt')):
        return True
    
    # Date-suffixed log files (e.g., mongodb.log.20250826)
    if '.log.' in filename_lower or '.txt.' in filename_lower:
        return True
    
    # Check for MongoDB-related keywords in filename
    mongodb_keywords = ['mongodb', 'mongo', 'slow', 'error', 'audit', 'query']
    if any(keyword in filename_lower for keyword in mongodb_keywords):
        return True
    
    # Check pattern: something.log.datepattern or something.txt.datepattern
    parts = filename_lower.split('.')
    if len(parts) >= 3:
        # Check if second-to-last part is 'log' or 'txt'
        if parts[-2] in ['log', 'txt']:
            # Check if last part looks like a date
            last_part = parts[-1]
            if (last_part.isdigit() and len(last_part) == 8) or \
               (last_part.replace('-', '').replace('_', '').isdigit()):
                return True
    
    # If no extension or unknown extension, assume it could be a log file
    # This is very permissive - let the content parsing determine if it's valid
    if '.' not in filename or filename_lower.split('.')[-1] not in ['zip', 'tar', 'gz', 'rar']:
        return True
    
    return False

def cleanup_temp_folder():
    """Clean up temp folder before each upload"""
    temp_folder = app.config['TEMP_FOLDER']
    if os.path.exists(temp_folder):
        try:
            # Remove all contents of temp folder
            for filename in os.listdir(temp_folder):
                file_path = os.path.join(temp_folder, filename)
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            print(f"Cleaned up temp folder: {temp_folder}")
        except Exception as e:
            print(f"Error cleaning temp folder: {e}")

def create_temp_file(original_filename):
    """Create a unique temporary file path"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
    name, ext = os.path.splitext(original_filename)
    temp_filename = f"temp_{timestamp}_{name}{ext}"
    return os.path.join(app.config['TEMP_FOLDER'], temp_filename)

class MongoLogAnalyzer:
    def __init__(self):
        self.connections = []
        self.slow_queries = []
        self.authentications = []
        self.database_access = []
        self.raw_log_data = {}  # Store raw log lines for searching
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
            'files_processed': 0,
            'parsing_errors': [],
            'file_summaries': []  # Per-file tracking
        }
        
    def parse_log_file(self, filepath):
        """Parse MongoDB log file and extract connection information"""
        # Don't clear arrays here as we might be processing multiple files
        filename = os.path.basename(filepath)
        
        # Initialize per-file tracking
        file_summary = {
            'filename': filename,
            'filepath': filepath,
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
            'has_useful_data': False
        }
        
        # Update global counters
        self.parsing_summary['files_processed'] += 1
        
        # Store raw lines for search functionality
        raw_lines = []
        
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
            for line_num, line in enumerate(file, 1):
                # Store the raw line
                raw_lines.append(line.rstrip())
                
                # Update both global and file-specific counters
                self.parsing_summary['total_lines'] += 1
                file_summary['total_lines'] += 1
                line = line.strip()
                
                if not line:
                    self.parsing_summary['skipped_lines'] += 1
                    file_summary['skipped_lines'] += 1
                    continue
                
                try:
                    # Try to parse as JSON format first
                    if line.startswith('{') and line.endswith('}'):
                        self.parsing_summary['json_lines'] += 1
                        file_summary['json_lines'] += 1
                        events_found = self.parse_json_log_line(line, file_summary, filepath, line_num)
                    else:
                        # Fall back to legacy text format
                        self.parsing_summary['text_lines'] += 1
                        file_summary['text_lines'] += 1
                        events_found = self.parse_text_log_line(line, file_summary)
                    
                    self.parsing_summary['parsed_lines'] += 1
                    file_summary['parsed_lines'] += 1
                    
                    # Mark file as having useful data if events were found
                    if events_found:
                        file_summary['has_useful_data'] = True
                    
                except Exception as e:
                    self.parsing_summary['error_lines'] += 1
                    file_summary['error_lines'] += 1
                    error_msg = f"Line {line_num}: {str(e)[:100]}"
                    self.parsing_summary['parsing_errors'].append(f"{filename}: {error_msg}")
                    file_summary['parsing_errors'].append(error_msg)
                    print(f"Error parsing line {line_num} in {filename}: {e}")
                    continue
        
        # Store raw lines for search functionality
        self.raw_log_data[filepath] = raw_lines
        
        # Store file summary
        self.parsing_summary['file_summaries'].append(file_summary)
    
    def parse_json_log_line(self, line, file_summary=None, filepath=None, line_num=None):
        """Parse MongoDB JSON format log line"""
        events_found = False
        try:
            log_entry = json.loads(line)
            
            # Extract timestamp
            timestamp = self.extract_json_timestamp(log_entry)
            
            # Parse different types of events
            component = log_entry.get('c', '')
            message = log_entry.get('msg', '')
            log_id = log_entry.get('id', 0)
            attr = log_entry.get('attr', {})
            
            # Extract connection ID from context
            ctx = log_entry.get('ctx', '')
            if ctx.startswith('conn') and ctx != 'conn':
                conn_id = ctx.replace('conn', '')
            elif ctx == 'listener':
                # For listener events, get connectionId from attr
                conn_id = str(attr.get('connectionId', 'unknown'))
            else:
                conn_id = 'unknown'
            
            # Parse connection events
            if component == 'NETWORK' and ('connection accepted' in message or 'Connection accepted' in message):
                remote = attr.get('remote', '')
                if ':' in remote:
                    ip, port = remote.rsplit(':', 1)
                    self.connections.append({
                        'timestamp': timestamp,
                        'ip': ip,
                        'port': port,
                        'connection_id': conn_id,
                        'type': 'connection_accepted',
                        'user': None,
                        'database': None
                    })
                    self.parsing_summary['connection_events'] += 1
                    if file_summary:
                        file_summary['connection_events'] += 1
                    events_found = True
            
            # Parse connection end events
            elif component == 'NETWORK' and ('connection ended' in message or 'Connection ended' in message or 'end connection' in message):
                remote = attr.get('remote', '')
                if ':' in remote:
                    ip, port = remote.rsplit(':', 1)
                    self.connections.append({
                        'timestamp': timestamp,
                        'ip': ip,
                        'port': port,
                        'connection_id': conn_id,
                        'type': 'connection_ended'
                    })
                    self.parsing_summary['connection_events'] += 1
                    if file_summary:
                        file_summary['connection_events'] += 1
                    events_found = True
            
            # Parse authentication events
            elif component == 'ACCESS' and ('Successfully authenticated' in message or 'Authentication succeeded' in message):
                principal = attr.get('user', attr.get('principalName', ''))
                auth_db = attr.get('db', attr.get('authenticationDatabase', ''))
                mechanism = attr.get('mechanism', 'SCRAM-SHA-256')
                
                self.authentications.append({
                    'timestamp': timestamp,
                    'connection_id': conn_id,
                    'username': principal,
                    'database': auth_db,
                    'mechanism': mechanism,
                    'remote': attr.get('remote', ''),
                    'type': 'auth_success'
                })
                self.parsing_summary['auth_events'] += 1
                if file_summary:
                    file_summary['auth_events'] += 1
                events_found = True
            
            # Parse authentication failures
            elif component == 'ACCESS' and ('Authentication failed' in message or 'Failed to authenticate' in message):
                principal = attr.get('principalName', attr.get('user', ''))
                auth_db = attr.get('authenticationDatabase', attr.get('db', ''))
                mechanism = attr.get('mechanism', 'SCRAM-SHA-256')
                
                self.authentications.append({
                    'timestamp': timestamp,
                    'connection_id': conn_id,
                    'username': principal,
                    'database': auth_db,
                    'mechanism': mechanism,
                    'remote': attr.get('remote', ''),
                    'type': 'auth_failure'
                })
                self.parsing_summary['auth_events'] += 1
                if file_summary:
                    file_summary['auth_events'] += 1
                events_found = True
            
            # Parse command operations
            elif component == 'COMMAND' and (message == 'command' or message == 'Slow query'):
                command_info = attr.get('command', {})
                duration_stats = attr.get('durationStats', {})
                # Handle both duration formats
                if duration_stats:
                    duration = duration_stats.get('millis', 0)
                else:
                    # Direct durationMillis field (for slow query logs)
                    duration = attr.get('durationMillis', 0)
                
                # Extract database and collection info
                command_name = next(iter(command_info.keys())) if command_info else 'unknown'
                
                # Get database and collection from namespace
                ns = attr.get('ns', '')
                if '.' in ns:
                    database, collection = ns.split('.', 1)
                else:
                    database = attr.get('database', 'unknown')
                    # Try to get collection from command
                    if isinstance(command_info.get(command_name), str):
                        collection = command_info.get(command_name, 'unknown')
                    else:
                        collection = 'unknown'
                
                # Store database access
                self.database_access.append({
                    'timestamp': timestamp,
                    'connection_id': conn_id,
                    'database': database,
                    'collection': collection,
                    'command_type': command_name.lower(),
                    'username': None,  # Will be filled later
                    'operation': 'command'
                })
                self.parsing_summary['command_events'] += 1
                if file_summary:
                    file_summary['command_events'] += 1
                events_found = True
                
                # Check if it's a slow query
                if duration > 100:  # Consider queries > 100ms as slow
                    plan_summary = (
                        attr.get('planSummary') or 
                        attr.get('command', {}).get('planSummary') or 'None'
                    )
                    query_hash = attr.get('queryHash', None)
                    self.slow_queries.append({
                        'timestamp': timestamp,
                        'connection_id': conn_id,
                        'duration': duration,
                        'database': database,
                        'collection': collection,
                        'query': json.dumps(command_info, indent=None),
                        'plan_summary': plan_summary,
                        'query_hash': query_hash,
                        'username': None,  # Will be filled by correlation
                        'file_path': filepath,
                        'line_number': line_num,
                        # Extract performance metrics with proper None checking
                        'docsExamined': (
                            attr.get('docsExamined') if attr.get('docsExamined') is not None else
                            attr.get('docs_examined') if attr.get('docs_examined') is not None else
                            attr.get('totalDocsExamined') if attr.get('totalDocsExamined') is not None else
                            attr.get('command', {}).get('docsExamined') if attr.get('command', {}).get('docsExamined') is not None else 0
                        ),
                        'keysExamined': (
                            attr.get('keysExamined') if attr.get('keysExamined') is not None else
                            attr.get('keys_examined') if attr.get('keys_examined') is not None else
                            attr.get('totalKeysExamined') if attr.get('totalKeysExamined') is not None else
                            attr.get('command', {}).get('keysExamined') if attr.get('command', {}).get('keysExamined') is not None else 0
                        ),
                        'nReturned': (
                            attr.get('nReturned') if attr.get('nReturned') is not None else
                            attr.get('n_returned') if attr.get('n_returned') is not None else
                            attr.get('nreturned') if attr.get('nreturned') is not None else
                            attr.get('numReturned') if attr.get('numReturned') is not None else
                            attr.get('command', {}).get('nReturned') if attr.get('command', {}).get('nReturned') is not None else 0
                        ),
                        'planCacheKey': attr.get('planCacheKey', '')
                    })
                    self.parsing_summary['slow_query_events'] += 1
                    if file_summary:
                        file_summary['slow_query_events'] += 1
            
            return events_found
            
        except json.JSONDecodeError as e:
            # If JSON parsing fails, try to extract basic info for slow queries
            if 'Slow query' in line and 'durationMillis' in line:
                # Try to extract basic slow query info even from malformed JSON
                try:
                    # Extract namespace
                    ns_match = re.search(r'"ns":"([^"]+)"', line)
                    database = 'unknown'
                    collection = 'unknown'
                    if ns_match and '.' in ns_match.group(1):
                        database, collection = ns_match.group(1).split('.', 1)
                    
                    # Extract duration
                    duration_match = re.search(r'"durationMillis":(\d+)', line)
                    duration = int(duration_match.group(1)) if duration_match else 0
                    
                    # Extract connection ID
                    ctx_match = re.search(r'"ctx":"(conn\d+)"', line)
                    if ctx_match:
                        conn_id = ctx_match.group(1).replace('conn', '')
                    else:
                        # Try alternative patterns
                        ctx_match2 = re.search(r'"ctx":"([^"]*conn\d+[^"]*)"', line)
                        if ctx_match2:
                            ctx_str = ctx_match2.group(1)
                            conn_match = re.search(r'conn(\d+)', ctx_str)
                            conn_id = conn_match.group(1) if conn_match else 'unknown'
                        else:
                            conn_id = 'unknown'
                    
                    # Extract plan summary
                    plan_summary_match = re.search(r'"planSummary":"([^"]+)"', line)
                    plan_summary = plan_summary_match.group(1) if plan_summary_match else 'None'
                    
                    # Extract timestamp
                    timestamp_match = re.search(r'"t":\{"\\$date":"([^"]+)"\}', line)
                    if timestamp_match:
                        timestamp = datetime.fromisoformat(timestamp_match.group(1).replace('Z', '+00:00'))
                    else:
                        timestamp = datetime.now()
                    
                    # Extract query details from the line
                    query_data = {}
                    try:
                        # Try to extract command details
                        command_match = re.search(r'"command":\{([^}]*(?:\{[^}]*\}[^}]*)*)\}', line)
                        if command_match:
                            command_str = '{' + command_match.group(1) + '}'
                            try:
                                query_data = json.loads(command_str)
                            except:
                                query_data = {"raw_command": command_match.group(1)}
                        else:
                            # Fallback to simple description
                            query_data = {"description": f"Query on {database}.{collection}"}
                    except:
                        query_data = {"description": f"Query on {database}.{collection}"}
                    
                    if duration > 100:  # Only add if it's actually slow
                        query_hash = attr.get('queryHash', None)
                        self.slow_queries.append({
                            'timestamp': timestamp,
                            'connection_id': conn_id,
                            'duration': duration,
                            'database': database,
                            'collection': collection,
                            'query': json.dumps(query_data, indent=None),
                            'plan_summary': plan_summary,
                            'query_hash': query_hash,
                            'username': None,  # Will be filled by correlation
                            'file_path': filepath,
                            'line_number': line_num,
                            # Extract performance metrics with proper None checking
                            'docsExamined': (
                                attr.get('docsExamined') if attr.get('docsExamined') is not None else
                                attr.get('docs_examined') if attr.get('docs_examined') is not None else
                                attr.get('totalDocsExamined') if attr.get('totalDocsExamined') is not None else
                                attr.get('command', {}).get('docsExamined') if attr.get('command', {}).get('docsExamined') is not None else 0
                            ),
                            'keysExamined': (
                                attr.get('keysExamined') if attr.get('keysExamined') is not None else
                                attr.get('keys_examined') if attr.get('keys_examined') is not None else
                                attr.get('totalKeysExamined') if attr.get('totalKeysExamined') is not None else
                                attr.get('command', {}).get('keysExamined') if attr.get('command', {}).get('keysExamined') is not None else 0
                            ),
                            'nReturned': (
                                attr.get('nReturned') if attr.get('nReturned') is not None else
                                attr.get('n_returned') if attr.get('n_returned') is not None else
                                attr.get('nreturned') if attr.get('nreturned') is not None else
                                attr.get('numReturned') if attr.get('numReturned') is not None else
                                attr.get('command', {}).get('nReturned') if attr.get('command', {}).get('nReturned') is not None else 0
                            ),
                            'planCacheKey': attr.get('planCacheKey', '')
                        })
                        
                        self.database_access.append({
                            'timestamp': timestamp,
                            'connection_id': conn_id,
                            'database': database,
                            'collection': collection,
                            'command_type': 'aggregate',  # Most of the slow queries are aggregate
                            'username': None,
                            'operation': 'command'
                        })
                        
                        self.parsing_summary['slow_query_events'] += 1
                        self.parsing_summary['command_events'] += 1
                        if file_summary:
                            file_summary['slow_query_events'] += 1
                            file_summary['command_events'] += 1
                        events_found = True
                        
                except Exception as extract_error:
                    print(f"Could not extract slow query info: {extract_error}")
            
            # Fall back to text format parsing
            return self.parse_text_log_line(line, file_summary) or events_found
    
    def parse_text_log_line(self, line, file_summary=None):
        """Parse legacy text format MongoDB log lines"""
        events_found = False
        
        # Parse connection events
        connection_match = re.search(r'connection accepted from ([0-9.]+):(\d+) #conn(\d+)', line)
        if connection_match:
            ip = connection_match.group(1)
            port = connection_match.group(2)
            conn_id = connection_match.group(3)
            timestamp = self.extract_timestamp(line)
            
            self.connections.append({
                'timestamp': timestamp,
                'ip': ip,
                'port': port,
                'connection_id': conn_id,
                'type': 'connection_accepted',
                'user': None,
                'database': None
            })
            if file_summary:
                file_summary['connection_events'] += 1
            events_found = True
        
        # Parse end connection events
        end_connection_match = re.search(r'end connection ([0-9.]+):(\d+) \((\d+) connection', line)
        if end_connection_match:
            ip = end_connection_match.group(1)
            port = end_connection_match.group(2)
            timestamp = self.extract_timestamp(line)
            
            self.connections.append({
                'timestamp': timestamp,
                'ip': ip,
                'port': port,
                'connection_id': None,
                'type': 'connection_ended'
            })
        
        # Parse authentication events
        auth_match = re.search(r'Successfully authenticated as principal ([^@\s]+)(@([^@\s]+))? on ([^@\s]+)', line)
        if auth_match:
            username = auth_match.group(1)
            database = auth_match.group(4)
            timestamp = self.extract_timestamp(line)
            conn_match = re.search(r'\[conn(\d+)\]', line)
            conn_id = conn_match.group(1) if conn_match else 'unknown'
            
            # Extract authentication mechanism if available
            mechanism = 'SCRAM-SHA-1'  # Default MongoDB mechanism
            if 'SCRAM-SHA-256' in line:
                mechanism = 'SCRAM-SHA-256'
            elif 'MONGODB-CR' in line:
                mechanism = 'MONGODB-CR'
            elif 'GSSAPI' in line:
                mechanism = 'GSSAPI'
            elif 'X.509' in line:
                mechanism = 'X.509'
            
            # Extract remote IP from log line if available
            remote = ''
            remote_match = re.search(r'remote:\s*([^\s,]+)', line)
            if remote_match:
                remote = remote_match.group(1)
            
            self.authentications.append({
                'timestamp': timestamp,
                'connection_id': conn_id,
                'username': username,
                'database': database,
                'mechanism': mechanism,
                'remote': remote,
                'type': 'auth_success'
            })
        
        # Parse authentication failures
        auth_fail_match = re.search(r'Failed to authenticate ([^@\s]+)(@([^@\s]+))? on ([^@\s]+)', line)
        if auth_fail_match:
            username = auth_fail_match.group(1)
            database = auth_fail_match.group(4)
            timestamp = self.extract_timestamp(line)
            conn_match = re.search(r'\[conn(\d+)\]', line)
            conn_id = conn_match.group(1) if conn_match else 'unknown'
            
            # Extract mechanism from failure message
            mechanism = 'SCRAM-SHA-1'  # Default
            if 'SCRAM-SHA-1' in line:
                mechanism = 'SCRAM-SHA-1'
            elif 'SCRAM-SHA-256' in line:
                mechanism = 'SCRAM-SHA-256'
            elif 'MONGODB-CR' in line:
                mechanism = 'MONGODB-CR'
            elif 'GSSAPI' in line:
                mechanism = 'GSSAPI'
            elif 'X.509' in line:
                mechanism = 'X.509'
            
            # Extract remote IP from log line if available
            remote = ''
            remote_match = re.search(r'remote:\s*([^\s,]+)', line)
            if remote_match:
                remote = remote_match.group(1)
            
            self.authentications.append({
                'timestamp': timestamp,
                'connection_id': conn_id,
                'username': username,
                'database': database,
                'mechanism': mechanism,
                'remote': remote,
                'type': 'auth_failure'
            })
        
        # Parse database access from commands
        db_access_match = re.search(r'command ([^.\s]+)\.([^.\s]+) command: (\w+)', line)
        if db_access_match:
            database = db_access_match.group(1)
            collection = db_access_match.group(2)
            command_type = db_access_match.group(3).lower()
            timestamp = self.extract_timestamp(line)
            conn_match = re.search(r'\[conn(\d+)\]', line)
            conn_id = conn_match.group(1) if conn_match else 'unknown'
            
            self.database_access.append({
                'timestamp': timestamp,
                'connection_id': conn_id,
                'database': database,
                'collection': collection,
                'command_type': command_type,
                'username': None,  # Will be filled later
                'operation': 'command'
            })
        
        # Parse slow queries
        if 'ms' in line and ('command' in line or 'query' in line):
            slow_query_match = re.search(r'(\d+)ms$', line)
            if slow_query_match:
                duration = int(slow_query_match.group(1))
                if duration > 100:  # Consider queries > 100ms as slow
                    timestamp = self.extract_timestamp(line)
                    conn_match = re.search(r'\[conn(\d+)\]', line)
                    conn_id = conn_match.group(1) if conn_match else 'unknown'
                    
                    # Extract database and collection from slow query
                    db_match = re.search(r'command ([^.\s]+)\.([^.\s]+)', line)
                    database = db_match.group(1) if db_match else 'unknown'
                    collection = db_match.group(2) if db_match else 'unknown'
                    
                    self.slow_queries.append({
                        'timestamp': timestamp,
                        'connection_id': conn_id,
                        'duration': duration,
                        'database': database,
                        'collection': collection,
                        'query': line,
                        'query_hash': None,  # Legacy format - no hash available
                        'file_path': filepath,
                        'line_number': line_num
                    })
                    if file_summary:
                        file_summary['slow_query_events'] += 1
                    events_found = True
        
        return events_found
    
    def extract_json_timestamp(self, log_entry):
        """Extract timestamp from JSON log entry"""
        try:
            t_obj = log_entry.get('t', {})
            if isinstance(t_obj, dict) and '$date' in t_obj:
                date_str = t_obj['$date']
                # Parse ISO format timestamp
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return datetime.now()
        except:
            return datetime.now()
    
    def extract_timestamp(self, line):
        """Extract timestamp from MongoDB log line"""
        # MongoDB timestamp format: 2023-08-26T10:30:45.123+0000
        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}[+-]\d{4})', line)
        if timestamp_match:
            try:
                return datetime.fromisoformat(timestamp_match.group(1).replace('Z', '+00:00'))
            except:
                pass
        
        # Alternative format
        timestamp_match = re.search(r'(\w{3} \w{3} \d{2} \d{2}:\d{2}:\d{2}.\d{3})', line)
        if timestamp_match:
            try:
                return datetime.strptime(timestamp_match.group(1), '%a %b %d %H:%M:%S.%f')
            except:
                pass
                
        return datetime.now()
    
    def correlate_users_with_access(self):
        """Correlate usernames with database access records based on connection IDs"""
        # Create a mapping of connection_id to username from successful authentications
        conn_to_user = {}
        for auth in self.authentications:
            if auth['type'] == 'auth_success' and auth['username']:
                conn_to_user[auth['connection_id']] = auth['username']
        
        # Update database access records with usernames
        for access in self.database_access:
            if access['connection_id'] in conn_to_user:
                access['username'] = conn_to_user[access['connection_id']]
        
        # Also update slow queries with usernames
        for query in self.slow_queries:
            if query['connection_id'] in conn_to_user:
                query['username'] = conn_to_user.get(query['connection_id'], None)
    
    def get_connection_stats(self, start_date=None, end_date=None, ip_filter=None, user_filter=None):
        """Get connection statistics with optional filters"""
        if not self.connections:
            return {}
        
        # Apply filters to data
        filtered_connections = self.connections
        filtered_authentications = self.authentications
        filtered_database_access = self.database_access
        
        if start_date or end_date or ip_filter or user_filter:
            if start_date or end_date:
                filtered_connections = [conn for conn in self.connections
                                      if (not start_date or conn['timestamp'] >= start_date) and
                                         (not end_date or conn['timestamp'] <= end_date)]
                filtered_authentications = [auth for auth in self.authentications
                                          if (not start_date or auth['timestamp'] >= start_date) and
                                             (not end_date or auth['timestamp'] <= end_date)]
                filtered_database_access = [access for access in self.database_access
                                          if (not start_date or access['timestamp'] >= start_date) and
                                             (not end_date or access['timestamp'] <= end_date)]
            
            if ip_filter:
                filtered_connections = [conn for conn in filtered_connections
                                      if ip_filter.lower() in conn['ip'].lower()]
                
            if user_filter:
                filtered_authentications = [auth for auth in filtered_authentications
                                          if auth['username'] and user_filter.lower() in auth['username'].lower()]
                filtered_database_access = [access for access in filtered_database_access
                                          if access.get('username') and user_filter.lower() in access['username'].lower()]
        
        # Use filtered data for the rest of the method
        self.correlate_users_with_access()
            
        ip_counts = Counter([conn['ip'] for conn in filtered_connections if conn['type'] == 'connection_accepted'])
        
        # Group connections by IP
        connections_by_ip = defaultdict(list)
        for conn in filtered_connections:
            connections_by_ip[conn['ip']].append(conn)
        
        # Get authentication statistics
        auth_success = [auth for auth in filtered_authentications if auth['type'] == 'auth_success']
        auth_failures = [auth for auth in filtered_authentications if auth['type'] == 'auth_failure']
        
        # Get unique users and databases
        unique_users = set([auth['username'] for auth in filtered_authentications if auth['username']])
        unique_databases = set([db['database'] for db in filtered_database_access if db['database'] != 'unknown'])
        
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
            if auth['username'] and auth['database']:
                user_db_activity[auth['username']]['databases'].add(auth['database'])
                user_db_activity[auth['username']]['connections'].add(auth['connection_id'])
                user_db_activity[auth['username']]['auth_success'] = True
                user_db_activity[auth['username']]['auth_mechanism'] = auth.get('mechanism', 'Unknown')
                if not user_db_activity[auth['username']]['last_seen'] or auth['timestamp'] > user_db_activity[auth['username']]['last_seen']:
                    user_db_activity[auth['username']]['last_seen'] = auth['timestamp']
        
        # Count authentication failures per user
        for auth in auth_failures:
            if auth['username']:
                user_db_activity[auth['username']]['auth_failures'] += 1
                if not user_db_activity[auth['username']]['auth_mechanism']:
                    user_db_activity[auth['username']]['auth_mechanism'] = auth.get('mechanism', 'Unknown')
        
        for db_access in filtered_database_access:
            # Try to find username for this connection
            conn_auths = [auth for auth in auth_success if auth['connection_id'] == db_access['connection_id']]
            if conn_auths:
                username = conn_auths[0]['username']
                user_db_activity[username]['databases'].add(db_access['database'])
                user_db_activity[username]['connections'].add(db_access['connection_id'])
                if not user_db_activity[username]['last_seen'] or db_access['timestamp'] > user_db_activity[username]['last_seen']:
                    user_db_activity[username]['last_seen'] = db_access['timestamp']
        
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
        
        stats = {
            'total_connections': len([c for c in filtered_connections if c['type'] == 'connection_accepted']),
            'unique_ips': len(ip_counts),
            'connections_by_ip': dict(ip_counts),
            'slow_queries_count': len(self.slow_queries),
            'connections_timeline': sorted(filtered_connections, key=lambda x: x['timestamp']),
            'auth_success_count': len(auth_success),
            'auth_failure_count': len(auth_failures),
            'unique_users': list(unique_users),
            'unique_databases': list(unique_databases),
            'user_activity': user_activity,
            'database_access': filtered_database_access,
            'authentications': filtered_authentications
        }
        
        return stats
    
    def search_logs(self, keyword=None, field_name=None, field_value=None,
                    start_date=None, end_date=None, limit=100, conditions=None):
        """Search through log entries with optional field and advanced condition filters."""
        results = []
        total_found = 0

        normalized_start = normalize_datetime_for_comparison(start_date) if start_date else None
        normalized_end = normalize_datetime_for_comparison(end_date) if end_date else None

        eff_conditions = []
        if keyword:
            eff_conditions.append({'type': 'keyword', 'value': keyword, 'regex': False, 'case_sensitive': False, 'negate': False})
        if field_name and field_value:
            eff_conditions.append({'type': 'field', 'name': field_name, 'value': field_value, 'regex': False, 'case_sensitive': False, 'negate': False})
        if conditions:
            eff_conditions.extend(conditions)

        for file_path, raw_lines in (self.raw_log_data or {}).items():
            for line_num, line in enumerate(raw_lines, 1):
                line_stripped = line.rstrip('\n')
                parsed_json = None
                try:
                    parsed_json = json.loads(line_stripped)
                except Exception:
                    parsed_json = None

                timestamp = None
                if parsed_json:
                    timestamp = self.extract_timestamp_from_json(parsed_json.get('t', {}))
                if not timestamp:
                    timestamp = self._ml_try_parse_timestamp_from_line(line_stripped)

                if normalized_start or normalized_end:
                    if not timestamp:
                        continue
                    normalized_ts = normalize_datetime_for_comparison(timestamp)
                    if normalized_start and normalized_ts and normalized_ts < normalized_start:
                        continue
                    if normalized_end and normalized_ts and normalized_ts > normalized_end:
                        continue

                if eff_conditions:
                    if not self._ml_line_matches_conditions(line_stripped, parsed_json, eff_conditions):
                        continue

                total_found += 1
                if len(results) < limit:
                    results.append({
                        'file_path': file_path,
                        'line_number': line_num,
                        'timestamp': timestamp,
                        'raw_line': line_stripped,
                        'parsed_json': parsed_json or {}
                    })

        results.sort(key=lambda x: (x['timestamp'] or datetime.min, x['file_path'], x['line_number']), reverse=True)
        return {'results': results, 'total_found': total_found}
    
    def get_nested_field(self, obj, field_path):
        """Get nested field value using dot notation (e.g., 'attr.remote', 'command.find')"""
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
    
    def get_original_log_line(self, file_path, line_number):
        """Get the original raw log line using file path and line number"""
        if file_path in self.raw_log_data and line_number:
            raw_lines = self.raw_log_data[file_path]
            # line_number is 1-based, but array is 0-based
            if 1 <= line_number <= len(raw_lines):
                return raw_lines[line_number - 1]
        return None

    # Ephemeral and streaming search helpers for compatibility with routes
    def _ml_try_parse_timestamp_from_line(self, line):
        import re
        patterns = [
            r'"t":\{"\\$date":"([^"]+)"\}',
            r'"t":\{"\$date":"([^"]+)"\}',
            r'"t":\{"[$]date":"([^"]+)"\}'
        ]
        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                try:
                    return datetime.fromisoformat(match.group(1).replace('Z', '+00:00'))
                except Exception:
                    continue
        return None

    def _ml_get_nested_field(self, obj, field_path):
        if not field_path or not isinstance(obj, dict):
            return None
        cur = obj
        for part in str(field_path).split('.'):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    def _ml_line_matches_conditions(self, raw_line, parsed_json, conditions):
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
                    patt = None
            compiled.append((ctype, name, value, patt, is_regex, case_sensitive, negate))
        for (ctype, name, value, patt, is_regex, case_sensitive, negate) in compiled:
            matched = False
            if ctype == 'field':
                field_val = self._ml_get_nested_field(parsed_json or {}, name) if name else None
                field_str = '' if field_val is None else str(field_val)
                if is_regex and patt is not None:
                    matched = patt.search(field_str) is not None
                elif is_regex and patt is None:
                    matched = False
                else:
                    matched = value in field_str if case_sensitive else (value.lower() in field_str.lower())
            else:
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
        from datetime import datetime
        results = []
        total_found = 0
        start_dt = start_date
        end_dt = end_date
        eff_conditions = []
        if keyword:
            eff_conditions.append({'type': 'keyword', 'value': keyword, 'regex': False, 'case_sensitive': False, 'negate': False})
        if field_name and field_value:
            eff_conditions.append({'type': 'field', 'name': field_name, 'value': field_value, 'regex': False, 'case_sensitive': False, 'negate': False})
        if conditions:
            eff_conditions.extend(conditions)
        for file_path, lines in (raw_map or {}).items():
            for idx, line in enumerate(lines, 1):
                s = line.rstrip('\n')
                parsed = None
                ts = None
                try:
                    parsed = json.loads(s)
                    t = parsed.get('t', {})
                    ts_str = t.get('$date') if isinstance(t, dict) else None
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                except Exception:
                    parsed = None
                    ts = None
                if start_dt or end_dt:
                    if not ts:
                        continue
                    if start_dt and ts < start_dt:
                        continue
                    if end_dt and ts > end_dt:
                        continue
                if not self._ml_line_matches_conditions(s, parsed, eff_conditions):
                    continue
                total_found += 1
                if len(results) < limit:
                    results.append({'file_path': file_path, 'line_number': idx, 'timestamp': ts, 'raw_line': s})
        return {'results': results, 'total_found': total_found}

    def search_logs_streaming(self, keyword=None, field_name=None, field_value=None,
                               start_date=None, end_date=None, limit=100, compute_total=False, conditions=None):
        raw_map = getattr(self, 'raw_log_data', {}) or {}
        search_data = self.search_logs_ephemeral(raw_map, keyword, field_name, field_value, start_date, end_date, limit, conditions)
        if isinstance(search_data, dict):
            if compute_total:
                return search_data
            return {'results': search_data.get('results', []), 'total_found': len(search_data.get('results', []))}
        # Fallback compatibility if older analyzer returned list
        return {'results': search_data, 'total_found': len(search_data)}
    
    def get_available_date_range(self):
        """Get the date range from all parsed log entries"""
        if not self.slow_queries:
            return None, None
        
        # Get all timestamps from slow queries
        timestamps = [query['timestamp'] for query in self.slow_queries if query.get('timestamp')]
        
        # Also include timestamps from connections and other events
        for conn in self.connections:
            if conn.get('timestamp'):
                timestamps.append(conn['timestamp'])
        
        for auth in self.authentications:
            if auth.get('timestamp'):
                timestamps.append(auth['timestamp'])
        
        if not timestamps:
            return None, None
        
        min_date = min(timestamps)
        max_date = max(timestamps)
        
        return min_date, max_date
    
    def _group_queries_by_pattern(self, queries):
        """Group queries by pattern for unique queries view"""
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
            'slowest_execution_timestamp': None
        })
        
        for query in queries:
            # Create pattern key
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
                pattern['first_seen'] = query.get('timestamp')
                # Initialize with first query as slowest
                pattern['slowest_query_full'] = query.get('query', '')
                pattern['slowest_execution_timestamp'] = query.get('timestamp')
            
            # Add execution data
            pattern['executions'].append(query)
            pattern['durations'].append(query.get('duration', 0))
            pattern['total_count'] += 1
            pattern['last_seen'] = query.get('timestamp')
        
        # Convert to list and calculate aggregated stats
        unique_patterns = []
        for pattern_key, pattern in patterns.items():
            durations = pattern['durations']
            executions = pattern['executions']
            
            # Find the slowest execution
            max_duration = max(durations) if durations else 0
            slowest_executions = [exec for exec in executions if exec.get('duration', 0) == max_duration]
            
            # If multiple executions have same max duration, pick the most recent one
            if slowest_executions:
                slowest_execution = max(slowest_executions, 
                                      key=lambda x: x.get('timestamp') if x.get('timestamp') else datetime.min)
                pattern['slowest_query_full'] = slowest_execution.get('query', '')
                pattern['slowest_execution_timestamp'] = slowest_execution.get('timestamp')
            
            pattern.update({
                'avg_duration': sum(durations) / len(durations) if durations else 0,
                'min_duration': min(durations) if durations else 0,
                'max_duration': max_duration,
                'duration_range': f"{min(durations)}ms-{max(durations)}ms" if durations else "N/A"
            })
            unique_patterns.append(pattern)
        
        # Sort by average duration (descending)
        unique_patterns.sort(key=lambda x: x['avg_duration'], reverse=True)
        
        return unique_patterns
    
    def extract_timestamp_from_json(self, t_field):
        """Extract timestamp from MongoDB JSON t field"""
        if isinstance(t_field, dict) and '$date' in t_field:
            try:
                timestamp_str = t_field['$date']
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except:
                pass
        return None
    
    def get_parsing_summary_message(self):
        """Generate a human-readable parsing summary message"""
        summary = self.parsing_summary
        messages = []
        
        # Overall parsing stats
        messages.append(f"📊 **Overall Summary:** Processed {summary['files_processed']} file(s) with {summary['total_lines']:,} total lines")
        
        if summary['json_lines'] > 0:
            messages.append(f"📝 Found {summary['json_lines']:,} JSON format lines and {summary['text_lines']:,} text format lines")
        else:
            messages.append(f"📝 Found {summary['text_lines']:,} text format lines (no JSON format detected)")
        
        # Overall events found
        events_found = []
        if summary['connection_events'] > 0:
            events_found.append(f"{summary['connection_events']} connections")
        if summary['auth_events'] > 0:
            events_found.append(f"{summary['auth_events']} authentications") 
        if summary['command_events'] > 0:
            events_found.append(f"{summary['command_events']} database commands")
        if summary['slow_query_events'] > 0:
            events_found.append(f"{summary['slow_query_events']} slow queries")
            
        if events_found:
            messages.append(f"✅ **Total Events Extracted:** {', '.join(events_found)}")
        else:
            messages.append("⚠️ **No relevant events found!** This appears to be system/startup logs rather than operational logs with connections, authentications, or database commands.")
            messages.append("💡 **Tip:** Upload logs that contain connection events, authentication attempts, and database operations for analysis.")
        
        # Per-file breakdown (only if multiple files or if single file has no useful data)
        if len(summary['file_summaries']) > 1 or (len(summary['file_summaries']) == 1 and not summary['file_summaries'][0]['has_useful_data']):
            messages.append("")
            messages.append("📁 **Per-file breakdown:**")
            
            for file_info in summary['file_summaries']:
                filename = file_info['filename']
                lines = file_info['total_lines']
                
                # Build event list for this file
                file_events = []
                if file_info['connection_events'] > 0:
                    file_events.append(f"{file_info['connection_events']} connections")
                if file_info['auth_events'] > 0:
                    file_events.append(f"{file_info['auth_events']} auth")
                if file_info['command_events'] > 0:
                    file_events.append(f"{file_info['command_events']} commands")
                if file_info['slow_query_events'] > 0:
                    file_events.append(f"{file_info['slow_query_events']} slow queries")
                
                if file_events:
                    messages.append(f"✅ **{filename}:** {lines} lines → {', '.join(file_events)}")
                else:
                    messages.append(f"⚠️ **{filename}:** {lines} lines → 0 events (system/startup logs only)")
        
        # Errors and warnings
        if summary['error_lines'] > 0:
            messages.append(f"⚠️ {summary['error_lines']} lines had parsing errors")
            
        if summary['skipped_lines'] > 0:
            messages.append(f"ℹ️ Skipped {summary['skipped_lines']} empty lines")
            
        return messages
    
    def analyze_index_suggestions(self):
        """Analyze queries to suggest high-accuracy indexes (ranked, IXSCAN-aware)."""
        import re
        from collections import defaultdict

        def spec_from_suggestion(sug):
            # Expect 'index': '{a: 1, b: -1}', parse lightly
            try:
                inside = sug.get('index', '').strip().strip('{}').strip()
                parts = [p.strip() for p in inside.split(',') if p.strip()]
                fields = []
                for p in parts:
                    if ':' in p:
                        f, d = p.split(':', 1)
                        fields.append((f.strip(), int(d.strip())))
                return tuple(fields)
            except Exception:
                return tuple()

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

        per_spec = defaultdict(lambda: {
            'collection': '',
            'spec': tuple(),
            'occurrences': 0,
            'total_duration': 0,
            'docs_examined': 0,
            'returned': 0,
            'reason': '',
            'confidence': 'high'
        })

        # Weight by patterns (total executions across similar shapes)
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

            db_collection = f"{query.get('database', 'unknown')}.{query.get('collection', 'unknown')}"
            s = suggestions[db_collection]
            s['collection_name'] = db_collection
            if is_collscan:
                s['collscan_queries'] += 1
            if is_ixscan:
                s['ixscan_ineff_queries'] += 1

            # Prefer values already stored in DB; fall back to original-line parsing only if missing
            docs = 0
            try:
                docs = int(
                    (query.get('docsExamined') if query.get('docsExamined') is not None else
                     query.get('docs_examined') if query.get('docs_examined') is not None else 0)
                )
            except Exception:
                docs = 0
            if not docs:
                try:
                    docs = int(self._get_docs_examined(query) or 0)
                except Exception:
                    docs = 0

            try:
                ret = int(query.get('nReturned', query.get('docs_returned', 0)) or 0)
            except Exception:
                ret = 0
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

            # Derive candidate suggestions (high-accuracy only)
            query_str = query.get('query', '')
            parsed_suggestions = self._extract_index_suggestions(query_str, query.get('collection', 'unknown'))
            for sug in parsed_suggestions:
                spec = spec_from_suggestion(sug)
                if not spec:
                    continue
                key = (db_collection, spec)
                rec = per_spec[key]
                rec['collection'] = db_collection
                rec['spec'] = spec
                rec['occurrences'] += 1
                rec['total_duration'] += dur
                rec['docs_examined'] += docs
                rec['returned'] += ret
                rec['reason'] = 'COLLSCAN filter/sort coverage' if is_collscan else 'IXSCAN inefficiency improvement'

                # Apply patterns weighting
                try:
                    qh = query.get('query_hash') or ''
                    if not qh and hasattr(self, '_generate_synthetic_query_hash'):
                        qh = self._generate_synthetic_query_hash(query)
                    pk = f"{query.get('database','unknown')}.{query.get('collection','unknown')}_{qh}_{query.get('plan_summary','None')}"
                    pat = patterns.get(pk)
                    if pat:
                        rec['occurrences'] += max(0, int(pat.get('total_count', 0) or 0) - 1)
                        rec['total_duration'] += int((pat.get('avg_duration', 0) or 0) * (pat.get('total_count', 0) or 0))
                except Exception:
                    pass

        # Rank and deduplicate (coverage)
        def is_covered(shorter, longer):
            return len(shorter) <= len(longer) and shorter == longer[:len(shorter)]

        coll_to_recs = defaultdict(list)
        for (coll, spec), rec in per_spec.items():
            ineff = (rec['docs_examined'] / max(1, rec['returned'])) if rec['returned'] is not None else 1.0
            impact = rec['total_duration'] * max(1.0, ineff)
            rec['impact_score'] = int(impact)
            rec['inefficiency_ratio'] = round(ineff, 2)
            rec['avg_duration'] = int(rec['total_duration'] / max(1, rec['occurrences']))
            coll_to_recs[coll].append(rec)

        results = {}
        for coll, items in coll_to_recs.items():
            items.sort(key=lambda r: (-r['impact_score'], -len(r['spec'])))
            kept = []
            for r in items:
                if any(is_covered(r['spec'], k['spec']) for k in kept):
                    continue
                kept.append(r)

            final = []
            for r in kept[:10]:
                idx_fields = ', '.join([f"{f}: {d}" for f, d in r['spec']])
                final.append({
                    'type': 'compound',
                    'index': f'{{{idx_fields}}}',
                    'reason': r['reason'],
                    'priority': 'high',
                    'confidence': 'high',
                    'impact_score': r['impact_score'],
                    'occurrences': r['occurrences'],
                    'avg_duration_ms': r['avg_duration'],
                    'inefficiency_ratio': r['inefficiency_ratio'],
                    'command': f"db.{coll.split('.',1)[1]}.createIndex({{{idx_fields}}})"
                })

            s = suggestions[coll]
            total_q = s['collscan_queries'] + s['ixscan_ineff_queries']
            if total_q > 0:
                s['avg_duration'] = s['total_duration'] / total_q
                s['avg_docs_per_query'] = s['total_docs_examined'] / total_q
            s['suggestions'] = final
            results[coll] = s

        return dict(results)
    
    def _get_docs_examined(self, query):
        """Extract docsExamined from query data"""
        try:
            # Try to parse from the original log line if available
            original_line = self.get_original_log_line(query.get('file_path'), query.get('line_number'))
            if original_line:
                original_json = json.loads(original_line)
                return original_json.get('attr', {}).get('docsExamined', 0)
        except:
            pass
        return 0
    
    def _extract_index_suggestions(self, query_str, collection_name):
        """Extract index suggestions from query string for high-accuracy scenarios"""
        suggestions = []
        
        try:
            # Parse the query JSON
            if query_str.startswith('{') and query_str.endswith('}'):
                query_obj = json.loads(query_str)
            else:
                return suggestions
            
            # Handle different query types
            if 'find' in query_obj:
                suggestions.extend(self._analyze_find_query(query_obj, collection_name))
            elif 'aggregate' in query_obj:
                suggestions.extend(self._analyze_aggregate_query(query_obj, collection_name))
            
        except json.JSONDecodeError:
            pass
        except Exception:
            pass
            
        return suggestions
    
    def _analyze_find_query(self, query_obj, collection_name):
        """Analyze find queries for index suggestions"""
        suggestions = []
        
        # Extract filter conditions
        filter_obj = query_obj.get('filter', {})
        sort_obj = query_obj.get('sort', {})
        
        # Single field indexes for filter conditions
        for field, value in filter_obj.items():
            if field not in ['$and', '$or', '$nor']:  # Skip complex operators
                suggestions.append({
                    'type': 'single_field',
                    'index': f'{{{field}: 1}}',
                    'reason': f'Filter on {field}',
                    'priority': 'high',
                    'command': f'db.{collection_name}.createIndex({{{field}: 1}})'
                })
        
        # Sort indexes
        if sort_obj:
            sort_fields = []
            for field, direction in sort_obj.items():
                sort_fields.append(f'{field}: {direction}')
            
            if len(sort_fields) == 1:
                field, direction = list(sort_obj.items())[0]
                suggestions.append({
                    'type': 'sort',
                    'index': f'{{{field}: {direction}}}',
                    'reason': f'Sort by {field}',
                    'priority': 'high',
                    'command': f'db.{collection_name}.createIndex({{{field}: {direction}}})'
                })
            elif len(sort_fields) <= 3:  # Compound sort index
                index_spec = ', '.join(sort_fields)
                suggestions.append({
                    'type': 'compound_sort',
                    'index': f'{{{index_spec}}}',
                    'reason': f'Compound sort on {", ".join([f.split(":")[0] for f in sort_fields])}',
                    'priority': 'medium',
                    'command': f'db.{collection_name}.createIndex({{{index_spec}}})'
                })
        
        # Compound index for filter + sort (if both exist and simple)
        if filter_obj and sort_obj and len(filter_obj) == 1 and len(sort_obj) == 1:
            filter_field = list(filter_obj.keys())[0]
            sort_field, sort_dir = list(sort_obj.items())[0]
            if filter_field != sort_field:
                suggestions.append({
                    'type': 'compound_filter_sort',
                    'index': f'{{{filter_field}: 1, {sort_field}: {sort_dir}}}',
                    'reason': f'Filter on {filter_field} and sort by {sort_field}',
                    'priority': 'high',
                    'command': f'db.{collection_name}.createIndex({{{filter_field}: 1, {sort_field}: {sort_dir}}})'
                })
        
        return suggestions
    
    def _analyze_aggregate_query(self, query_obj, collection_name):
        """Analyze aggregate queries for basic index suggestions"""
        suggestions = []
        
        pipeline = query_obj.get('pipeline', [])
        
        for stage in pipeline:
            # Analyze $match stages
            if '$match' in stage:
                match_obj = stage['$match']
                # Only handle simple match conditions (not empty matches)
                if match_obj:  # Skip empty matches like {"$match": {}}
                    for field, value in match_obj.items():
                        if field not in ['$and', '$or', '$nor', '$expr']:  # Skip complex operators
                            suggestions.append({
                                'type': 'aggregate_match',
                                'index': f'{{{field}: 1}}',
                                'reason': f'$match stage filter on {field}',
                                'priority': 'high',
                                'command': f'db.{collection_name}.createIndex({{{field}: 1}})'
                            })
            
            # Analyze $sort stages
            elif '$sort' in stage:
                sort_obj = stage['$sort']
                if len(sort_obj) == 1:
                    field, direction = list(sort_obj.items())[0]
                    suggestions.append({
                        'type': 'aggregate_sort',
                        'index': f'{{{field}: {direction}}}',
                        'reason': f'$sort stage on {field}',
                        'priority': 'high',
                        'command': f'db.{collection_name}.createIndex({{{field}: {direction}}})'
                    })
        
        return suggestions
    
    def analyze_query_patterns(self):
        """Analyze slow query patterns for statistical analysis"""
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
            'avg_selectivity': None,
            'avg_index_efficiency': 0,
            'sample_query': '',
            'slowest_query_full': '',
            'slowest_execution_timestamp': None,
            'first_seen': None,
            'last_seen': None,
            'complexity_score': 0,
            'optimization_potential': 'low'
        })
        
        # Extract query hash and performance data from original log lines
        for query in self.slow_queries:
            query_hash = self._extract_query_hash(query)
            if not query_hash:
                # Use synthetic hash generation for consistent uniqueness
                query_hash = self._generate_synthetic_query_hash(query)
                
            pattern_key = f"{query.get('database', 'unknown')}.{query.get('collection', 'unknown')}_{query_hash}_{query.get('plan_summary', 'None')}"
            pattern = patterns[pattern_key]
            
            # Get detailed metrics from original log line
            metrics = self._extract_detailed_metrics(query)
            
            # Check if metrics are estimated (when docsExamined/nReturned weren't in original log)
            is_estimated = (
                not query.get('docsExamined', 0) and 
                not query.get('nReturned', 0) and 
                metrics.get('docsExamined', 0) > 0
            )
            
            # Initialize pattern metadata if first occurrence
            if pattern['total_count'] == 0:
                pattern['query_hash'] = query_hash
                pattern['plan_cache_key'] = metrics.get('planCacheKey', query_hash)
                pattern['collection'] = query.get('collection', 'unknown')
                pattern['database'] = query.get('database', 'unknown')
                pattern['query_type'] = self._determine_query_type(query.get('query', ''))
                pattern['plan_summary'] = query.get('plan_summary', 'None')
                pattern['sample_query'] = query.get('query', '')[:200] + ('...' if len(query.get('query', '')) > 200 else '')
                pattern['slowest_query_full'] = query.get('query', '')
                pattern['slowest_execution_timestamp'] = query.get('timestamp')
                pattern['first_seen'] = query.get('timestamp')
                pattern['complexity_score'] = self._calculate_complexity_score(query.get('query', ''))
                pattern['is_estimated'] = is_estimated
            
            # Add execution data
            execution = {
                'duration': query.get('duration', 0),
                'docs_examined': metrics.get('docsExamined', 0),
                'keys_examined': metrics.get('keysExamined', 0),
                'returned': metrics.get('nReturned', 0),
                'cpu_nanos': metrics.get('cpuNanos', 0),
                'bytes_read': metrics.get('bytesRead', 0),
                'bytes_written': metrics.get('bytesWritten', 0),
                'memory_mb': metrics.get('memoryMb', 0),
                'timestamp': query.get('timestamp')
            }
            
            pattern['executions'].append(execution)
            pattern['total_count'] += 1
            pattern['last_seen'] = query.get('timestamp')
            
            # Aggregate additional metrics when available
            pattern['total_bytes_read'] = pattern.get('total_bytes_read', 0) + (metrics.get('bytesRead', 0) or 0)
            pattern['total_bytes_written'] = pattern.get('total_bytes_written', 0) + (metrics.get('bytesWritten', 0) or 0)
            pattern['total_memory_mb'] = pattern.get('total_memory_mb', 0) + (metrics.get('memoryMb', 0) or 0)

            # Update slowest query if this execution is slower, or if same duration but more recent
            current_duration = query.get('duration', 0)
            current_max_duration = max([execution['duration'] for execution in pattern['executions']])
            
            if (pattern['slowest_query_full'] == '' or 
                current_duration >= current_max_duration):
                # If tied on duration, prefer the most recent execution
                if (current_duration == current_max_duration and 
                    query.get('timestamp') and pattern.get('slowest_execution_timestamp') and
                    query.get('timestamp') <= pattern.get('slowest_execution_timestamp')):
                    pass  # Keep the current slowest (more recent)
                else:
                    pattern['slowest_query_full'] = query.get('query', '')
                    pattern['slowest_execution_timestamp'] = query.get('timestamp')
        
        # Calculate statistics for each pattern
        for pattern_key, pattern in patterns.items():
            if pattern['total_count'] == 0:
                continue
                
            durations = [execution['duration'] for execution in pattern['executions']]
            docs_examined = [execution['docs_examined'] for execution in pattern['executions']]
            keys_examined = [execution['keys_examined'] for execution in pattern['executions']]
            returned = [execution['returned'] for execution in pattern['executions']]
            
            # Duration statistics
            pattern['avg_duration'] = statistics.mean(durations)
            pattern['min_duration'] = min(durations)
            pattern['max_duration'] = max(durations)
            pattern['median_duration'] = statistics.median(durations)
            
            # Efficiency metrics
            pattern['total_docs_examined'] = sum(docs_examined)
            pattern['total_keys_examined'] = sum(keys_examined) 
            pattern['total_returned'] = sum(returned)
            pattern['avg_docs_examined'] = pattern['total_docs_examined'] / pattern['total_count'] if pattern['total_count'] else 0
            pattern['avg_docs_returned'] = pattern['total_returned'] / pattern['total_count'] if pattern['total_count'] else 0
            pattern['avg_keys_examined'] = pattern['total_keys_examined'] / pattern['total_count'] if pattern['total_count'] else 0
            
            # Calculate selectivity (docs returned / docs examined)
            if pattern['total_docs_examined'] > 0:
                pattern['avg_selectivity'] = (pattern['total_returned'] / pattern['total_docs_examined']) * 100
            else:
                # Debug: if no docs examined, keep selectivity undefined instead of 0
                pattern['avg_selectivity'] = None
            
            # Calculate index efficiency (keys examined / docs examined)  
            if pattern['total_docs_examined'] > 0:
                pattern['avg_index_efficiency'] = (pattern['total_keys_examined'] / pattern['total_docs_examined']) * 100
            
            # Determine optimization potential
            pattern['optimization_potential'] = self._assess_optimization_potential(pattern)
        
        # Sort patterns by total execution time (impact)
        sorted_patterns = dict(sorted(patterns.items(), 
                                    key=lambda x: x[1]['avg_duration'] * x[1]['total_count'], 
                                    reverse=True))
        
        return sorted_patterns
    
    def _extract_query_hash(self, query):
        """Extract queryHash from original log line"""
        try:
            original_line = self.get_original_log_line(query.get('file_path'), query.get('line_number'))
            if original_line:
                original_json = json.loads(original_line)
                query_hash = original_json.get('attr', {}).get('queryHash')
                if query_hash:
                    return query_hash
        except Exception:
            pass
        return None  # Return None to trigger fallback hash generation
    
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
                        for key in ['find', 'aggregate', 'update', 'delete', 'insert', 'command']:
                            if key in query_obj:
                                normalized_parts.append(f"op:{key}")
                                break
                        
                        # Extract filter/match conditions (without values)
                        if 'filter' in query_obj:
                            filter_keys = self._extract_query_structure(query_obj['filter'])
                            if filter_keys:
                                normalized_parts.append(f"filter:{','.join(sorted(filter_keys))}")
                        
                        # Extract pipeline structure for aggregation
                        if 'pipeline' in query_obj and isinstance(query_obj['pipeline'], list):
                            pipeline_ops = []
                            sort_parts = []
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
                            if pipeline_ops:
                                normalized_parts.append(f"pipeline:{','.join(pipeline_ops)}")
                            if sort_parts:
                                normalized_parts.append(f"pipeline_sort:{','.join(sort_parts)}")
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
    
    def _extract_detailed_metrics(self, query):
        """Extract detailed metrics from original log line or parsed data"""
        # First check if metrics are already available in the query object
        if ('docsExamined' in query or 'docs_examined' in query or 
            'nReturned' in query or 'n_returned' in query or 'docs_returned' in query or
            'keysExamined' in query or 'keys_examined' in query):
            return {
                'docsExamined': (
                    query.get('docsExamined') if query.get('docsExamined') is not None else
                    query.get('docs_examined') if query.get('docs_examined') is not None else 0
                ),
                'keysExamined': (
                    query.get('keysExamined') if query.get('keysExamined') is not None else
                    query.get('keys_examined') if query.get('keys_examined') is not None else 0
                ), 
                'nReturned': (
                    query.get('nReturned') if query.get('nReturned') is not None else
                    query.get('n_returned') if query.get('n_returned') is not None else
                    query.get('docs_returned') if query.get('docs_returned') is not None else 0
                ),
                'cpuNanos': query.get('cpuNanos', 0),
                'planCacheKey': query.get('planCacheKey', ''),
                'queryFramework': query.get('queryFramework', ''),
                'readConcern': query.get('readConcern', {}),
                'writeConcern': query.get('writeConcern', {}),
                'bytesRead': query.get('bytesRead', 0),
                'bytesWritten': query.get('bytesWritten', 0),
                'memoryMb': query.get('memoryMb', 0)
            }
        
        # Try to extract from original log line
        try:
            original_line = self.get_original_log_line(query.get('file_path'), query.get('line_number'))
            if original_line:
                original_json = json.loads(original_line)
                attr = original_json.get('attr', {})
                
                # Extract metrics with multiple possible field names
                docs_examined = (
                    attr.get('docsExamined') if attr.get('docsExamined') is not None else
                    attr.get('docs_examined') if attr.get('docs_examined') is not None else
                    attr.get('totalDocsExamined') if attr.get('totalDocsExamined') is not None else
                    attr.get('executionStats', {}).get('docsExamined') if attr.get('executionStats', {}).get('docsExamined') is not None else
                    attr.get('command', {}).get('docsExamined') if attr.get('command', {}).get('docsExamined') is not None else 0
                )
                
                keys_examined = (
                    attr.get('keysExamined') if attr.get('keysExamined') is not None else
                    attr.get('keys_examined') if attr.get('keys_examined') is not None else
                    attr.get('totalKeysExamined') if attr.get('totalKeysExamined') is not None else
                    attr.get('executionStats', {}).get('keysExamined') if attr.get('executionStats', {}).get('keysExamined') is not None else
                    attr.get('command', {}).get('keysExamined') if attr.get('command', {}).get('keysExamined') is not None else 0
                )
                
                n_returned = (
                    attr.get('nReturned') if attr.get('nReturned') is not None else
                    attr.get('n_returned') if attr.get('n_returned') is not None else
                    attr.get('nreturned') if attr.get('nreturned') is not None else
                    attr.get('numReturned') if attr.get('numReturned') is not None else
                    attr.get('executionStats', {}).get('nReturned') if attr.get('executionStats', {}).get('nReturned') is not None else
                    attr.get('command', {}).get('nReturned') if attr.get('command', {}).get('nReturned') is not None else 0
                )
                
                # Disk/IO and memory (best-effort)
                bytes_read = (
                    attr.get('bytesRead') if attr.get('bytesRead') is not None else
                    (attr.get('network', {}).get('bytesRead') if isinstance(attr.get('network', {}), dict) else 0)
                )
                bytes_written = (
                    attr.get('bytesWritten') if attr.get('bytesWritten') is not None else
                    (attr.get('network', {}).get('bytesWritten') if isinstance(attr.get('network', {}), dict) else 0)
                )
                mem_mb = None
                mem = attr.get('memory', {}) if isinstance(attr.get('memory', {}), dict) else {}
                if mem:
                    mem_mb = mem.get('residentMb') or mem.get('residentMB') or mem.get('workingSetMb')
                if mem_mb is None:
                    ws = attr.get('workingSet') or attr.get('resident')
                    if isinstance(ws, (int, float)):
                        mem_mb = round(float(ws) / (1024*1024), 2)
                if mem_mb is None:
                    mem_mb = 0

                return {
                    'docsExamined': docs_examined,
                    'keysExamined': keys_examined,
                    'nReturned': n_returned,
                    'cpuNanos': attr.get('cpuNanos', 0),
                    'planCacheKey': attr.get('planCacheKey', ''),
                    'queryFramework': attr.get('queryFramework', ''),
                    'readConcern': attr.get('readConcern', {}),
                    'writeConcern': attr.get('writeConcern', {}),
                    'bytesRead': bytes_read or 0,
                    'bytesWritten': bytes_written or 0,
                    'memoryMb': mem_mb or 0
                }
        except Exception as e:
            # For debugging: log what went wrong
            pass
        
        # Try to estimate metrics from query structure for legacy logs
        return self._estimate_metrics_from_query(query)
    
    def _estimate_metrics_from_query(self, query):
        """Estimate basic metrics for queries without detailed performance data"""
        # For COLLSCAN queries, we can estimate that many docs were examined
        plan_summary = query.get('plan_summary', 'None')
        duration = query.get('duration', 0)
        
        if plan_summary == 'COLLSCAN':
            # Estimate based on duration - longer queries likely examined more docs
            if duration > 5000:  # >5 seconds
                estimated_docs = 50000
            elif duration > 1000:  # >1 second  
                estimated_docs = 10000
            elif duration > 500:  # >500ms
                estimated_docs = 5000
            else:
                estimated_docs = 1000
            
            # Assume low selectivity for COLLSCAN
            estimated_returned = max(1, int(estimated_docs * 0.1))  # 10% selectivity
            
            return {
                'docsExamined': estimated_docs,
                'keysExamined': 0,  # COLLSCAN doesn't use keys
                'nReturned': estimated_returned,
                'cpuNanos': 0,
                'planCacheKey': '',
                'queryFramework': '',
                'readConcern': {},
                'writeConcern': {}
            }
            
        elif 'IXSCAN' in plan_summary:
            # For index scans, assume better selectivity
            if duration > 1000:
                estimated_keys = 2000
                estimated_docs = 1000
            elif duration > 200:
                estimated_keys = 500
                estimated_docs = 300
            else:
                estimated_keys = 100
                estimated_docs = 50
            
            estimated_returned = max(1, int(estimated_docs * 0.5))  # 50% selectivity
            
            return {
                'docsExamined': estimated_docs,
                'keysExamined': estimated_keys,
                'nReturned': estimated_returned,
                'cpuNanos': 0,
                'planCacheKey': '',
                'queryFramework': '',
                'readConcern': {},
                'writeConcern': {}
            }
        
        # Default fallback
        return {
            'docsExamined': 0,
            'keysExamined': 0,
            'nReturned': 0,
            'cpuNanos': 0,
            'planCacheKey': '',
            'queryFramework': '',
            'readConcern': {},
            'writeConcern': {}
        }
    
    def _determine_query_type(self, query_str):
        """Determine query type from query string"""
        try:
            if query_str:
                query_obj = json.loads(query_str)
                if 'find' in query_obj:
                    return 'find'
                elif 'aggregate' in query_obj:
                    return 'aggregate'
                elif 'update' in query_obj:
                    return 'update'
                elif 'delete' in query_obj:
                    return 'delete'
                elif 'count' in query_obj:
                    return 'count'
        except:
            pass
        return 'unknown'
    
    def _calculate_complexity_score(self, query_str):
        """Calculate query complexity score (1-10 scale)"""
        score = 1
        try:
            if query_str:
                query_obj = json.loads(query_str)
                
                # Aggregate pipeline complexity
                if 'aggregate' in query_obj:
                    pipeline = query_obj.get('pipeline', [])
                    score += len(pipeline)  # +1 per stage
                    
                    for stage in pipeline:
                        if '$lookup' in stage:
                            score += 2  # Joins are expensive
                        if '$group' in stage:
                            score += 1
                        if '$sort' in stage:
                            score += 1
                        if '$match' in stage:
                            match_obj = stage['$match']
                            if '$or' in match_obj or '$and' in match_obj:
                                score += 1  # Complex conditions
                
                # Find query complexity
                elif 'find' in query_obj:
                    if 'filter' in query_obj:
                        filter_obj = query_obj['filter']
                        score += len(filter_obj)  # +1 per filter condition
                    if 'sort' in query_obj:
                        score += 1
                    
        except:
            pass
        
        return min(score, 10)  # Cap at 10
    
    def _assess_optimization_potential(self, pattern):
        """Assess optimization potential based on pattern characteristics"""
        score = 0
        
        # High duration variance suggests inconsistent performance (using min/max range instead)
        duration_range = pattern.get('max_duration', 0) - pattern.get('min_duration', 0)
        if duration_range > pattern['avg_duration'] * 0.5:
            score += 2
        
        # COLLSCAN is always high optimization potential
        if pattern['plan_summary'] == 'COLLSCAN':
            score += 3
        
        # High docs examined vs returned ratio
        if pattern['avg_selectivity'] is not None and pattern['avg_selectivity'] < 10:  # Less than 10% selectivity
            score += 2
        
        # Frequent execution
        if pattern['total_count'] >= 10:
            score += 1
        
        # High average duration
        if pattern['avg_duration'] > 1000:  # > 1 second
            score += 2
        
        # High complexity
        if pattern['complexity_score'] >= 7:
            score += 1
        
        if score >= 6:
            return 'high'
        elif score >= 3:
            return 'medium'
        else:
            return 'low'

# Global analyzer instance - use optimized version if available
if OPTIMIZED_ANALYZER_AVAILABLE:
    analyzer = OptimizedMongoLogAnalyzer({
        'use_database': True,        # Enable database storage
        'chunk_size': 50000,
        'max_workers': 4,
        'store_raw_lines': False,     # Reduce memory usage - don't store raw lines
        'database_path': 'mongodb_analysis.db'  # Persistent database file
    })
    print("🚀 Using optimized analyzer for better performance!")
else:
    analyzer = MongoLogAnalyzer()
    print("⚠️  Using original analyzer (slower)")

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'files' not in request.files:
        flash('No files selected. Please choose files to upload.', 'error')
        return redirect(url_for('index'))
    
    files = request.files.getlist('files')
    if not files or files[0].filename == '':
        flash('No files selected. Please choose files to upload.', 'error')
        return redirect(url_for('index'))
    
    # Clean up temp folder before processing new uploads
    cleanup_temp_folder()
    
    temp_files_created = []
    extracted_files = []
    extraction_dir = os.path.join(app.config['TEMP_FOLDER'], f'extracted_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    
    archive_counter = 0

    try:
        for file in files:
            if file and allowed_file(file.filename):
                original_filename = secure_filename(file.filename)
                temp_filepath = create_temp_file(original_filename)
                
                # Save to temp location
                file.save(temp_filepath)
                temp_files_created.append(temp_filepath)
                
                # Check if it's an archive file
                if is_archive_file(original_filename):
                    archive_counter += 1
                    archive_root = original_filename
                    for ext in ['.tar.gz', '.tgz', '.tar.bz2', '.tar', '.gz', '.zip']:
                        if archive_root.lower().endswith(ext):
                            archive_root = archive_root[:-len(ext)]
                            break
                    archive_root = secure_filename(archive_root) or f"archive_{archive_counter}"
                    target_dir = os.path.join(extraction_dir, f"{archive_root}_{archive_counter:02d}")
                    # Extract archive files to temp directory
                    extracted = extract_archive(temp_filepath, target_dir)
                    if extracted:
                        extracted_files.extend(extracted)
                        flash(f'Extracted {len(extracted)} log file(s) from {original_filename}')
                    else:
                        flash(f'No valid log files found in archive {original_filename}', 'warning')
                elif is_log_file(original_filename):
                    # Regular log file - keep temp path for processing
                    pass
                else:
                    flash(f'Invalid filename: {file.filename}. Please upload a different file and try again.', 'error')
                    return redirect(url_for('index'))
            else:
                flash(f'Invalid filename: {file.filename}. Please upload a different file and try again.', 'error')
                return redirect(url_for('index'))
        
        # Combine temp files and extracted files for analysis
        log_files_to_analyze = []
        for temp_file in temp_files_created:
            if is_log_file(temp_file):
                log_files_to_analyze.append(temp_file)
        log_files_to_analyze.extend(extracted_files)
        
        if log_files_to_analyze:
            # Clear previous analysis (create new analyzer instance)
            global analyzer
            # Use optimized analyzer for upload processing
            if OPTIMIZED_ANALYZER_AVAILABLE:
                analyzer = OptimizedMongoLogAnalyzer({
                    'use_database': True,        # Enable database storage
                    'chunk_size': 50000,
                    'max_workers': 4,
                    'store_raw_lines': False,     # Reduce memory usage
                    'database_path': 'mongodb_analysis.db'  # Persistent database file
                })
                # Clear existing database data for new upload
                analyzer.clear_database_data()
                
                # PHASE 3: Enable verbose logging for multi-file uploads to show progress
                analyzer.config['verbose_logging'] = len(log_files_to_analyze) > 1
                
                print("🚀 Upload: Using optimized analyzer with database cleanup!")
                if len(log_files_to_analyze) > 1:
                    print(f"📁 PHASE 3: Multi-file upload detected ({len(log_files_to_analyze)} files) - using single bulk session")
            else:
                analyzer = MongoLogAnalyzer()
                print("⚠️  Upload: Using original analyzer (slower)")
            
            # Record source files for streaming grep-like search (low-memory search)
            try:
                analyzer.source_log_files = [os.path.abspath(p) for p in log_files_to_analyze]
                analyzer.source_total_bytes = sum(os.path.getsize(p) for p in analyzer.source_log_files if os.path.exists(p))
            except Exception:
                analyzer.source_log_files = [p for p in log_files_to_analyze if os.path.exists(p)]
                analyzer.source_total_bytes = 0

            # PHASE 3: Analyze all files with single bulk session for multi-file uploads
            processed_count = 0
            is_multi_file = len(log_files_to_analyze) > 1
            
            if OPTIMIZED_ANALYZER_AVAILABLE and is_multi_file:
                # Start single bulk session for all files
                print(f"🚀 PHASE 3: Starting single bulk session for {len(log_files_to_analyze)} files")
                total_size_mb = sum(os.path.getsize(f) / (1024 * 1024) for f in log_files_to_analyze if os.path.exists(f))
                analyzer.start_bulk_load(total_size_mb)
                
                try:
                    for filepath in log_files_to_analyze:
                        if os.path.exists(filepath) and is_log_file(filepath):
                            print(f"📄 Processing {os.path.basename(filepath)} (skipping individual bulk session)...")
                            analyzer.parse_log_file_optimized(filepath, skip_bulk_session=True)
                            processed_count += 1
                finally:
                    # Complete the single bulk session
                    print("🎯 PHASE 3: Completing single bulk session for all files")
                    analyzer.finish_bulk_load()
            else:
                # Single file or fallback - use individual sessions
                for filepath in log_files_to_analyze:
                    if os.path.exists(filepath) and is_log_file(filepath):
                        if OPTIMIZED_ANALYZER_AVAILABLE:
                            print(f"🚀 Processing {os.path.basename(filepath)} with optimized analyzer...")
                            analyzer.parse_log_file_optimized(filepath)
                        else:
                            analyzer.parse_log_file(filepath)
                        processed_count += 1
            
            # Generate parsing summary messages
            summary_messages = analyzer.get_parsing_summary_message()
            for message in summary_messages:
                flash(message)
            
            return redirect(url_for('dashboard'))
        else:
            flash('No valid log files to process. Please select different files and try again.', 'error')
            return redirect(url_for('index'))
            
    finally:
        # Cleanup happens automatically on next upload via cleanup_temp_folder()
        # But we could also clean up immediately if needed
        pass

@app.route('/dashboard')
def dashboard():
    # PHASE 1 OPTIMIZATION: Use efficient SQL method instead of Python iteration
    min_date, max_date = analyzer.get_available_date_range_sql()
    
    # Get filter parameters
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    ip_filter = request.args.get('ip_filter', '').strip()
    user_filter = request.args.get('user_filter', '').strip()
    
    # Parse date filters with improved handling
    start_date = parse_date_filter(start_date_str, is_end_date=False)
    end_date = parse_date_filter(end_date_str, is_end_date=True)
    
    # Get enhanced dashboard summary stats
    stats = analyzer.get_dashboard_summary_stats(
        start_date=start_date,
        end_date=end_date,
        ip_filter=ip_filter,
        user_filter=user_filter
    )
    
    return render_template('dashboard_results.html', 
                         stats=stats,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         min_date=format_datetime_for_input(min_date),
                         max_date=format_datetime_for_input(max_date),
                         ip_filter=ip_filter,
                         user_filter=user_filter)

@app.route('/api/stats')
def api_stats():
    """API endpoint that works with both database and memory modes"""
    try:
        if hasattr(analyzer, 'config') and analyzer.config.get('use_database', False):
            # Database mode - get stats from database
            stats = analyzer.get_connection_stats()
            # Also add slow queries count from database
            if hasattr(analyzer, 'db_conn'):
                import sqlite3
                cursor = analyzer.db_conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM slow_queries')
                slow_queries_count = cursor.fetchone()[0]
                cursor.execute('SELECT COUNT(*) FROM connections')
                connections_count = cursor.fetchone()[0]
                cursor.execute('SELECT COUNT(*) FROM authentications')
                auth_count = cursor.fetchone()[0]
                
                # Update stats with actual database counts
                stats['slow_queries_count'] = slow_queries_count
                stats['total_connections'] = connections_count
                stats['auth_success_count'] = auth_count
                
                # Get unique databases from database
                cursor.execute('SELECT DISTINCT database FROM slow_queries WHERE database != "" AND database IS NOT NULL')
                unique_dbs = [row[0] for row in cursor.fetchall()]
                stats['unique_databases'] = unique_dbs
                
        else:
            # Memory mode - use original method
            stats = analyzer.get_connection_stats()
        
        return jsonify(stats)
    except Exception as e:
        # Return empty stats on error
        return jsonify({
            'auth_failure_count': 0,
            'auth_success_count': 0,
            'authentications': [],
            'connections_by_ip': {},
            'connections_timeline': [],
            'database_access': [],
            'slow_queries_count': 0,
            'total_connections': 0,
            'unique_databases': [],
            'unique_ips': 0,
            'unique_users': [],
            'user_activity': {},
            'error': str(e)
        })

@app.route('/slow-queries')
def slow_queries():
    # Ensure user correlation is done before filtering
    analyzer.correlate_users_with_access()
    
    # PHASE 1 OPTIMIZATION: Use efficient SQL methods instead of Python iteration
    
    # Get unique databases for filtering using SQL
    databases = set(analyzer.get_distinct_databases_sql())
    
    # Get available date range using SQL
    min_date, max_date = analyzer.get_available_date_range_sql()
    
    # Note: Query hash generation handled per-query basis when needed for performance
    
    # Get filter parameters
    selected_db = request.args.get('database', 'all')
    threshold = int(request.args.get('threshold', 100))
    selected_plan = request.args.get('plan_summary', 'all')
    view_mode = request.args.get('view_mode', 'all_executions')  # 'all_executions' or 'unique_queries'
    grouping_type = request.args.get('grouping', 'pattern_key')  # 'pattern_key', 'namespace', or 'query_hash'
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    
    # Parse date filters with improved handling
    start_date = parse_date_filter(start_date_str, is_end_date=False)
    end_date = parse_date_filter(end_date_str, is_end_date=True)
    
    # Get pagination parameters
    page, per_page = get_pagination_params(request)
    
    # Use SQL-based filtering for optimal performance
    query_result = analyzer.get_slow_queries_filtered(
        threshold=threshold,
        database=selected_db,
        plan_summary=selected_plan,
        start_date=start_date.isoformat() if start_date else None,
        end_date=end_date.isoformat() if end_date else None,
        page=page,
        per_page=per_page if view_mode != 'unique_queries' else None
    )
    
    # Extract data from the returned dictionary
    filtered_queries = query_result.get('items', [])
    total_count = query_result.get('total', 0)
    
    # Process data based on view mode
    if view_mode == 'unique_queries':
        # Use pre-aggregated patterns with selected grouping strategy
        unique_queries = analyzer.get_aggregated_patterns_by_grouping(
            grouping=grouping_type,
            threshold=threshold,
            database=selected_db,
            plan_summary=selected_plan,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
            limit=per_page * 10  # Get more for grouping
        )
        display_data = unique_queries
        total_count = len(unique_queries)
        # Apply pagination for unique queries
        pagination = paginate_data(display_data, page, per_page)
    else:
        # All executions mode - data already paginated from SQL
        display_data = filtered_queries
        # Create pagination object with SQL results
        pagination = {
            'items': filtered_queries,
            'page': page,
            'per_page': per_page,
            'total': total_count,
            'pages': (total_count + per_page - 1) // per_page,
            'prev_num': page - 1 if page > 1 else None,
            'next_num': page + 1 if page * per_page < total_count else None,
            'has_prev': page > 1,
            'has_next': page * per_page < total_count
        }
    
    return render_template('slow_queries.html', 
                         queries=pagination['items'], 
                         pagination=pagination,
                         databases=sorted(databases),
                         selected_db=selected_db,
                         threshold=threshold,
                         selected_plan=selected_plan,
                         view_mode=view_mode,
                         grouping_type=grouping_type,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         min_date=format_datetime_for_input(min_date),
                         max_date=format_datetime_for_input(max_date),
                         total_queries=total_count)

@app.route('/export-slow-queries')
def export_slow_queries():
    # Ensure user correlation is done before filtering
    analyzer.correlate_users_with_access()
    
    # PHASE 1 OPTIMIZATION: Get available date range using SQL for validation
    min_date, max_date = analyzer.get_available_date_range_sql()
    
    # Get filter parameters
    selected_db = request.args.get('database', 'all')
    threshold = int(request.args.get('threshold', 100))
    selected_plan = request.args.get('plan_summary', 'all')
    view_mode = request.args.get('view_mode', 'all_executions')
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    
    # Export limit for performance (default 10k, max 50k)
    export_limit = min(int(request.args.get('limit', 10000)), 50000)
    
    # Parse date filters with improved handling
    start_date = parse_date_filter(start_date_str, is_end_date=False)
    end_date = parse_date_filter(end_date_str, is_end_date=True)
    
    # SQL-side filtering for accurate totals and efficient fetch
    filtered_result = analyzer.get_slow_queries_filtered(
        threshold=threshold,
        database=selected_db,
        plan_summary=selected_plan,
        start_date=start_date.isoformat() if start_date else None,
        end_date=end_date.isoformat() if end_date else None,
        page=1,
        per_page=export_limit
    )
    filtered_queries = filtered_result.get('items', [])
    total_matched_pre_limit = filtered_result.get('total', len(filtered_queries))
    
    # Handle export based on view mode
    if view_mode == 'unique_queries':
        # Export unique patterns using same method as display for consistency
        unique_patterns = analyzer.get_aggregated_patterns(
            threshold=threshold,
            database=selected_db,
            plan_summary=selected_plan,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
            limit=export_limit
        )
        
        export_data = {
            'export_info': {
                'mode': 'unique_patterns',
                'total_patterns': len(unique_patterns),
                'total_executions': sum(p.get('execution_count', 0) for p in unique_patterns),
                'exported_at': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                'filters_applied': {
                    'database': selected_db,
                    'threshold_ms': threshold,
                    'plan_summary': selected_plan,
                    'start_date': start_date_str,
                    'end_date': end_date_str
                }
            },
            'patterns': []
        }
        
        for pattern in unique_patterns:
            # Use the aggregated pattern data directly (already has slowest query text)
            pattern_export = {
                'pattern_summary': {
                    'query_hash': pattern.get('query_hash', ''),
                    'database': pattern.get('database', ''),
                    'collection': pattern.get('collection', ''),
                    'namespace': pattern.get('namespace', ''),
                    'execution_count': pattern.get('execution_count', 0),
                    'avg_duration_ms': round(pattern.get('avg_duration', 0), 2),
                    'min_duration_ms': pattern.get('min_duration', 0),
                    'max_duration_ms': pattern.get('max_duration', 0),
                    'total_duration_ms': pattern.get('total_duration', 0),
                    'avg_docs_examined': round(pattern.get('avg_docs_examined', 0), 2),
                    'avg_docs_returned': round(pattern.get('avg_docs_returned', 0), 2),
                    'avg_keys_examined': round(pattern.get('avg_keys_examined', 0), 2),
                    'plan_summary': pattern.get('plan_summary', 'None'),
                    'first_seen': pattern.get('first_seen').isoformat() if pattern.get('first_seen') else None,
                    'last_seen': pattern.get('last_seen').isoformat() if pattern.get('last_seen') else None
                },
                'slowest_query_sample': {
                    'query_text': _clean_query_text_for_export(pattern.get('sample_query', 'N/A'))
                }
            }
            export_data['patterns'].append(pattern_export)
        
        export_content = json.dumps(export_data, indent=2, default=str)
        filename_suffix = 'unique_patterns'
        
    else:
        # Export all executions with optimized batch file I/O
        original_log_lines = _batch_get_original_log_lines(filtered_queries)
        original_log_entries = []
        
        for i, query in enumerate(filtered_queries):
            original_line = original_log_lines[i]
            if original_line:
                try:
                    original_log_entries.append(json.loads(original_line))
                except json.JSONDecodeError:
                    original_log_entries.append({
                        'timestamp': query['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                        'connection_id': query.get('connection_id', None),
                        'duration_ms': query.get('duration', 0),
                        'database': query.get('database', ''),
                        'collection': query.get('collection', ''),
                        'query': query.get('query', {}),
                        'plan_summary': query.get('plan_summary', 'None'),
                        'username': query.get('username', 'Unknown'),
                        'note': 'Processed data - original log entry was malformed'
                    })
            else:
                original_log_entries.append({
                    'timestamp': query['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                    'connection_id': query.get('connection_id', None),
                    'duration_ms': query.get('duration', 0),
                    'database': query.get('database', ''),
                    'collection': query.get('collection', ''),
                    'query': query['query'],
                    'plan_summary': query.get('plan_summary', 'None'),
                    'username': query.get('username', 'Unknown'),
                    'note': 'Processed data - original log entry not found'
                })
        
        export_content = json.dumps(original_log_entries, indent=2, default=str)
        filename_suffix = 'all_executions'
    
    # Generate filename
    db_suffix = f"_{selected_db}" if selected_db != 'all' else "_all"
    filename = f"mongodb_slow_queries_{filename_suffix}{db_suffix}_{threshold}ms_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Use streaming response for large exports (>5MB)
    if len(export_content) > 5 * 1024 * 1024:  # 5MB threshold
        def generate_stream():
            yield export_content
        
        response = Response(generate_stream(), mimetype='application/json')
    else:
        response = Response(export_content, mimetype='application/json')
    
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    response.headers['X-Export-Count'] = str(len(filtered_queries))  # Actual exported count (post-limit)
    response.headers['X-Export-Limit'] = str(export_limit)  # Max export limit
    response.headers['X-Filtered-Total'] = str(total_matched_pre_limit)  # Total matched before limit applied
    
    return response

@app.route('/search-logs')
def search_logs():
    # Get available date range (prefer SQL if DB-backed)
    try:
        if hasattr(analyzer, 'get_available_date_range_sql') and getattr(analyzer, 'use_database', False):
            min_date, max_date = analyzer.get_available_date_range_sql()
        else:
            min_date, max_date = analyzer.get_available_date_range()
    except Exception:
        min_date, max_date = analyzer.get_available_date_range()
    
    # Get search parameters (legacy single keyword/field kept for compatibility)
    keyword = request.args.get('keyword', '').strip()
    field_name = request.args.get('field_name', '').strip()
    field_value = request.args.get('field_value', '').strip()
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    limit = min(int(request.args.get('limit', 100)), 1000)  # Cap at 1000 results
    
    # Parse date filters with improved handling
    start_date = parse_date_filter(start_date_str, is_end_date=False)
    end_date = parse_date_filter(end_date_str, is_end_date=True)
    
    # Get pagination parameters
    page, per_page = get_pagination_params(request)
    
    # Parse chained filters early so we can trigger search without legacy fields
    conditions = []
    try:
        indices = set()
        for k in request.args.keys():
            if k.startswith('filters['):
                try:
                    idx = int(k.split('[',1)[1].split(']',1)[0])
                    indices.add(idx)
                except Exception:
                    pass
        for idx in sorted(indices):
            getv = lambda suffix, default='': request.args.get(f'filters[{idx}][{suffix}]', default)
            ctype = getv('type')
            if not ctype:
                continue
            cond = {
                'type': ctype,
                'name': getv('name', ''),
                'value': getv('value', ''),
                'regex': getv('regex') in ('on','true','1'),
                'case_sensitive': getv('case') in ('on','true','1'),
                'negate': getv('not') in ('on','true','1'),
            }
            conditions.append(cond)
    except Exception:
        conditions = []

    # Perform search (grep-like with optional multiple conditions)
    results = []
    search_performed = False
    error_message = None
    pagination = None
    total_found_exact = True
    heavy_search_warning = False

    # Trigger search if legacy fields are provided, or chained conditions exist, or a date window is set
    if keyword or (field_name and field_value) or conditions or (start_date or end_date):
        search_performed = True
        try:
            per_page_value = per_page if isinstance(per_page, int) else DEFAULT_QUERY_LIMIT
            base_search_limit = DEFAULT_QUERY_LIMIT if per_page == 'all' else max(per_page_value * 10, per_page_value, DEFAULT_PAGE_SIZE)
            search_limit = max(1, min(limit, base_search_limit, DEFAULT_QUERY_LIMIT))

            heavy_search = (
                bool(keyword)
                and not field_name
                and not field_value
                and not conditions
                and not start_date
                and not end_date
            )

            base_kwargs = {
                'keyword': keyword or None,
                'field_name': field_name or None,
                'field_value': field_value or None,
                'start_date': start_date,
                'end_date': end_date,
                'limit': search_limit,
            }

            # Choose search mode: in-memory (raw) → ephemeral (small files) → streaming (grep-like)
            use_raw_memory = bool(getattr(analyzer, 'raw_log_data', {}))
            search_data = None
            if use_raw_memory:
                # Use existing in-memory search
                try:
                    search_data = analyzer.search_logs(conditions=conditions, **base_kwargs)
                except TypeError:
                    search_data = analyzer.search_logs(**base_kwargs)
            else:
                # Decide ephemeral threshold from env (default 200MB); fallback to streaming otherwise
                total_bytes = int(getattr(analyzer, 'source_total_bytes', 0) or 0)
                try:
                    max_mb = int(os.environ.get('SEARCH_EPHEMERAL_MAX_MB', '200'))
                except Exception:
                    max_mb = 200
                threshold = max_mb * 1024 * 1024
                if total_bytes > 0 and total_bytes <= threshold:
                    # Ephemeral local load for small data
                    local_raw = {}
                    for p in getattr(analyzer, 'source_log_files', []):
                        try:
                            with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                                local_raw[p] = [ln.rstrip('\n') for ln in f]
                        except Exception:
                            continue
                    # Guard for analyzer capability
                    if hasattr(analyzer, 'search_logs_ephemeral'):
                        eph_results = analyzer.search_logs_ephemeral(
                            local_raw,
                            keyword=base_kwargs['keyword'],
                            field_name=base_kwargs['field_name'],
                            field_value=base_kwargs['field_value'],
                            start_date=base_kwargs['start_date'],
                            end_date=base_kwargs['end_date'],
                            limit=base_kwargs['limit'],
                            conditions=conditions
                        )
                        if isinstance(eph_results, dict):
                            search_data = eph_results
                        else:
                            search_data = {'results': eph_results, 'total_found': len(eph_results)}
                    else:
                        # Fallback to in-memory search if available
                        try:
                            search_data = analyzer.search_logs(conditions=conditions, **base_kwargs)
                        except TypeError:
                            search_data = analyzer.search_logs(**base_kwargs)
                else:
                    # Streaming grep-like search
                    if hasattr(analyzer, 'search_logs_streaming'):
                        compute_total = not heavy_search
                        if not compute_total:
                            heavy_search_warning = True
                        search_data = analyzer.search_logs_streaming(
                            keyword=base_kwargs['keyword'],
                            field_name=base_kwargs['field_name'],
                            field_value=base_kwargs['field_value'],
                            start_date=base_kwargs['start_date'],
                            end_date=base_kwargs['end_date'],
                            limit=base_kwargs['limit'],
                            compute_total=compute_total,
                            conditions=conditions
                        )
                    else:
                        # Fallback to in-memory search if available
                        try:
                            search_data = analyzer.search_logs(conditions=conditions, **base_kwargs)
                        except TypeError:
                            search_data = analyzer.search_logs(**base_kwargs)
            
            total_found = 0
            # Handle both dict and list returns from search methods
            if isinstance(search_data, dict):
                # Optimized analyzer returns dict with 'results' key
                results = search_data.get('results', [])
                total_found = search_data.get('total_found', len(results))
                if heavy_search_warning:
                    total_found_exact = False
            else:
                # Non-optimized analyzer returns list directly
                results = search_data or []
                total_found = len(results)

            # Apply pagination to results
            pagination = paginate_data(results, page, per_page)
            results = pagination['items']
            
            # Update pagination total if we have more accurate count
            if pagination and total_found:
                pagination['total'] = total_found

        except Exception as e:
            error_message = f"Search error: {str(e)}"

    return render_template('search_logs.html',
                         keyword=keyword,
                         field_name=field_name,
                         field_value=field_value,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         min_date=format_datetime_for_input(min_date),
                         max_date=format_datetime_for_input(max_date),
                         limit=limit,
                         results=results,
                         pagination=pagination,
                         search_performed=search_performed,
                         error_message=error_message,
                         result_count=pagination['total'] if pagination else len(results),
                         total_found_exact=total_found_exact,
                         heavy_search_warning=heavy_search_warning)

@app.route('/export-search-results')
def export_search_results():
    # Get the same search parameters
    keyword = request.args.get('keyword', '').strip()
    field_name = request.args.get('field_name', '').strip()
    field_value = request.args.get('field_value', '').strip()
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    limit = min(int(request.args.get('limit', 1000)), 1000)
    
    # Parse date filters with improved handling
    start_date = parse_date_filter(start_date_str, is_end_date=False)
    end_date = parse_date_filter(end_date_str, is_end_date=True)
    
    # Parse chained filters
    conditions = []
    try:
        indices = set()
        for k in request.args.keys():
            if k.startswith('filters['):
                try:
                    idx = int(k.split('[',1)[1].split(']',1)[0])
                    indices.add(idx)
                except Exception:
                    pass
        for idx in sorted(indices):
            getv = lambda suffix, default='': request.args.get(f'filters[{idx}][{suffix}]', default)
            ctype = getv('type')
            if not ctype:
                continue
            conditions.append({
                'type': ctype,
                'name': getv('name',''),
                'value': getv('value',''),
                'regex': getv('regex') in ('on','true','1'),
                'case_sensitive': getv('case') in ('on','true','1'),
                'negate': getv('not') in ('on','true','1'),
            })
    except Exception:
        conditions = []

    # Perform search via streaming grep-like to compute accurate totals
    if hasattr(analyzer, 'search_logs_streaming'):
        search_data = analyzer.search_logs_streaming(
            keyword=keyword or None,
            field_name=field_name or None,
            field_value=field_value or None,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            compute_total=True,
            conditions=conditions
        )
    else:
        try:
            fallback_results = analyzer.search_logs(
                keyword=keyword or None,
                field_name=field_name or None,
                field_value=field_value or None,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                conditions=conditions
            )
        except TypeError:
            fallback_results = analyzer.search_logs(
                keyword=keyword or None,
                field_name=field_name or None,
                field_value=field_value or None,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )

        if isinstance(fallback_results, dict):
            search_data = fallback_results
        else:
            search_data = {'results': fallback_results, 'total_found': len(fallback_results)}
    
    # Handle dict return from streaming search
    results = search_data.get('results', []) if isinstance(search_data, dict) else (search_data or [])
    
    # Create export data with only original JSON entries
    original_log_entries = []
    
    for result in results:
        # Handle case where result might be a string instead of dict
        if isinstance(result, str):
            try:
                original_json = json.loads(result)
                original_log_entries.append(original_json)
            except json.JSONDecodeError:
                # Skip invalid entries
                continue
        elif isinstance(result, dict):
            # Try different field names based on analyzer type
            if result.get('parsed_json'):
                # Non-optimized analyzer format
                original_log_entries.append(result['parsed_json'])
            else:
                # Try to parse from available line content
                raw_line = (result.get('raw_line') or 
                           result.get('line_content') or 
                           '')
                
                if raw_line:
                    try:
                        original_json = json.loads(raw_line)
                        original_log_entries.append(original_json)
                    except json.JSONDecodeError:
                        # Skip invalid JSON entries
                        continue
        else:
            # Skip unexpected data types
            continue
    
    export_content = json.dumps(original_log_entries, indent=2, default=str)
    
    # Generate filename
    search_term = keyword or f"{field_name}_{field_value}" or "search"
    filename = f"mongodb_search_{search_term.replace(' ', '_').replace(':', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    response = Response(export_content, mimetype='application/json')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    # Pre-limit totals for validation/analytics
    try:
        total_found = int(search_data.get('total_found', len(results))) if isinstance(search_data, dict) else len(results)
    except Exception:
        total_found = len(results)
    response.headers['X-Search-Total'] = str(total_found)
    response.headers['X-Returned-Count'] = str(len(original_log_entries))
    response.headers['X-Export-Limit'] = str(limit)

    return response

@app.route('/index-suggestions')
def index_suggestions():
    # Ensure user correlation is done before analysis
    analyzer.correlate_users_with_access()
    
    # Analyze index suggestions
    suggestions = analyzer.analyze_index_suggestions()
    
    # Calculate summary statistics
    total_collscan_queries = sum(data['collscan_queries'] for data in suggestions.values())
    total_suggestions = sum(len(data['suggestions']) for data in suggestions.values())
    
    # Calculate average docs examined across all COLLSCAN queries
    total_docs_examined = sum(data['total_docs_examined'] for data in suggestions.values())
    avg_docs_examined = total_docs_examined / total_collscan_queries if total_collscan_queries > 0 else 0
    
    # Build Top-by-Impact list across all collections
    top_suggestions = []
    for coll_name, data in suggestions.items():
        for sug in data.get('suggestions', []):
            entry = {
                'collection': coll_name,
                'impact_score': sug.get('impact_score', 0),
                'index': sug.get('index', ''),
                'reason': sug.get('reason', ''),
                'confidence': sug.get('confidence', 'high'),
                'occurrences': sug.get('occurrences', 0),
                'avg_duration_ms': sug.get('avg_duration_ms', 0),
                'inefficiency_ratio': sug.get('inefficiency_ratio', None),
                'command': sug.get('command', ''),
                'priority': sug.get('priority', 'high')
            }
            top_suggestions.append(entry)
    top_suggestions.sort(key=lambda x: x.get('impact_score', 0), reverse=True)
    top_suggestions = top_suggestions[:10]

    return render_template('index_suggestions.html',
                         suggestions=suggestions,
                         total_collscan_queries=total_collscan_queries,
                         total_suggestions=total_suggestions,
                         avg_docs_examined=avg_docs_examined,
                         top_suggestions=top_suggestions)

@app.route('/export-index-suggestions')
def export_index_suggestions():
    # Ensure user correlation is done before analysis
    analyzer.correlate_users_with_access()
    
    # Analyze index suggestions
    suggestions = analyzer.analyze_index_suggestions()
    
    # Create export content with all index creation commands
    export_data = {
        'generated_on': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_collections': len(suggestions),
        'index_commands': []
    }
    
    for collection_name, data in suggestions.items():
        collection_commands = {
            'collection': collection_name,
            'collscan_queries': data.get('collscan_queries', 0),
            'ixscan_ineff_queries': data.get('ixscan_ineff_queries', 0),
            'avg_duration_ms': round(data.get('avg_duration', 0)),
            'avg_docs_per_query': round(data.get('avg_docs_per_query', 0)),
            'commands': []
        }
        
        for suggestion in data.get('suggestions', []):
            collection_commands['commands'].append({
                'priority': suggestion.get('priority', 'medium'),
                'reason': suggestion.get('reason', 'Index suggestion'),
                'command': suggestion.get('command', ''),
                'index': suggestion.get('index', ''),
                'fields': suggestion.get('fields', []),
                'confidence': suggestion.get('confidence', 'high'),
                'impact_score': suggestion.get('impact_score', 0),
                'occurrences': suggestion.get('occurrences', 0),
                'avg_duration_ms': suggestion.get('avg_duration_ms', 0),
                'inefficiency_ratio': suggestion.get('inefficiency_ratio', None),
                'how_to_validate': 'Create as hidden index, run explain() to confirm usage, then unhide if beneficial.'
            })
        
        if collection_commands['commands']:  # Only include collections with suggestions
            export_data['index_commands'].append(collection_commands)
    
    export_content = json.dumps(export_data, indent=2, default=str)
    
    # Generate filename
    filename = f"mongodb_index_suggestions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    response = Response(export_content, mimetype='application/json')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    
    return response

@app.route('/slow-query-analysis')
def slow_query_analysis():
    # Ensure user correlation is done before analysis
    analyzer.correlate_users_with_access()
    
    # Get available date range from log data
    min_date, max_date = analyzer.get_available_date_range()
    
    # Get filter parameters
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    grouping_type = request.args.get('grouping', 'pattern_key')
    # Handle checkbox logic - with hidden field, we get multiple values when checked
    exclude_system_db_values = request.args.getlist('exclude_system_db')
    if not exclude_system_db_values:
        exclude_system_db = True  # Default behavior when no parameters
    else:
        # If checkbox is checked, we get ['0', '1'], if unchecked we get ['0']
        exclude_system_db = '1' in exclude_system_db_values

    # Debug output
    # Parse date filters with improved handling
    start_date = parse_date_filter(start_date_str, is_end_date=False)
    end_date = parse_date_filter(end_date_str, is_end_date=True)

    # Filter slow queries by date range and optionally exclude system databases
    filtered_queries = []
    excluded_count = 0
    system_db_count = 0

    for query in analyzer.slow_queries:
        # Count system database queries
        if is_system_database(query.get('database')):
            system_db_count += 1

        # Optionally exclude system databases from analysis
        if exclude_system_db and is_system_database(query.get('database')):
            excluded_count += 1
            continue

        # Date filters with timezone normalization
        query_timestamp = normalize_datetime_for_comparison(query.get('timestamp'))
        if start_date and query_timestamp and query_timestamp < start_date:
            continue
        if end_date and query_timestamp and query_timestamp > end_date:
            continue
        filtered_queries.append(query)

    # Get pagination parameters
    page, per_page = get_pagination_params(request)
    
    # Analyze query patterns with filtered data
    try:
        patterns_dict = analyzer.analyze_query_patterns(filtered_queries)
    except TypeError:
        original_queries = analyzer.slow_queries
        analyzer.slow_queries = filtered_queries
        try:
            patterns_dict = analyzer.analyze_query_patterns()
        finally:
            analyzer.slow_queries = original_queries

    patterns = _aggregate_patterns_by_group(patterns_dict, grouping_type)

    # Calculate summary statistics
    total_executions = sum(pattern['total_count'] for pattern in patterns.values())
    high_priority_count = sum(1 for pattern in patterns.values() if pattern['optimization_potential'] == 'high')
    
    # Calculate overall average duration
    if total_executions > 0:
        total_duration = sum(pattern['avg_duration'] * pattern['total_count'] for pattern in patterns.values())
        avg_duration = total_duration / total_executions
    else:
        avg_duration = 0
    
    # Convert patterns dict to list for pagination
    pattern_count = len(patterns)
    patterns_list = list(patterns.items())
    
    # Apply pagination
    pagination = paginate_data(patterns_list, page, per_page)
    
    # Convert paginated items back to dict for template
    paginated_patterns = dict(pagination['items']) if pagination['items'] else {}
    
    return render_template('slow_query_analysis.html',
                         patterns=paginated_patterns,
                         pagination=pagination,
                         total_executions=total_executions,
                         avg_duration=avg_duration,
                         high_priority_count=high_priority_count,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         exclude_system_db=exclude_system_db,
                         grouping_type=grouping_type,
                         pattern_count=pattern_count,
                         min_date=min_date.strftime('%Y-%m-%dT%H:%M') if min_date else None,
                         max_date=max_date.strftime('%Y-%m-%dT%H:%M') if max_date else None)

@app.route('/export-query-analysis')
def export_query_analysis():
    # Ensure user correlation is done before analysis
    analyzer.correlate_users_with_access()
    
    # Get filter parameters (same as main analysis page)
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    grouping_type = request.args.get('grouping', 'pattern_key')
    # Handle checkbox logic - with hidden field, we get multiple values when checked
    exclude_system_db_values = request.args.getlist('exclude_system_db')
    if not exclude_system_db_values:
        exclude_system_db = True  # Default behavior when no parameters
    else:
        # If checkbox is checked, we get ['0', '1'], if unchecked we get ['0']
        exclude_system_db = '1' in exclude_system_db_values

    # Parse date filters
    start_date = parse_date_filter(start_date_str, is_end_date=False)
    end_date = parse_date_filter(end_date_str, is_end_date=True)

    # Filter slow queries by date range and optionally exclude system databases
    filtered_queries = []

    for query in analyzer.slow_queries:
        # Optionally exclude system databases from analysis
        if exclude_system_db and is_system_database(query.get('database')):
            continue

        # Date filters with timezone normalization
        query_timestamp = normalize_datetime_for_comparison(query.get('timestamp'))
        if start_date and query_timestamp and query_timestamp < start_date:
            continue
        if end_date and query_timestamp and query_timestamp > end_date:
            continue
        filtered_queries.append(query)
    
    # Temporarily replace slow_queries for analysis
    original_queries = analyzer.slow_queries
    analyzer.slow_queries = filtered_queries
    
    # Analyze query patterns
    patterns_dict = analyzer.analyze_query_patterns()

    # Restore original queries
    analyzer.slow_queries = original_queries

    patterns = _aggregate_patterns_by_group(patterns_dict, grouping_type)

    # Create export data
    export_data = {
        'generated_on': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_patterns': len(patterns),
        'grouping': grouping_type,
        'summary': {
            'total_executions': sum(pattern['total_count'] for pattern in patterns.values()),
            'high_priority_issues': sum(1 for pattern in patterns.values() if pattern['optimization_potential'] == 'high'),
            'avg_duration_ms': sum(pattern['avg_duration'] * pattern['total_count'] for pattern in patterns.values()) / sum(pattern['total_count'] for pattern in patterns.values()) if patterns else 0
        },
        'patterns': []
    }

    for pattern_key, pattern in patterns.items():
        pattern_export = {
            'pattern_key': pattern_key,
            'database': pattern['database'],
            'collection': pattern['collection'],
            'query_hash': pattern['query_hash'],
            'query_type': pattern['query_type'],
            'plan_summary': pattern['plan_summary'],
            'complexity_score': pattern['complexity_score'],
            'optimization_potential': pattern['optimization_potential'],
            'performance_stats': {
                'total_executions': pattern['total_count'],
                'avg_duration_ms': round(pattern['avg_duration']),
                'min_duration_ms': round(pattern['min_duration']),
                'max_duration_ms': round(pattern.get('max_duration', 0)),
                'median_duration_ms': round(pattern['median_duration'])
            },
            'efficiency_metrics': {
                'total_docs_examined': pattern['total_docs_examined'],
                'total_keys_examined': pattern['total_keys_examined'],
                'total_returned': pattern['total_returned'],
                'avg_selectivity_percent': round(pattern['avg_selectivity'], 2),
                'avg_index_efficiency_percent': round(pattern['avg_index_efficiency'], 2)
            },
            'sample_query': pattern['sample_query'],
            'first_seen': pattern['first_seen'].isoformat() if pattern['first_seen'] else None,
            'last_seen': pattern['last_seen'].isoformat() if pattern['last_seen'] else None
        }
        
        export_data['patterns'].append(pattern_export)
    
    export_content = json.dumps(export_data, indent=2, default=str)
    
    # Generate filename
    filename = f"mongodb_query_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    response = Response(export_content, mimetype='application/json')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    
    return response

@app.route('/current-op-analyzer', methods=['GET', 'POST'])
def current_op_analyzer():
    """Analyze MongoDB db.currentOp() output"""
    if request.method == 'POST':
        try:
            current_op_data = ''
            original_data = ''
            
            # Check if data was uploaded as a file
            if 'currentop_file' in request.files:
                file = request.files['currentop_file']
                if file and file.filename:
                    try:
                        # Read file content
                        file_content = file.read().decode('utf-8')
                        current_op_data = file_content.strip()
                        original_data = f"[From file: {file.filename}]\n{current_op_data}"
                    except UnicodeDecodeError:
                        flash('Error reading file. Please ensure it contains valid text data.', 'error')
                        return render_template('current_op_analyzer.html')
            
            # If no file data, check for pasted text
            if not current_op_data:
                current_op_data = request.form.get('current_op_data', '').strip()
                original_data = current_op_data
            
            if not current_op_data:
                flash('Please paste the db.currentOp() output or upload a file containing the output.', 'error')
                return render_template('current_op_analyzer.html')
            
            # Parse filter inputs
            try:
                threshold = int(request.form.get('threshold', 30) or 30)
            except Exception:
                threshold = 30
            only_active = request.form.get('only_active') == 'on'
            only_waiting = request.form.get('only_waiting') == 'on'
            only_collscan = request.form.get('only_collscan') == 'on'

            # Parse and analyze the currentOp data with filters
            analysis = analyze_current_op(
                current_op_data,
                threshold=threshold,
                filters={
                    'only_active': only_active,
                    'only_waiting': only_waiting,
                    'only_collscan': only_collscan,
                }
            )
            
            return render_template('current_op_analyzer.html', 
                                 analysis=analysis, 
                                 original_data=original_data)
                                 
        except Exception as e:
            flash(f'Error analyzing currentOp data: {str(e)}', 'error')
            return render_template('current_op_analyzer.html')
    
    return render_template('current_op_analyzer.html')

def analyze_current_op(current_op_data, threshold: int = 30, filters: dict | None = None):
    """Analyze db.currentOp() output and provide insights"""
    try:
        # Parse JSON data
        if current_op_data.startswith('db.currentOp()'):
            current_op_data = current_op_data.replace('db.currentOp()', '').strip()
        
        # Handle various formats
        if not current_op_data.startswith('{'):
            current_op_data = current_op_data.strip()
            if current_op_data.startswith('inprog'):
                # Handle output that starts with "inprog : ["
                start_idx = current_op_data.find('[')
                end_idx = current_op_data.rfind(']')
                if start_idx != -1 and end_idx != -1:
                    current_op_data = '{"inprog": ' + current_op_data[start_idx:end_idx+1] + '}'
        
        data = json.loads(current_op_data)
        operations = data.get('inprog', []) if isinstance(data, dict) else data
        
        if not operations:
            return {
                'error': 'No operations found in the provided data',
                'total_operations': 0
            }
        
        # Initialize analysis results
        analysis = {
            'total_operations': len(operations),
            'operation_types': Counter(),
            'operation_states': Counter(),
            'long_running_ops': [],
            'resource_intensive_ops': [],
            'lock_analysis': {
                'read_locks': [],
                'write_locks': [],
                'waiting_operations': []
            },
            'collection_hotspots': Counter(),
            'database_hotspots': Counter(),
            'client_connections': Counter(),
            'query_analysis': {
                'collscans': [],
                'index_scans': [],
                'duplicate_queries': []
            },
            'recommendations': [],
            'performance_metrics': {
                'avg_duration': 0,
                'max_duration': 0,
                'min_duration': float('inf'),
                'total_duration': 0
            },
            # Compact resource-centric view per op, shown prominently in UI
            'ops_brief': [],
            'filters': {
                'threshold': threshold,
                'only_active': bool(filters.get('only_active') if filters else False),
                'only_waiting': bool(filters.get('only_waiting') if filters else False),
                'only_collscan': bool(filters.get('only_collscan') if filters else False),
            }
        }
        
        durations = []
        query_patterns = defaultdict(list)
        
        # Analyze each operation
        for op in operations:
            if not isinstance(op, dict):
                continue
                
            # Operation type analysis
            op_type = op.get('op', 'unknown')
            analysis['operation_types'][op_type] += 1
            
            # Operation state analysis  
            if 'active' in op:
                state = 'active' if op['active'] else 'inactive'
            elif 'waitingForLock' in op:
                state = 'waiting_for_lock' if op['waitingForLock'] else 'active'
            else:
                state = 'unknown'
            analysis['operation_states'][state] += 1
            
            # Duration analysis
            duration = 0
            if 'microsecs_running' in op:
                duration = op['microsecs_running'] / 1000000  # Convert to seconds
            elif 'secs_running' in op:
                duration = op['secs_running']
            
            if duration > 0:
                durations.append(duration)
                analysis['performance_metrics']['total_duration'] += duration
                analysis['performance_metrics']['max_duration'] = max(
                    analysis['performance_metrics']['max_duration'], duration
                )
                analysis['performance_metrics']['min_duration'] = min(
                    analysis['performance_metrics']['min_duration'], duration
                )
                
                # Long running operations (> threshold seconds)
                if duration > (threshold or 30):
                    analysis['long_running_ops'].append({
                        'opid': op.get('opid'),
                        'op': op_type,
                        'duration': duration,
                        'ns': op.get('ns', 'unknown'),
                        'desc': op.get('desc', ''),
                        'client': op.get('client', 'unknown')
                    })
            
            # Lock analysis
            locks = op.get('locks', {})
            waiting_for_lock = op.get('waitingForLock', False)
            
            if waiting_for_lock:
                analysis['lock_analysis']['waiting_operations'].append({
                    'opid': op.get('opid'),
                    'op': op_type,
                    'ns': op.get('ns', 'unknown'),
                    'duration': duration
                })
            
            for lock_type, lock_info in locks.items():
                if isinstance(lock_info, dict):
                    lock_mode = lock_info.get('mode', '')
                    if lock_mode in ['R', 'r']:
                        analysis['lock_analysis']['read_locks'].append({
                            'opid': op.get('opid'),
                            'type': lock_type,
                            'ns': op.get('ns', 'unknown')
                        })
                elif lock_mode in ['W', 'w', 'X']:
                    analysis['lock_analysis']['write_locks'].append({
                        'opid': op.get('opid'),
                        'type': lock_type,
                        'ns': op.get('ns', 'unknown')
                    })

            # Compact OS/resource view per op (best-effort)
            try:
                cpu_s = 0.0
                if isinstance(op.get('microsecs_running'), (int, float)):
                    cpu_s = float(op['microsecs_running']) / 1_000_000.0
                elif isinstance(op.get('secs_running'), (int, float)):
                    cpu_s = float(op['secs_running'])

                bytes_read = op.get('bytesRead') or op.get('bytes_read')
                if bytes_read is None and isinstance(op.get('network'), dict):
                    bytes_read = op['network'].get('bytesRead')
                bytes_written = op.get('bytesWritten') or op.get('bytes_written')
                if bytes_written is None and isinstance(op.get('network'), dict):
                    bytes_written = op['network'].get('bytesWritten')

                mem_mb = None
                mem = op.get('memory') or {}
                if isinstance(mem, dict):
                    mem_mb = mem.get('residentMb') or mem.get('residentMB') or mem.get('workingSetMb')
                if mem_mb is None:
                    ws = op.get('workingSet') or op.get('resident')
                    if isinstance(ws, (int, float)):
                        mem_mb = round(float(ws) / (1024*1024), 2)

                analysis['ops_brief'].append({
                    'opid': op.get('opid'),
                    'ns': op.get('ns'),
                    'op': op.get('op'),
                    'client': op.get('client') or op.get('conn'),
                    'active': bool(op.get('active')),
                    'waitingForLock': bool(op.get('waitingForLock')),
                    'planSummary': op.get('planSummary') or 'None',
                    'cpuTime_s': round(cpu_s, 3),
                    'bytesRead': bytes_read if isinstance(bytes_read, (int, float)) else None,
                    'bytesWritten': bytes_written if isinstance(bytes_written, (int, float)) else None,
                    'memoryMb': mem_mb,
                })
            except Exception:
                pass
            
            # Collection and database hotspots
            ns = op.get('ns', '')
            if ns and '.' in ns:
                db_name, collection_name = ns.split('.', 1)
                analysis['database_hotspots'][db_name] += 1
                analysis['collection_hotspots'][ns] += 1
            
            # Client connection analysis
            client = op.get('client', 'unknown')
            if client != 'unknown':
                analysis['client_connections'][client] += 1
            
            # Query analysis
            command = op.get('command', {})
            plan_summary = op.get('planSummary', '')
            
            if plan_summary == 'COLLSCAN':
                analysis['query_analysis']['collscans'].append({
                    'opid': op.get('opid'),
                    'ns': ns,
                    'duration': duration,
                    'command': str(command)[:200] + '...' if len(str(command)) > 200 else str(command)
                })
            elif 'IXSCAN' in plan_summary:
                analysis['query_analysis']['index_scans'].append({
                    'opid': op.get('opid'),
                    'ns': ns,
                    'plan': plan_summary
                })
            
            # Group similar queries for duplicate detection
            if command:
                query_key = _normalize_query_for_grouping(command)
                query_patterns[query_key].append({
                    'opid': op.get('opid'),
                    'ns': ns,
                    'duration': duration
                })
        
        # Calculate average duration
        if durations:
            analysis['performance_metrics']['avg_duration'] = sum(durations) / len(durations)
        else:
            analysis['performance_metrics']['min_duration'] = 0
        
        # Find duplicate queries
        for query_key, ops in query_patterns.items():
            if len(ops) > 1:
                analysis['query_analysis']['duplicate_queries'].append({
                    'query_pattern': query_key[:100] + '...' if len(query_key) > 100 else query_key,
                    'count': len(ops),
                    'operations': ops
                })
        
        # Sort compact ops by cpu time (desc) and cap list for UI
        try:
            analysis['ops_brief'].sort(key=lambda x: (x.get('cpuTime_s') or 0.0), reverse=True)
            analysis['ops_brief'] = analysis['ops_brief'][:50]
        except Exception:
            pass

        # Generate recommendations
        analysis['recommendations'] = _generate_current_op_recommendations(analysis)
        
        return analysis
        
    except json.JSONDecodeError as e:
        return {
            'error': f'Invalid JSON format: {str(e)}',
            'total_operations': 0
        }
    except Exception as e:
        return {
            'error': f'Error analyzing data: {str(e)}',
            'total_operations': 0
        }

def _normalize_query_for_grouping(command):
    """Normalize query command for grouping similar operations"""
    if not isinstance(command, dict):
        return str(command)
    
    # Create a normalized version by removing variable values
    normalized = {}
    for key, value in command.items():
        if key in ['find', 'aggregate', 'update', 'insert', 'delete']:
            normalized[key] = value  # Keep collection name
        elif key in ['filter', 'query', 'pipeline']:
            # Normalize the query structure
            normalized[key] = _normalize_query_structure(value)
        else:
            normalized[key] = type(value).__name__  # Just keep the type
    
    return json.dumps(normalized, sort_keys=True)

def _normalize_query_structure(query):
    """Normalize query structure by replacing values with types"""
    if isinstance(query, dict):
        normalized = {}
        for k, v in query.items():
            if isinstance(v, (str, int, float, bool)):
                normalized[k] = type(v).__name__
            elif isinstance(v, (list, dict)):
                normalized[k] = _normalize_query_structure(v)
            else:
                normalized[k] = 'mixed'
        return normalized
    elif isinstance(query, list):
        if query:
            return [_normalize_query_structure(query[0])] if query else []
        return []
    else:
        return type(query).__name__

def _extract_index_info(plan_summary):
    """Extract index information from planSummary field"""
    if not plan_summary or plan_summary == 'None':
        return {
            'scan_type': 'Unknown',
            'index_pattern': None,
            'index_name': None,
            'display_text': 'Unknown'
        }
    
    # Handle different plan summary formats
    if plan_summary == 'COLLSCAN':
        return {
            'scan_type': 'COLLSCAN',
            'index_pattern': None,
            'index_name': None,
            'display_text': 'Collection Scan (No Index)'
        }
    elif plan_summary.startswith('IXSCAN'):
        # Extract index pattern from plan summary like "IXSCAN { userId: 1, status: 1 }"
        index_pattern = None
        display_text = 'Index Scan'
        
        if '{' in plan_summary and '}' in plan_summary:
            try:
                # Extract the pattern between braces
                start = plan_summary.find('{')
                end = plan_summary.rfind('}') + 1
                pattern_str = plan_summary[start:end]
                
                # Try to parse as JSON to validate and format
                index_pattern = json.loads(pattern_str)
                
                # Create readable display text
                if isinstance(index_pattern, dict):
                    fields = []
                    for field, direction in index_pattern.items():
                        if direction == 1:
                            fields.append(f"{field} (asc)")
                        elif direction == -1:
                            fields.append(f"{field} (desc)")
                        else:
                            fields.append(f"{field}")
                    display_text = f"Index Scan: {', '.join(fields)}"
                else:
                    display_text = f"Index Scan: {pattern_str}"
                    
            except:
                # If JSON parsing fails, extract as string
                start = plan_summary.find('{')
                end = plan_summary.rfind('}') + 1
                pattern_str = plan_summary[start:end]
                index_pattern = pattern_str
                display_text = f"Index Scan: {pattern_str}"
        
        return {
            'scan_type': 'IXSCAN',
            'index_pattern': index_pattern,
            'index_name': None,  # Would need detailed logs for actual index name
            'display_text': display_text
        }
    else:
        # Handle other plan types (SORT, PROJECTION, etc.)
        return {
            'scan_type': plan_summary,
            'index_pattern': None,
            'index_name': None,
            'display_text': plan_summary
        }

def _generate_current_op_recommendations(analysis):
    """Generate recommendations based on currentOp analysis"""
    recommendations = []
    
    # Long running operations
    if analysis['long_running_ops']:
        recommendations.append({
            'type': 'warning',
            'title': 'Long-Running Operations Detected',
            'description': f"Found {len(analysis['long_running_ops'])} operations running longer than 30 seconds.",
            'action': 'Review and consider killing operations that may be stuck or inefficient.',
            'priority': 'high'
        })
    
    # Collection scans
    if analysis['query_analysis']['collscans']:
        recommendations.append({
            'type': 'error',
            'title': 'Collection Scans Detected',
            'description': f"Found {len(analysis['query_analysis']['collscans'])} operations performing full collection scans.",
            'action': 'Consider adding appropriate indexes to improve query performance.',
            'priority': 'high'
        })
    
    # Duplicate queries
    if analysis['query_analysis']['duplicate_queries']:
        recommendations.append({
            'type': 'info',
            'title': 'Duplicate Operations Found',
            'description': f"Found {len(analysis['query_analysis']['duplicate_queries'])} patterns with multiple concurrent executions.",
            'action': 'Review if these operations can be optimized or cached.',
            'priority': 'medium'
        })
    
    # Lock contention
    if analysis['lock_analysis']['waiting_operations']:
        recommendations.append({
            'type': 'warning',
            'title': 'Lock Contention Detected',
            'description': f"Found {len(analysis['lock_analysis']['waiting_operations'])} operations waiting for locks.",
            'action': 'Review operations causing lock contention and consider optimization.',
            'priority': 'high'
        })
    
    # High connection count from single client
    for client, count in analysis['client_connections'].most_common(3):
        if count > 10:
            recommendations.append({
                'type': 'info',
                'title': 'High Connection Count',
                'description': f"Client {client} has {count} active operations.",
                'action': 'Review connection pooling and operation efficiency for this client.',
                'priority': 'medium'
            })
    
    # High activity on specific collections
    for ns, count in analysis['collection_hotspots'].most_common(3):
        if count > 5:
            recommendations.append({
                'type': 'info',
                'title': 'Collection Hotspot',
                'description': f"Collection {ns} has {count} concurrent operations.",
                'action': 'Monitor for potential bottlenecks and consider sharding if needed.',
                'priority': 'low'
            })
    
    return recommendations




@app.route('/workload-summary')
def workload_summary():
    """OS Resource-centric workload analysis with real storage and CPU metrics from database"""
    # Get comprehensive OS resource workload data using actual database metrics
    workload_data = analyzer.get_os_resource_workload_summary()
    
    # Prepare data for template with proper field mapping
    top_docs = []  # Using docs_examined from old analyze_query_patterns for backward compatibility
    top_keys = []  # Using keys_examined from old analyze_query_patterns for backward compatibility 
    
    # Get docs/keys data for backward compatibility (these use existing analysis method)
    try:
        patterns = analyzer.analyze_query_patterns()
        entries = []
        for key, pat in patterns.items():
            docs = int(pat.get('total_docs_examined', 0) or 0)
            keys = int(pat.get('total_keys_examined', 0) or 0)
            if docs > 0 or keys > 0:
                entries.append({
                    'database': pat.get('database', 'unknown'),
                    'collection': pat.get('collection', 'unknown'),
                    'plan_summary': pat.get('plan_summary', 'None'),
                    'total_count': pat.get('total_count', 0),
                    'avg_duration': int(pat.get('avg_duration', 0) or 0),
                    'docs_examined': docs,
                    'keys_examined': keys
                })
        
        # Get top docs/keys examined for compatibility
        top_docs = sorted([e for e in entries if e['docs_examined'] > 0], 
                         key=lambda e: e['docs_examined'], reverse=True)[:10]
        top_keys = sorted([e for e in entries if e['keys_examined'] > 0], 
                         key=lambda e: e['keys_examined'], reverse=True)[:10]
    except Exception as e:
        print(f"⚠️  Error getting docs/keys data: {e}")

    return render_template('workload_summary.html',
                           # Legacy fields for template compatibility
                           top_docs=top_docs,
                           top_keys=top_keys,
                           # Real OS resource data from database
                           top_bytes=workload_data['top_bytes_read'],
                           top_cpu=workload_data['top_cpu_usage'],
                           top_mem=workload_data['top_bytes_written'],  # Use bytes_written for "memory" section
                           # Additional new data
                           top_io_time=workload_data['top_io_time'],
                           workload_stats=workload_data['stats'])

@app.route('/search-user-access')
def search_user_access():
    """Search User Access page with authentication table integration"""
    if not analyzer:
        flash('Please upload a log file first.', 'warning')
        return redirect(url_for('index'))
    
    # Get filter parameters
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    filter_type = request.args.get('filter_type', '').strip()
    filter_value = request.args.get('filter_value', '').strip()
    use_regex = request.args.get('use_regex') == 'on'
    
    # Map combined filter to individual parameters
    ip_filter = filter_value if filter_type == 'ip_address' else ''
    username_filter = filter_value if filter_type == 'username' else ''
    mechanism_filter = filter_value if filter_type == 'mechanism' else ''
    auth_status_filter = filter_value if filter_type == 'auth_status' else ''
    
    # Get pagination parameters
    page, per_page = get_pagination_params(request)
    
    # Parse date filters
    parsed_start = parse_date_filter(start_date) if start_date else None
    parsed_end = parse_date_filter(end_date, is_end_date=True) if end_date else None
    
    try:
        # Get authentication data directly from database
        user_access_data = analyzer.get_authentication_data(
            start_date=parsed_start,
            end_date=parsed_end,
            ip_filter=ip_filter,
            username_filter=username_filter,
            mechanism_filter=mechanism_filter,
            auth_status_filter=auth_status_filter,
            use_regex=use_regex,
            page=page,
            per_page=per_page
        )
        
        # Get available dropdown options from authentication table
        available_mechanisms = analyzer.get_available_mechanisms_from_auth()
        available_ips = analyzer.get_available_ips_from_auth()
        available_users = analyzer.get_available_users_from_auth()
        available_auth_statuses = analyzer.get_available_auth_statuses()
        
        return render_template('search_user_access.html',
                             user_access_data=user_access_data,
                             available_mechanisms=sorted(available_mechanisms),
                             available_ips=sorted(available_ips),
                             available_users=sorted(available_users),
                             available_auth_statuses=sorted(available_auth_statuses),
                             filters={
                                 'start_date': start_date,
                                 'end_date': end_date,
                                 'filter_type': filter_type,
                                 'filter_value': filter_value,
                                 'use_regex': use_regex,
                                 # Keep legacy for template compatibility
                                 'ip_address': ip_filter,
                                 'username': username_filter,
                                 'mechanism': mechanism_filter,
                                 'auth_status': auth_status_filter
                             })
        
    except ValueError as e:
        # Handle regex validation errors specifically
        flash(f'Invalid regex pattern: {str(e)}', 'error')
        # Return to empty state with available options
        user_access_data = {
            'items': [],
            'page': 1,
            'per_page': per_page,
            'total': 0,
            'pages': 0,
            'has_prev': False,
            'has_next': False,
            'prev_num': None,
            'next_num': None
        }
        
        # Get available dropdown options from authentication table
        available_mechanisms = analyzer.get_available_mechanisms_from_auth()
        available_ips = analyzer.get_available_ips_from_auth()
        available_users = analyzer.get_available_users_from_auth()
        available_auth_statuses = analyzer.get_available_auth_statuses()
        
        return render_template('search_user_access.html',
                             user_access_data=user_access_data,
                             available_mechanisms=sorted(available_mechanisms),
                             available_ips=sorted(available_ips),
                             available_users=sorted(available_users),
                             available_auth_statuses=sorted(available_auth_statuses),
                             filters={
                                 'start_date': start_date,
                                 'end_date': end_date,
                                 'filter_type': filter_type,
                                 'filter_value': filter_value,
                                 'use_regex': use_regex,
                                 # Keep legacy for template compatibility
                                 'ip_address': ip_filter,
                                 'username': username_filter,
                                 'mechanism': mechanism_filter,
                                 'auth_status': auth_status_filter
                             })
        
    except Exception as e:
        flash(f'Error retrieving user access data: {str(e)}', 'error')
        # Return empty pagination structure for error case
        empty_pagination_dict = {
            'items': [],
            'page': 1,
            'per_page': per_page,
            'total': 0,
            'pages': 0,
            'has_prev': False,
            'has_next': False,
            'prev_num': None,
            'next_num': None
        }
        
        return render_template('search_user_access.html',
                             user_access_data=empty_pagination_dict,
                             available_mechanisms=[],
                             available_ips=[],
                             available_users=[],
                             available_auth_statuses=[],
                             filters={
                                 'start_date': start_date,
                                 'end_date': end_date,
                                 'filter_type': filter_type,
                                 'filter_value': filter_value,
                                 'ip_address': ip_filter,
                                 'username': username_filter,
                                 'mechanism': mechanism_filter,
                                 'auth_status': auth_status_filter
                             })

@app.route('/query-trend')
def query_trend():
    """Enhanced Query Trend page with scatter plot and top queries table"""
    if not analyzer:
        flash('Please upload a log file first.', 'warning')
        return redirect(url_for('index'))
    
    # Get filter parameters
    grouping_type = request.args.get('groupingType', 'pattern_key')
    selected_db = request.args.get('database', 'all')
    selected_namespace = request.args.get('namespace', 'all')
    
    try:
        dataset = analyzer.get_query_trend_dataset(
            grouping=grouping_type,
            database=selected_db,
            namespace=selected_namespace,
            limit=500,
            scatter_limit=2000
        )

        patterns = dataset.get('patterns', [])
        executions = dataset.get('executions', [])

        # Ensure executions carry a group key for quick lookup (fallback datasets)
        def resolve_group_key(exec_row):
            if 'group_key' in exec_row:
                return exec_row['group_key']
            ns = exec_row.get('namespace')
            if not ns:
                ns = f"{exec_row.get('database', 'unknown')}.{exec_row.get('collection', 'unknown')}"
            if grouping_type == 'namespace':
                return ns
            elif grouping_type == 'query_hash':
                return exec_row.get('query_hash', 'unknown')
            return f"{exec_row.get('query_hash', 'unknown')}_{exec_row.get('database', 'unknown')}_{exec_row.get('collection', 'unknown')}"

        for exec_row in executions:
            exec_row['group_key'] = resolve_group_key(exec_row)

        # Prepare enhanced data for scatter plot and table
        trend_data = []
        top_queries_data = []
        query_details = {}  # For modal popups

        def format_duration_time(total_ms):
            """Convert milliseconds to human-readable format"""
            total_seconds = total_ms / 1000
            if total_seconds < 60:
                return f"{total_seconds:.1f}s"
            elif total_seconds < 3600:
                minutes = total_seconds / 60
                return f"{minutes:.1f}m"
            else:
                hours = total_seconds / 3600
                return f"{hours:.1f}h"
        
        def format_duration_detailed(ms_value):
            """Convert milliseconds to human-readable format with raw value"""
            if ms_value < 1000:
                return {
                    'formatted': f"{ms_value:.0f}ms",
                    'raw': f"{ms_value:.0f}ms"
                }
            elif ms_value < 60000:  # Less than 1 minute
                seconds = ms_value / 1000
                return {
                    'formatted': f"{seconds:.1f}s",
                    'raw': f"{ms_value:,.0f}ms"
                }
            elif ms_value < 3600000:  # Less than 1 hour
                minutes = ms_value / 60000
                return {
                    'formatted': f"{minutes:.1f}m",
                    'raw': f"{ms_value:,.0f}ms"
                }
            else:  # 1 hour or more
                hours = ms_value / 3600000
                return {
                    'formatted': f"{hours:.1f}h",
                    'raw': f"{ms_value:,.0f}ms"
                }

        def infer_operation(query_blob):
            try:
                if not query_blob:
                    return 'unknown'
                if isinstance(query_blob, str):
                    parsed = json.loads(query_blob)
                elif isinstance(query_blob, dict):
                    parsed = query_blob
                else:
                    return 'unknown'

                if not isinstance(parsed, dict):
                    return 'unknown'

                operation_candidates = ['findAndModify', 'aggregate', 'find', 'update', 'delete', 'insert', 'distinct', 'count', 'explain']
                for candidate in operation_candidates:
                    if candidate in parsed:
                        return 'update' if candidate == 'findAndModify' else candidate

                command_obj = parsed.get('command') if isinstance(parsed.get('command'), dict) else None
                if command_obj:
                    for candidate in operation_candidates:
                        if candidate in command_obj:
                            return 'update' if candidate == 'findAndModify' else candidate
                    op_name = command_obj.get('commandName') or command_obj.get('operation')
                    if isinstance(op_name, str) and op_name:
                        return op_name.lower()

                if 'saslStart' in parsed or (command_obj and 'saslStart' in command_obj):
                    return 'saslStart'

                return 'unknown'
            except Exception:
                return 'unknown'

        for exec_row in executions:
            if not exec_row.get('operation_type'):
                query_text = exec_row.get('query_text') or exec_row.get('query')
                exec_row['operation_type'] = infer_operation(query_text)

        # Process each pattern for both scatter plot and table
        for pattern in patterns:
            if not pattern:
                continue

            namespace = pattern.get('namespace') or f"{pattern.get('database', 'unknown')}.{pattern.get('collection', 'unknown')}"
            operation_type = pattern.get('operation_type', 'unknown')
            if operation_type == 'unknown':
                operation_type = infer_operation(pattern.get('sample_query'))
            avg_duration = pattern.get('avg_duration', 0)
            max_duration = pattern.get('max_duration', 0)
            total_duration = pattern.get('total_duration', 0)
            execution_count = pattern.get('execution_count', 0)

            mean_duration_formatted = format_duration_detailed(avg_duration)
            max_duration_formatted = format_duration_detailed(max_duration)

            # Prepare table row data
            top_queries_data.append({
                'namespace': namespace,
                'operation': operation_type,
                'execution_count': execution_count,
                'mean_duration': avg_duration,
                'mean_duration_formatted': mean_duration_formatted['formatted'],
                'mean_duration_raw': mean_duration_formatted['raw'],
                'max_duration': max_duration,
                'max_duration_formatted': max_duration_formatted['formatted'],
                'max_duration_raw': max_duration_formatted['raw'],
                'sum_duration': total_duration,
                'sum_duration_formatted': format_duration_time(total_duration),
                'query_hash': pattern.get('query_hash', 'unknown'),
                'pattern_key': pattern.get('group_key'),
                'sample_query': pattern.get('sample_query', '')
            })

            group_key = pattern.get('group_key') or pattern.get('query_hash', 'unknown')

            # Collect executions for this group
            group_execs = [
                exec_row for exec_row in executions
                if exec_row.get('group_key') == group_key
            ]

            # Store query details for modal
            query_details[group_key] = {
                'namespace': namespace,
                'operation_type': operation_type,
                'avg_duration': avg_duration,
                'execution_count': execution_count,
                'sample_query': pattern.get('sample_query', ''),
                'pattern_key': group_key,
                'query_hash': pattern.get('query_hash', 'unknown'),
                'executions': group_execs[:10]
            }

            # Prepare scatter plot data
            for execution in group_execs:
                timestamp = execution.get('timestamp')
                duration = execution.get('duration', 0)
                query_hash = execution.get('query_hash', pattern.get('query_hash', 'unknown'))
                operation = execution.get('operation_type') or operation_type

                if timestamp and duration:
                    # Format timestamp for JavaScript
                    if hasattr(timestamp, 'isoformat'):
                        timestamp_str = timestamp.isoformat()
                    elif isinstance(timestamp, str):
                        timestamp_str = timestamp
                    else:
                        timestamp_str = str(timestamp)
                    
                    trend_data.append({
                        'timestamp': timestamp_str,
                        'duration': duration,
                        'query_hash': query_hash,
                        'database': execution.get('database', pattern.get('database', 'unknown')),
                        'collection': execution.get('collection', pattern.get('collection', 'unknown')),
                        'namespace': namespace,
                        'operation': operation,
                        'pattern_key': group_key,
                        'avg_duration': avg_duration
                    })
        
        # Sort table data by sum duration (highest first)
        top_queries_data.sort(key=lambda x: x['sum_duration'], reverse=True)

        # Sort scatter plot data by timestamp
        trend_data.sort(key=lambda x: x['timestamp'])
        
        # Limit data points for performance (take every nth point if too many)
        max_points = 2000
        if len(trend_data) > max_points:
            step = len(trend_data) // max_points
            trend_data = trend_data[::step]
        
        # Calculate enhanced statistics
        if trend_data:
            durations = [d['duration'] for d in trend_data]
            stats = {
                'total_executions': len(trend_data),
                'avg_duration': sum(durations) / len(durations),
                'max_duration': max(durations),
                'min_duration': min(durations),
                'unique_patterns': len(patterns),
                'unique_namespaces': len(set(d['namespace'] for d in trend_data)),
                'unique_operations': len(set(d['operation'] for d in trend_data))
            }
        else:
            stats = {
                'total_executions': 0,
                'avg_duration': 0,
                'max_duration': 0,
                'min_duration': 0,
                'unique_patterns': 0,
                'unique_namespaces': 0,
                'unique_operations': 0
            }
        
        # Sort table by mean duration (primary) then max duration (secondary) - both descending
        top_queries_data.sort(key=lambda x: (x['mean_duration'], x['max_duration']), reverse=True)
        
        # Get available databases and namespaces for filter dropdowns
        databases = analyzer.get_available_databases() if hasattr(analyzer, 'get_available_databases') else []
        namespaces = analyzer.get_available_namespaces() if hasattr(analyzer, 'get_available_namespaces') else []
        
        return render_template('query_trend.html',
                             trend_data=trend_data,
                             stats=stats,
                             top_queries=top_queries_data[:50],  # Limit to top 50
                             query_details=query_details,
                             grouping_type=grouping_type,
                             databases=sorted(databases),
                             namespaces=sorted(namespaces),
                             selected_db=selected_db,
                             selected_namespace=selected_namespace)
        
    except Exception as e:
        flash(f'Error generating query trend data: {str(e)}', 'error')
        return render_template('query_trend.html',
                             trend_data=[],
                             stats={},
                             top_queries=[],
                             query_details={},
                             grouping_type='pattern_key',
                             databases=[],
                             namespaces=[],
                             selected_db='all',
                             selected_namespace='all')


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)  # Listen only on localhost
