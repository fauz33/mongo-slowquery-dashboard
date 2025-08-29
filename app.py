from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, Response
from werkzeug.utils import secure_filename
import os
import re
import json
import hashlib
from datetime import datetime
import pandas as pd
from collections import defaultdict, Counter
import tempfile
import io
import zipfile
import tarfile
import gzip
import shutil

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-change-this'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['TEMP_FOLDER'] = 'temp'

# Add custom Jinja2 filter for index information
@app.template_filter('extract_index_info')
def extract_index_info_filter(plan_summary):
    """Template filter to extract index information from plan summary"""
    return _extract_index_info(plan_summary)
# Removed file size limit - can now upload files of any size

# Ensure folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['TEMP_FOLDER'], exist_ok=True)

ALLOWED_EXTENSIONS = {'log', 'txt', 'zip', 'tar', 'gz'}

def paginate_data(data, page=1, per_page=100):
    """
    Paginate a list of data
    
    Args:
        data: List of items to paginate
        page: Current page number (1-based)
        per_page: Number of items per page (100, 300, or 'all')
    
    Returns:
        dict with paginated data and pagination info
    """
    if per_page == 'all' or per_page == -1:
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
            if per_page not in [100, 300]:
                per_page = 100
        except (ValueError, TypeError):
            per_page = 100
    
    return page, per_page

def allowed_file(filename):
    # Check for .tar.gz files specifically
    if filename.lower().endswith('.tar.gz'):
        return True
    
    # Check for date-suffixed log files (e.g., mongodb.log.20250826, app.log.2025-08-26)
    if '.log.' in filename.lower() or '.txt.' in filename.lower():
        return True
    
    # Check standard extensions
    if '.' in filename:
        ext = filename.rsplit('.', 1)[1].lower()
        if ext in ALLOWED_EXTENSIONS:
            return True
        
        # Check if it's a date pattern after log/txt (e.g., .log.20250826)
        parts = filename.lower().split('.')
        if len(parts) >= 3:
            # Check if second-to-last part is 'log' or 'txt'
            if parts[-2] in ['log', 'txt']:
                # Check if last part looks like a date (8 digits or date format)
                last_part = parts[-1]
                if (last_part.isdigit() and len(last_part) == 8) or \
                   (last_part.replace('-', '').replace('_', '').isdigit()):
                    return True
    
    return False

def is_archive_file(filename):
    """Check if file is a compressed archive"""
    filename_lower = filename.lower()
    return (filename_lower.endswith('.zip') or 
            filename_lower.endswith('.tar') or 
            filename_lower.endswith('.tar.gz') or
            filename_lower.endswith('.gz'))

def extract_archive(archive_path, extract_to_dir):
    """Extract archive and return list of extracted log files"""
    extracted_files = []
    
    try:
        # Create extraction directory
        os.makedirs(extract_to_dir, exist_ok=True)
        
        if archive_path.lower().endswith('.zip'):
            # Handle ZIP files
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    # Only extract files that look like log files
                    member_name = member.split('/')[-1]  # Get filename without path
                    if (is_log_file(member_name) and 
                        not member.startswith('__MACOSX/') and 
                        member_name):  # Skip directories and empty names
                        
                        zip_ref.extract(member, extract_to_dir)
                        extracted_path = os.path.join(extract_to_dir, member)
                        if os.path.isfile(extracted_path):
                            extracted_files.append(extracted_path)
        
        elif archive_path.lower().endswith(('.tar', '.tar.gz')):
            # Handle TAR and TAR.GZ files
            mode = 'r:gz' if archive_path.lower().endswith('.tar.gz') else 'r'
            with tarfile.open(archive_path, mode) as tar_ref:
                for member in tar_ref.getmembers():
                    member_name = member.name.split('/')[-1]  # Get filename without path
                    if (member.isfile() and 
                        is_log_file(member_name) and
                        member_name):  # Skip directories and empty names
                        
                        # Extract with safe name (avoid directory traversal)
                        safe_name = os.path.basename(member.name)
                        member.name = safe_name
                        tar_ref.extract(member, extract_to_dir)
                        extracted_path = os.path.join(extract_to_dir, safe_name)
                        if os.path.isfile(extracted_path):
                            extracted_files.append(extracted_path)
        
        elif archive_path.lower().endswith('.gz') and not archive_path.lower().endswith('.tar.gz'):
            # Handle standalone .gz files
            with gzip.open(archive_path, 'rb') as gz_file:
                # Create output filename by removing .gz extension
                base_name = os.path.basename(archive_path)
                if base_name.lower().endswith('.gz'):
                    output_name = base_name[:-3]  # Remove .gz extension
                else:
                    output_name = base_name + '.extracted'
                
                output_path = os.path.join(extract_to_dir, output_name)
                
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
    """Check if file is a log file"""
    filename_lower = filename.lower()
    
    # Standard log files
    if filename_lower.endswith(('.log', '.txt')):
        return True
    
    # Date-suffixed log files (e.g., mongodb.log.20250826)
    if '.log.' in filename_lower or '.txt.' in filename_lower:
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
            elif component == 'ACCESS' and 'Successfully authenticated' in message:
                principal = attr.get('user', attr.get('principalName', ''))
                auth_db = attr.get('db', attr.get('authenticationDatabase', ''))
                mechanism = attr.get('mechanism', 'SCRAM-SHA-256')
                
                self.authentications.append({
                    'timestamp': timestamp,
                    'connection_id': conn_id,
                    'username': principal,
                    'database': auth_db,
                    'mechanism': mechanism,
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
                        # Extract performance metrics if available
                        'docsExamined': (
                            attr.get('docsExamined') or 
                            attr.get('command', {}).get('docsExamined') or 0
                        ),
                        'keysExamined': (
                            attr.get('keysExamined') or 
                            attr.get('command', {}).get('keysExamined') or 0
                        ),
                        'nReturned': (
                            attr.get('nReturned') or 
                            attr.get('command', {}).get('nReturned') or 0
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
                            # Extract performance metrics if available
                            'docsExamined': (
                                attr.get('docsExamined') or 
                                attr.get('command', {}).get('docsExamined') or 0
                            ),
                            'keysExamined': (
                                attr.get('keysExamined') or 
                                attr.get('command', {}).get('keysExamined') or 0
                            ),
                            'nReturned': (
                                attr.get('nReturned') or 
                                attr.get('command', {}).get('nReturned') or 0
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
            
            self.authentications.append({
                'timestamp': timestamp,
                'connection_id': conn_id,
                'username': username,
                'database': database,
                'mechanism': mechanism,
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
            
            self.authentications.append({
                'timestamp': timestamp,
                'connection_id': conn_id,
                'username': username,
                'database': database,
                'mechanism': mechanism,
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
    
    def search_logs(self, keyword=None, field_name=None, field_value=None, use_regex=False, start_date=None, end_date=None, limit=100):
        """Search through log entries with various criteria"""
        import re
        results = []
        
        for file_path, raw_lines in self.raw_log_data.items():
            for line_num, line in enumerate(raw_lines, 1):
                try:
                    # Parse JSON to get timestamp for date filtering
                    log_entry = json.loads(line.strip())
                    timestamp = self.extract_timestamp_from_json(log_entry.get('t', {}))
                    
                    # Apply date filters
                    if start_date and timestamp < start_date:
                        continue
                    if end_date and timestamp > end_date:
                        continue
                    
                    # Apply search criteria
                    match = True
                    
                    if keyword:
                        # Simple keyword search in entire line
                        if use_regex:
                            if not re.search(keyword, line, re.IGNORECASE):
                                match = False
                        else:
                            if keyword.lower() not in line.lower():
                                match = False
                    
                    if field_name and field_value and match:
                        # Field-specific search
                        field_data = self.get_nested_field(log_entry, field_name)
                        if field_data is None:
                            match = False
                        else:
                            field_str = str(field_data)
                            if use_regex:
                                if not re.search(field_value, field_str, re.IGNORECASE):
                                    match = False
                            else:
                                if field_value.lower() not in field_str.lower():
                                    match = False
                    
                    if match:
                        results.append({
                            'file_path': file_path,
                            'line_number': line_num,
                            'timestamp': timestamp,
                            'raw_line': line.strip(),
                            'parsed_json': log_entry
                        })
                        
                        if len(results) >= limit:
                            break
                            
                except (json.JSONDecodeError, Exception):
                    # Try to extract timestamp from malformed JSON
                    timestamp = None
                    # Try different timestamp patterns
                    timestamp_patterns = [
                        r'"t":\{"\\$date":"([^"]+)"\}',  # Escaped format
                        r'"t":\{"\$date":"([^"]+)"\}',   # Standard format
                        r'"t":\{"[$]date":"([^"]+)"\}'   # Alternative format
                    ]
                    
                    for pattern in timestamp_patterns:
                        timestamp_match = re.search(pattern, line)
                        if timestamp_match:
                            try:
                                timestamp_str = timestamp_match.group(1)
                                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                break
                            except:
                                continue
                    
                    # Apply date filters even for malformed JSON if we have timestamp
                    if timestamp:
                        if start_date and timestamp < start_date:
                            continue
                        if end_date and timestamp > end_date:
                            continue
                    
                    # For non-JSON lines, only do keyword search
                    if keyword:
                        if use_regex:
                            if re.search(keyword, line, re.IGNORECASE):
                                results.append({
                                    'file_path': file_path,
                                    'line_number': line_num,
                                    'timestamp': timestamp,
                                    'raw_line': line.strip(),
                                    'parsed_json': None
                                })
                        else:
                            if keyword.lower() in line.lower():
                                results.append({
                                    'file_path': file_path,
                                    'line_number': line_num,
                                    'timestamp': timestamp,
                                    'raw_line': line.strip(),
                                    'parsed_json': None
                                })
                                
                        if len(results) >= limit:
                            break
            
            if len(results) >= limit:
                break
        
        # Sort by timestamp if available, otherwise by file and line number
        results.sort(key=lambda x: (x['timestamp'] or datetime.min, x['file_path'], x['line_number']), reverse=True)
        
        return results
    
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
        """Analyze COLLSCAN queries and generate index suggestions for high-accuracy scenarios"""
        import re
        from collections import defaultdict
        
        suggestions = defaultdict(lambda: {
            'collection_name': '',
            'suggestions': [],
            'collscan_queries': 0,
            'total_docs_examined': 0,
            'avg_duration': 0,
            'sample_queries': []
        })
        
        # Analyze each slow query
        for query in self.slow_queries:
            if query.get('plan_summary') != 'COLLSCAN':
                continue
                
            db_collection = f"{query.get('database', 'unknown')}.{query.get('collection', 'unknown')}"
            suggestions[db_collection]['collection_name'] = db_collection
            suggestions[db_collection]['collscan_queries'] += 1
            suggestions[db_collection]['total_docs_examined'] += self._get_docs_examined(query)
            suggestions[db_collection]['avg_duration'] += query.get('duration', 0)
            
            # Store sample query for analysis
            if len(suggestions[db_collection]['sample_queries']) < 3:
                suggestions[db_collection]['sample_queries'].append({
                    'query': query.get('query', ''),
                    'duration': query.get('duration', 0),
                    'timestamp': query.get('timestamp')
                })
            
            # Parse query for index suggestions
            query_str = query.get('query', '')
            parsed_suggestions = self._extract_index_suggestions(query_str, query.get('collection', 'unknown'))
            
            # Add unique suggestions
            for suggestion in parsed_suggestions:
                if suggestion not in suggestions[db_collection]['suggestions']:
                    suggestions[db_collection]['suggestions'].append(suggestion)
        
        # Calculate averages and finalize
        for collection, data in suggestions.items():
            if data['collscan_queries'] > 0:
                data['avg_duration'] = data['avg_duration'] / data['collscan_queries']
                data['avg_docs_per_query'] = data['total_docs_examined'] / data['collscan_queries']
        
        return dict(suggestions)
    
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
            'avg_selectivity': 0,
            'avg_index_efficiency': 0,
            'sample_query': '',
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
                'timestamp': query.get('timestamp')
            }
            
            pattern['executions'].append(execution)
            pattern['total_count'] += 1
            pattern['last_seen'] = query.get('timestamp')
        
        # Calculate statistics for each pattern
        for pattern_key, pattern in patterns.items():
            if pattern['total_count'] == 0:
                continue
                
            durations = [exec['duration'] for exec in pattern['executions']]
            docs_examined = [exec['docs_examined'] for exec in pattern['executions']]
            keys_examined = [exec['keys_examined'] for exec in pattern['executions']]
            returned = [exec['returned'] for exec in pattern['executions']]
            
            # Duration statistics
            pattern['avg_duration'] = statistics.mean(durations)
            pattern['min_duration'] = min(durations)
            pattern['max_duration'] = max(durations)
            pattern['median_duration'] = statistics.median(durations)
            
            # Efficiency metrics
            pattern['total_docs_examined'] = sum(docs_examined)
            pattern['total_keys_examined'] = sum(keys_examined) 
            pattern['total_returned'] = sum(returned)
            
            # Calculate selectivity (docs returned / docs examined)
            if pattern['total_docs_examined'] > 0:
                pattern['avg_selectivity'] = (pattern['total_returned'] / pattern['total_docs_examined']) * 100
            
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
                            for stage in query_obj['pipeline']:
                                if isinstance(stage, dict):
                                    for op in stage.keys():
                                        if op.startswith('$'):
                                            pipeline_ops.append(op)
                            if pipeline_ops:
                                normalized_parts.append(f"pipeline:{','.join(pipeline_ops)}")
                    
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
        if 'docsExamined' in query or 'nReturned' in query or 'keysExamined' in query:
            return {
                'docsExamined': query.get('docsExamined', 0),
                'keysExamined': query.get('keysExamined', 0), 
                'nReturned': query.get('nReturned', 0),
                'cpuNanos': query.get('cpuNanos', 0),
                'planCacheKey': query.get('planCacheKey', ''),
                'queryFramework': query.get('queryFramework', ''),
                'readConcern': query.get('readConcern', {}),
                'writeConcern': query.get('writeConcern', {})
            }
        
        # Try to extract from original log line
        try:
            original_line = self.get_original_log_line(query.get('file_path'), query.get('line_number'))
            if original_line:
                original_json = json.loads(original_line)
                attr = original_json.get('attr', {})
                
                # Extract metrics with multiple possible field names
                docs_examined = (
                    attr.get('docsExamined') or 
                    attr.get('totalDocsExamined') or 
                    attr.get('executionStats', {}).get('docsExamined') or
                    attr.get('command', {}).get('docsExamined') or 0
                )
                
                keys_examined = (
                    attr.get('keysExamined') or 
                    attr.get('totalKeysExamined') or 
                    attr.get('executionStats', {}).get('keysExamined') or
                    attr.get('command', {}).get('keysExamined') or 0
                )
                
                n_returned = (
                    attr.get('nReturned') or 
                    attr.get('nreturned') or 
                    attr.get('numReturned') or 
                    attr.get('executionStats', {}).get('nReturned') or
                    attr.get('command', {}).get('nReturned') or 0
                )
                
                return {
                    'docsExamined': docs_examined,
                    'keysExamined': keys_examined,
                    'nReturned': n_returned,
                    'cpuNanos': attr.get('cpuNanos', 0),
                    'planCacheKey': attr.get('planCacheKey', ''),
                    'queryFramework': attr.get('queryFramework', ''),
                    'readConcern': attr.get('readConcern', {}),
                    'writeConcern': attr.get('writeConcern', {})
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
        duration_range = pattern['max_duration'] - pattern['min_duration']
        if duration_range > pattern['avg_duration'] * 0.5:
            score += 2
        
        # COLLSCAN is always high optimization potential
        if pattern['plan_summary'] == 'COLLSCAN':
            score += 3
        
        # High docs examined vs returned ratio
        if pattern['avg_selectivity'] < 10:  # Less than 10% selectivity
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

analyzer = MongoLogAnalyzer()

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'files' not in request.files:
        flash('No files selected')
        return redirect(request.url)
    
    files = request.files.getlist('files')
    if not files or files[0].filename == '':
        flash('No files selected')
        return redirect(url_for('index'))
    
    # Clean up temp folder before processing new uploads
    cleanup_temp_folder()
    
    temp_files_created = []
    extracted_files = []
    extraction_dir = os.path.join(app.config['TEMP_FOLDER'], f'extracted_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    
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
                    # Extract archive files to temp directory
                    extracted = extract_archive(temp_filepath, extraction_dir)
                    if extracted:
                        extracted_files.extend(extracted)
                        flash(f'Extracted {len(extracted)} log file(s) from {original_filename}')
                    else:
                        flash(f'No valid log files found in archive {original_filename}', 'warning')
                elif is_log_file(original_filename):
                    # Regular log file - keep temp path for processing
                    pass
                else:
                    flash(f'Invalid file type for {file.filename}. Please upload .log, .txt, .zip, .tar, or .tar.gz files only.')
                    return redirect(url_for('index'))
            else:
                flash(f'Invalid file type for {file.filename}. Please upload .log, .txt, .zip, .tar, or .tar.gz files only.')
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
            analyzer = MongoLogAnalyzer()
            
            # Analyze all files
            processed_count = 0
            for filepath in log_files_to_analyze:
                if os.path.exists(filepath) and is_log_file(filepath):
                    analyzer.parse_log_file(filepath)
                    processed_count += 1
            
            # Generate parsing summary messages
            summary_messages = analyzer.get_parsing_summary_message()
            for message in summary_messages:
                flash(message)
            
            return redirect(url_for('dashboard'))
        else:
            flash('No valid log files to process.')
            return redirect(url_for('index'))
            
    finally:
        # Cleanup happens automatically on next upload via cleanup_temp_folder()
        # But we could also clean up immediately if needed
        pass

@app.route('/dashboard')
def dashboard():
    # Get available date range from log data
    min_date, max_date = analyzer.get_available_date_range()
    
    # Get filter parameters
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    ip_filter = request.args.get('ip_filter', '').strip()
    user_filter = request.args.get('user_filter', '').strip()
    
    # Parse date filters
    start_date = None
    end_date = None
    if start_date_str:
        try:
            start_date = datetime.fromisoformat(start_date_str)
        except ValueError:
            pass
    if end_date_str:
        try:
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            pass
    
    # Get pagination parameters
    page, per_page = get_pagination_params(request)
    
    # Get stats with filters applied
    stats = analyzer.get_connection_stats(
        start_date=start_date,
        end_date=end_date,
        ip_filter=ip_filter,
        user_filter=user_filter
    )
    
    # Paginate database access data
    database_access_pagination = None
    if hasattr(stats, 'database_access') and stats.database_access:
        database_access_pagination = paginate_data(stats.database_access, page, per_page)
    
    # Paginate connections timeline
    connections_timeline_pagination = None
    if hasattr(stats, 'connections_timeline') and stats.connections_timeline:
        connections_timeline_pagination = paginate_data(stats.connections_timeline, page, per_page)
    
    return render_template('dashboard_results.html', 
                         stats=stats,
                         database_access_pagination=database_access_pagination,
                         connections_timeline_pagination=connections_timeline_pagination,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         min_date=min_date.strftime('%Y-%m-%dT%H:%M') if min_date else None,
                         max_date=max_date.strftime('%Y-%m-%dT%H:%M') if max_date else None,
                         ip_filter=ip_filter,
                         user_filter=user_filter)

@app.route('/api/stats')
def api_stats():
    stats = analyzer.get_connection_stats()
    return jsonify(stats)

@app.route('/slow-queries')
def slow_queries():
    # Ensure user correlation is done before filtering
    analyzer.correlate_users_with_access()
    
    # Ensure all queries have query_hash (generate synthetic if missing)
    for query in analyzer.slow_queries:
        if not query.get('query_hash'):
            query['query_hash'] = analyzer._generate_synthetic_query_hash(query)
    
    # Get unique databases for filtering
    databases = set()
    for query in analyzer.slow_queries:
        if query.get('database') and query['database'] != 'unknown':
            databases.add(query['database'])
    
    # Get available date range from log data
    min_date, max_date = analyzer.get_available_date_range()
    
    # Get filter parameters
    selected_db = request.args.get('database', 'all')
    threshold = int(request.args.get('threshold', 100))
    selected_plan = request.args.get('plan_summary', 'all')
    view_mode = request.args.get('view_mode', 'all_executions')  # 'all_executions' or 'unique_queries'
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    
    # Parse date filters
    start_date = None
    end_date = None
    if start_date_str:
        try:
            start_date = datetime.fromisoformat(start_date_str)
        except ValueError:
            pass
    if end_date_str:
        try:
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            pass
    
    # Filter slow queries
    filtered_queries = []
    for query in analyzer.slow_queries:
        # Duration filter
        if query['duration'] < threshold:
            continue
        
        # Database filter
        if selected_db != 'all' and query.get('database') != selected_db:
            continue
            
        # Date filters
        if start_date and query['timestamp'] < start_date:
            continue
        if end_date and query['timestamp'] > end_date:
            continue
        
        # Plan summary filter
        if selected_plan != 'all':
            query_plan = query.get('plan_summary', 'None')
            if selected_plan == 'COLLSCAN' and query_plan != 'COLLSCAN':
                continue
            elif selected_plan == 'IXSCAN' and query_plan != 'IXSCAN':
                continue
            elif selected_plan == 'other' and query_plan in ['COLLSCAN', 'IXSCAN']:
                continue
            
        filtered_queries.append(query)
    
    # Get pagination parameters
    page, per_page = get_pagination_params(request)
    
    # Process data based on view mode
    if view_mode == 'unique_queries':
        # Group queries by pattern
        unique_queries = analyzer._group_queries_by_pattern(filtered_queries)
        display_data = unique_queries
        total_count = len(unique_queries)
    else:
        # All executions mode (default)
        filtered_queries.sort(key=lambda x: x['duration'], reverse=True)
        display_data = filtered_queries
        total_count = len(filtered_queries)
    
    # Apply pagination
    pagination = paginate_data(display_data, page, per_page)
    
    return render_template('slow_queries.html', 
                         queries=pagination['items'], 
                         pagination=pagination,
                         databases=sorted(databases),
                         selected_db=selected_db,
                         threshold=threshold,
                         selected_plan=selected_plan,
                         view_mode=view_mode,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         min_date=min_date.strftime('%Y-%m-%dT%H:%M') if min_date else None,
                         max_date=max_date.strftime('%Y-%m-%dT%H:%M') if max_date else None,
                         total_queries=total_count)

@app.route('/export-slow-queries')
def export_slow_queries():
    # Ensure user correlation is done before filtering
    analyzer.correlate_users_with_access()
    
    # Get available date range from log data for validation
    min_date, max_date = analyzer.get_available_date_range()
    
    # Get filter parameters
    selected_db = request.args.get('database', 'all')
    threshold = int(request.args.get('threshold', 100))
    selected_plan = request.args.get('plan_summary', 'all')
    view_mode = request.args.get('view_mode', 'all_executions')
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    
    # Parse date filters
    start_date = None
    end_date = None
    if start_date_str:
        try:
            start_date = datetime.fromisoformat(start_date_str)
        except ValueError:
            pass
    if end_date_str:
        try:
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            pass
    
    # Filter slow queries
    filtered_queries = []
    for query in analyzer.slow_queries:
        # Duration filter
        if query['duration'] < threshold:
            continue
        
        # Database filter
        if selected_db != 'all' and query.get('database') != selected_db:
            continue
            
        # Date filters
        if start_date and query['timestamp'] < start_date:
            continue
        if end_date and query['timestamp'] > end_date:
            continue
        
        # Plan summary filter
        if selected_plan != 'all':
            query_plan = query.get('plan_summary', 'None')
            if selected_plan == 'COLLSCAN' and query_plan != 'COLLSCAN':
                continue
            elif selected_plan == 'IXSCAN' and query_plan != 'IXSCAN':
                continue
            elif selected_plan == 'other' and query_plan in ['COLLSCAN', 'IXSCAN']:
                continue
            
        filtered_queries.append(query)
    
    # Sort by duration (slowest first)
    filtered_queries.sort(key=lambda x: x['duration'], reverse=True)
    
    # Handle export based on view mode
    if view_mode == 'unique_queries':
        # Export unique patterns with all executions
        unique_patterns = analyzer._group_queries_by_pattern(filtered_queries)
        
        export_data = {
            'export_info': {
                'mode': 'unique_patterns',
                'total_patterns': len(unique_patterns),
                'total_executions': len(filtered_queries),
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
            # Find the slowest execution for this pattern
            slowest_execution = None
            max_duration = pattern['max_duration']
            for execution in pattern['executions']:
                if execution.get('duration', 0) == max_duration:
                    if slowest_execution is None or (execution.get('timestamp') and execution.get('timestamp') > slowest_execution.get('timestamp', datetime.min)):
                        slowest_execution = execution
            
            # Get original log entry for the slowest execution only
            slowest_original_entry = None
            if slowest_execution:
                original_line = analyzer.get_original_log_line(slowest_execution.get('file_path'), slowest_execution.get('line_number'))
                if original_line:
                    try:
                        slowest_original_entry = json.loads(original_line)
                    except json.JSONDecodeError:
                        slowest_original_entry = {
                            'timestamp': slowest_execution['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                            'duration_ms': slowest_execution['duration'],
                            'database': slowest_execution['database'],
                            'collection': slowest_execution['collection'],
                            'query': slowest_execution['query'],
                            'plan_summary': slowest_execution.get('plan_summary', 'None'),
                            'note': 'Processed data - original log entry was malformed'
                        }
                else:
                    slowest_original_entry = {
                        'timestamp': slowest_execution['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                        'duration_ms': slowest_execution['duration'],
                        'database': slowest_execution['database'],
                        'collection': slowest_execution['collection'],
                        'query': slowest_execution['query'],
                        'plan_summary': slowest_execution.get('plan_summary', 'None'),
                        'note': 'Processed data - original log entry not found'
                    }
            
            pattern_export = {
                'pattern_summary': {
                    'query_hash': pattern['query_hash'],
                    'database': pattern['database'],
                    'collection': pattern['collection'],
                    'plan_summary': pattern['plan_summary'],
                    'execution_count': pattern['total_count'],
                    'avg_duration_ms': round(pattern['avg_duration'], 2),
                    'min_duration_ms': pattern['min_duration'],
                    'max_duration_ms': pattern['max_duration'],
                    'first_seen': pattern['first_seen'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if pattern['first_seen'] else None,
                    'last_seen': pattern['last_seen'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if pattern['last_seen'] else None,
                    'sample_query': pattern['sample_query']
                },
                'slowest_execution': slowest_original_entry
            }
            export_data['patterns'].append(pattern_export)
        
        export_content = json.dumps(export_data, indent=2, default=str)
        filename_suffix = 'unique_patterns'
        
    else:
        # Export all executions (current behavior)
        original_log_entries = []
        for query in filtered_queries:
            original_line = analyzer.get_original_log_line(query.get('file_path'), query.get('line_number'))
            if original_line:
                try:
                    original_log_entries.append(json.loads(original_line))
                except json.JSONDecodeError:
                    original_log_entries.append({
                        'timestamp': query['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                        'connection_id': query['connection_id'],
                        'duration_ms': query['duration'],
                        'database': query['database'],
                        'collection': query['collection'],
                        'query': query['query'],
                        'plan_summary': query.get('plan_summary', 'None'),
                        'username': query.get('username', 'Unknown'),
                        'note': 'Processed data - original log entry was malformed'
                    })
            else:
                original_log_entries.append({
                    'timestamp': query['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                    'connection_id': query['connection_id'],
                    'duration_ms': query['duration'],
                    'database': query['database'],
                    'collection': query['collection'],
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
    
    response = Response(export_content, mimetype='application/json')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    
    return response

@app.route('/search-logs')
def search_logs():
    # Get available date range from log data
    min_date, max_date = analyzer.get_available_date_range()
    
    # Get search parameters
    keyword = request.args.get('keyword', '').strip()
    field_name = request.args.get('field_name', '').strip()
    field_value = request.args.get('field_value', '').strip()
    use_regex = request.args.get('use_regex') == 'on'
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    limit = min(int(request.args.get('limit', 100)), 1000)  # Cap at 1000 results
    
    # Parse date filters
    start_date = None
    end_date = None
    if start_date_str:
        try:
            start_date = datetime.fromisoformat(start_date_str)
        except ValueError:
            pass
    if end_date_str:
        try:
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            pass
    
    # Get pagination parameters
    page, per_page = get_pagination_params(request)
    
    # Perform search
    results = []
    search_performed = False
    error_message = None
    pagination = None
    
    if keyword or (field_name and field_value):
        search_performed = True
        try:
            # Get more results for pagination (up to 10,000 instead of using limit)
            search_limit = 10000 if per_page == 'all' else max(per_page * 10 if per_page != 'all' else 1000, 1000)
            results = analyzer.search_logs(
                keyword=keyword or None,
                field_name=field_name or None,
                field_value=field_value or None,
                use_regex=use_regex,
                start_date=start_date,
                end_date=end_date,
                limit=search_limit
            )
            
            # Apply pagination to results
            pagination = paginate_data(results, page, per_page)
            results = pagination['items']
            
        except Exception as e:
            error_message = f"Search error: {str(e)}"
    
    return render_template('search_logs.html',
                         keyword=keyword,
                         field_name=field_name,
                         field_value=field_value,
                         use_regex=use_regex,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         min_date=min_date.strftime('%Y-%m-%dT%H:%M') if min_date else None,
                         max_date=max_date.strftime('%Y-%m-%dT%H:%M') if max_date else None,
                         limit=limit,
                         results=results,
                         pagination=pagination,
                         search_performed=search_performed,
                         error_message=error_message,
                         result_count=pagination['total'] if pagination else len(results))

@app.route('/export-search-results')
def export_search_results():
    # Get the same search parameters
    keyword = request.args.get('keyword', '').strip()
    field_name = request.args.get('field_name', '').strip()
    field_value = request.args.get('field_value', '').strip()
    use_regex = request.args.get('use_regex') == 'on'
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    limit = min(int(request.args.get('limit', 1000)), 1000)
    
    # Parse date filters
    start_date = None
    end_date = None
    if start_date_str:
        try:
            start_date = datetime.fromisoformat(start_date_str)
        except ValueError:
            pass
    if end_date_str:
        try:
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            pass
    
    # Perform search
    results = analyzer.search_logs(
        keyword=keyword or None,
        field_name=field_name or None,
        field_value=field_value or None,
        use_regex=use_regex,
        start_date=start_date,
        end_date=end_date,
        limit=limit
    )
    
    # Create export data with only original JSON entries
    original_log_entries = []
    
    for result in results:
        if result['parsed_json']:
            original_log_entries.append(result['parsed_json'])
        else:
            # If parsing failed, try to parse the raw line again
            try:
                original_json = json.loads(result['raw_line'])
                original_log_entries.append(original_json)
            except json.JSONDecodeError:
                # Skip invalid JSON entries
                continue
    
    export_content = json.dumps(original_log_entries, indent=2, default=str)
    
    # Generate filename
    search_term = keyword or f"{field_name}_{field_value}" or "search"
    filename = f"mongodb_search_{search_term.replace(' ', '_').replace(':', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    response = Response(export_content, mimetype='application/json')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    
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
    
    return render_template('index_suggestions.html',
                         suggestions=suggestions,
                         total_collscan_queries=total_collscan_queries,
                         total_suggestions=total_suggestions,
                         avg_docs_examined=avg_docs_examined)

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
            'collscan_queries': data['collscan_queries'],
            'avg_duration_ms': round(data['avg_duration']),
            'commands': []
        }
        
        for suggestion in data['suggestions']:
            collection_commands['commands'].append({
                'priority': suggestion['priority'],
                'reason': suggestion['reason'],
                'command': suggestion['command']
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
    
    # Parse date filters
    start_date = None
    end_date = None
    if start_date_str:
        try:
            start_date = datetime.fromisoformat(start_date_str)
        except ValueError:
            pass
    if end_date_str:
        try:
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            pass
    
    # Filter slow queries by date range before analysis
    filtered_queries = []
    for query in analyzer.slow_queries:
        # Date filters
        if start_date and query['timestamp'] < start_date:
            continue
        if end_date and query['timestamp'] > end_date:
            continue
        filtered_queries.append(query)
    
    # Temporarily replace slow_queries for analysis
    original_queries = analyzer.slow_queries
    analyzer.slow_queries = filtered_queries
    
    # Get pagination parameters
    page, per_page = get_pagination_params(request)
    
    # Analyze query patterns with filtered data
    patterns = analyzer.analyze_query_patterns()
    
    # Restore original queries
    analyzer.slow_queries = original_queries
    
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
    patterns_list = list(patterns.items())
    
    # Apply pagination
    pagination = paginate_data(patterns_list, page, per_page)
    
    return render_template('slow_query_analysis.html',
                         patterns=dict(pagination['items']) if pagination['items'] else {},
                         pagination=pagination,
                         total_executions=total_executions,
                         avg_duration=avg_duration,
                         high_priority_count=high_priority_count,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         min_date=min_date.strftime('%Y-%m-%dT%H:%M') if min_date else None,
                         max_date=max_date.strftime('%Y-%m-%dT%H:%M') if max_date else None)

@app.route('/export-query-analysis')
def export_query_analysis():
    # Ensure user correlation is done before analysis
    analyzer.correlate_users_with_access()
    
    # Analyze query patterns
    patterns = analyzer.analyze_query_patterns()
    
    # Create export data
    export_data = {
        'generated_on': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_patterns': len(patterns),
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
                'max_duration_ms': round(pattern['max_duration']),
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
            
            # Parse and analyze the currentOp data
            analysis = analyze_current_op(current_op_data)
            
            return render_template('current_op_analyzer.html', 
                                 analysis=analysis, 
                                 original_data=original_data)
                                 
        except Exception as e:
            flash(f'Error analyzing currentOp data: {str(e)}', 'error')
            return render_template('current_op_analyzer.html')
    
    return render_template('current_op_analyzer.html')

def analyze_current_op(current_op_data):
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
                
                # Long running operations (>30 seconds)
                if duration > 30:
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

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)  # Listen only on localhost