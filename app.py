from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, Response
from werkzeug.utils import secure_filename
import os
import re
import json
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
# Removed file size limit - can now upload files of any size

# Ensure folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['TEMP_FOLDER'], exist_ok=True)

ALLOWED_EXTENSIONS = {'log', 'txt', 'zip', 'tar', 'gz'}

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
                    plan_summary = attr.get('planSummary', 'None')
                    self.slow_queries.append({
                        'timestamp': timestamp,
                        'connection_id': conn_id,
                        'duration': duration,
                        'database': database,
                        'collection': collection,
                        'query': json.dumps(command_info, indent=None),
                        'plan_summary': plan_summary,
                        'username': None,  # Will be filled by correlation
                        'file_path': filepath,
                        'line_number': line_num
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
                        self.slow_queries.append({
                            'timestamp': timestamp,
                            'connection_id': conn_id,
                            'duration': duration,
                            'database': database,
                            'collection': collection,
                            'query': json.dumps(query_data, indent=None),
                            'plan_summary': plan_summary,
                            'username': None,  # Will be filled by correlation
                            'file_path': filepath,
                            'line_number': line_num
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
                        'query': line
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
    
    # Get stats with filters applied
    stats = analyzer.get_connection_stats(
        start_date=start_date,
        end_date=end_date,
        ip_filter=ip_filter,
        user_filter=user_filter
    )
    
    return render_template('dashboard_results.html', 
                         stats=stats,
                         start_date=start_date_str,
                         end_date=end_date_str,
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
    
    # Get unique databases for filtering
    databases = set()
    for query in analyzer.slow_queries:
        if query.get('database') and query['database'] != 'unknown':
            databases.add(query['database'])
    
    # Get filter parameters
    selected_db = request.args.get('database', 'all')
    threshold = int(request.args.get('threshold', 100))
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
            
        filtered_queries.append(query)
    
    # Sort by duration (descending)
    filtered_queries.sort(key=lambda x: x['duration'], reverse=True)
    
    return render_template('slow_queries.html', 
                         queries=filtered_queries, 
                         databases=sorted(databases),
                         selected_db=selected_db,
                         threshold=threshold,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         total_queries=len(filtered_queries))

@app.route('/export-slow-queries')
def export_slow_queries():
    # Ensure user correlation is done before filtering
    analyzer.correlate_users_with_access()
    
    # Get filter parameters
    selected_db = request.args.get('database', 'all')
    threshold = int(request.args.get('threshold', 100))
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
            
        filtered_queries.append(query)
    
    # Sort by duration (slowest first)
    filtered_queries.sort(key=lambda x: x['duration'], reverse=True)
    
    # Generate export content with original JSON entries
    original_log_entries = []
    for query in filtered_queries:
        # Get the original raw log line
        original_line = analyzer.get_original_log_line(query.get('file_path'), query.get('line_number'))
        if original_line:
            try:
                # Parse the original line to ensure it's valid JSON
                original_json = json.loads(original_line)
                original_log_entries.append(original_json)
            except json.JSONDecodeError:
                # Fallback to processed data if original line is invalid
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
            # Fallback if we can't find the original line
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
    
    # Create JSON export with only original log entries
    export_content = json.dumps(original_log_entries, indent=2, default=str)
    
    # Generate filename
    db_suffix = f"_{selected_db}" if selected_db != 'all' else "_all"
    filename = f"mongodb_slow_queries{db_suffix}_{threshold}ms_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    response = Response(export_content, mimetype='application/json')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    
    return response

@app.route('/search-logs')
def search_logs():
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
    
    # Perform search
    results = []
    search_performed = False
    error_message = None
    
    if keyword or (field_name and field_value):
        search_performed = True
        try:
            results = analyzer.search_logs(
                keyword=keyword or None,
                field_name=field_name or None,
                field_value=field_value or None,
                use_regex=use_regex,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )
        except Exception as e:
            error_message = f"Search error: {str(e)}"
    
    return render_template('search_logs.html',
                         keyword=keyword,
                         field_name=field_name,
                         field_value=field_value,
                         use_regex=use_regex,
                         start_date=start_date_str,
                         end_date=end_date_str,
                         limit=limit,
                         results=results,
                         search_performed=search_performed,
                         error_message=error_message,
                         result_count=len(results))

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

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)  # Listen only on localhost