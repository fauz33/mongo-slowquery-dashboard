#!/bin/bash

# MongoDB CurrentOp Collection Script
# This script connects to MongoDB and collects db.currentOp() output for analysis

# Default configuration
DEFAULT_HOST="localhost"
DEFAULT_PORT="27017"
DEFAULT_DATABASE="admin"
DEFAULT_OUTPUT_DIR="./currentop_data"
DEFAULT_FILENAME_PREFIX="currentop"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display usage
show_help() {
    echo -e "${BLUE}MongoDB CurrentOp Collection Script${NC}"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  -h, --host HOST          MongoDB host (default: $DEFAULT_HOST)"
    echo "  -p, --port PORT          MongoDB port (default: $DEFAULT_PORT)"
    echo "  -d, --database DB        Database name (default: $DEFAULT_DATABASE)"
    echo "  -u, --username USER      MongoDB username"
    echo "  -P, --password PASS      MongoDB password"
    echo "  -a, --auth-db AUTH_DB    Authentication database"
    echo "  -o, --output-dir DIR     Output directory (default: $DEFAULT_OUTPUT_DIR)"
    echo "  -f, --filename PREFIX    Output filename prefix (default: $DEFAULT_FILENAME_PREFIX)"
    echo "  -c, --connection-string URI  MongoDB connection string (overrides other connection options)"
    echo "  -q, --query QUERY        Custom currentOp query (default: all operations)"
    echo "  -i, --interval SECONDS   Collect every N seconds (continuous mode)"
    echo "  -n, --count NUM          Number of collections in continuous mode (default: unlimited)"
    echo "  -t, --timeout SECONDS    Connection timeout (default: 10)"
    echo "  -s, --ssl                Use SSL/TLS connection"
    echo "  -v, --verbose            Verbose output"
    echo "  --help                   Show this help message"
    echo
    echo "Examples:"
    echo "  # Basic collection"
    echo "  $0"
    echo
    echo "  # Collect from remote MongoDB with authentication"
    echo "  $0 -h mongo.example.com -p 27017 -u admin -P password -a admin"
    echo
    echo "  # Use connection string"
    echo "  $0 -c 'mongodb://user:pass@host:27017/dbname?authSource=admin'"
    echo
    echo "  # Collect only active operations"
    echo "  $0 -q '{\"active\": true}'"
    echo
    echo "  # Collect long-running operations"
    echo "  $0 -q '{\"secs_running\": {\"\$gte\": 30}}'"
    echo
    echo "  # Continuous collection every 30 seconds"
    echo "  $0 -i 30 -n 10"
    echo
    echo "Queries:"
    echo "  All operations:           {} (default)"
    echo "  Active operations:        {\"active\": true}"
    echo "  Long-running (>30s):      {\"secs_running\": {\"\$gte\": 30}}"
    echo "  Specific database:        {\"ns\": /^mydb\\./}"
    echo "  Waiting for locks:        {\"waitingForLock\": true}"
}

# Function to log messages
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $level in
        "INFO")
            echo -e "${GREEN}[$timestamp] INFO:${NC} $message"
            ;;
        "WARN")
            echo -e "${YELLOW}[$timestamp] WARN:${NC} $message"
            ;;
        "ERROR")
            echo -e "${RED}[$timestamp] ERROR:${NC} $message"
            ;;
        "DEBUG")
            if [[ $VERBOSE == true ]]; then
                echo -e "${BLUE}[$timestamp] DEBUG:${NC} $message"
            fi
            ;;
    esac
}

# Function to check if mongosh or mongo is available
check_mongo_client() {
    if command -v mongosh &> /dev/null; then
        MONGO_CLIENT="mongosh"
        log_message "DEBUG" "Using mongosh client"
    elif command -v mongo &> /dev/null; then
        MONGO_CLIENT="mongo"
        log_message "DEBUG" "Using legacy mongo client"
    else
        log_message "ERROR" "Neither mongosh nor mongo client found. Please install MongoDB shell."
        exit 1
    fi
}

# Function to build MongoDB connection string
build_connection() {
    if [[ -n $CONNECTION_STRING ]]; then
        MONGO_CONN="$CONNECTION_STRING"
        log_message "DEBUG" "Using provided connection string"
    else
        MONGO_CONN="mongodb://"
        
        if [[ -n $USERNAME && -n $PASSWORD ]]; then
            MONGO_CONN="${MONGO_CONN}${USERNAME}:${PASSWORD}@"
        fi
        
        MONGO_CONN="${MONGO_CONN}${HOST}:${PORT}/${DATABASE}"
        
        # Add authentication source if provided
        if [[ -n $AUTH_DB ]]; then
            MONGO_CONN="${MONGO_CONN}?authSource=${AUTH_DB}"
        fi
        
        # Add SSL if requested
        if [[ $USE_SSL == true ]]; then
            if [[ $MONGO_CONN == *"?"* ]]; then
                MONGO_CONN="${MONGO_CONN}&ssl=true"
            else
                MONGO_CONN="${MONGO_CONN}?ssl=true"
            fi
        fi
    fi
    
    log_message "DEBUG" "Connection string built (credentials hidden for security)"
}

# Function to test MongoDB connection
test_connection() {
    log_message "INFO" "Testing MongoDB connection..."
    
    local test_command
    if [[ $MONGO_CLIENT == "mongosh" ]]; then
        test_command="db.runCommand({ping: 1})"
    else
        test_command="db.runCommand({ping: 1})"
    fi
    
    local result
    result=$($MONGO_CLIENT "$MONGO_CONN" --eval "$test_command" --quiet 2>&1)
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]] && [[ $result == *"ok"* ]]; then
        log_message "INFO" "MongoDB connection successful"
        return 0
    else
        log_message "ERROR" "MongoDB connection failed: $result"
        return 1
    fi
}

# Function to collect currentOp data
collect_currentop() {
    local output_file=$1
    local collection_num=$2
    
    log_message "INFO" "Collecting currentOp data${collection_num:+ (collection #$collection_num)}..."
    
    # Build the MongoDB command
    local mongo_command="printjson(db.currentOp($QUERY))"
    
    log_message "DEBUG" "Executing: db.currentOp($QUERY)"
    
    # Execute the command and capture output
    local result
    result=$($MONGO_CLIENT "$MONGO_CONN" --eval "$mongo_command" --quiet 2>&1)
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        # Clean up the output (remove MongoDB shell messages if any)
        echo "$result" | sed '/^MongoDB shell version/d' | sed '/^connecting to/d' > "$output_file"
        
        # Verify the file was created and has content
        if [[ -s "$output_file" ]]; then
            local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null)
            log_message "INFO" "CurrentOp data saved to: $output_file (${file_size} bytes)"
            
            # Show basic stats if verbose
            if [[ $VERBOSE == true ]]; then
                local operation_count=$(grep -o '"opid"' "$output_file" | wc -l | tr -d ' ')
                log_message "DEBUG" "Found $operation_count operations"
            fi
            
            return 0
        else
            log_message "ERROR" "Output file is empty or was not created"
            return 1
        fi
    else
        log_message "ERROR" "Failed to collect currentOp data: $result"
        return 1
    fi
}

# Function to generate output filename
generate_filename() {
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local collection_num=$1
    
    if [[ -n $collection_num ]]; then
        echo "${OUTPUT_DIR}/${FILENAME_PREFIX}_${timestamp}_${collection_num}.json"
    else
        echo "${OUTPUT_DIR}/${FILENAME_PREFIX}_${timestamp}.json"
    fi
}

# Function to create output directory
create_output_dir() {
    if [[ ! -d "$OUTPUT_DIR" ]]; then
        log_message "INFO" "Creating output directory: $OUTPUT_DIR"
        mkdir -p "$OUTPUT_DIR" || {
            log_message "ERROR" "Failed to create output directory: $OUTPUT_DIR"
            exit 1
        }
    fi
}

# Main execution function
main() {
    # Initialize variables with defaults
    HOST=$DEFAULT_HOST
    PORT=$DEFAULT_PORT
    DATABASE=$DEFAULT_DATABASE
    OUTPUT_DIR=$DEFAULT_OUTPUT_DIR
    FILENAME_PREFIX=$DEFAULT_FILENAME_PREFIX
    QUERY="{}"
    VERBOSE=false
    USE_SSL=false
    TIMEOUT=10
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--host)
                HOST="$2"
                shift 2
                ;;
            -p|--port)
                PORT="$2"
                shift 2
                ;;
            -d|--database)
                DATABASE="$2"
                shift 2
                ;;
            -u|--username)
                USERNAME="$2"
                shift 2
                ;;
            -P|--password)
                PASSWORD="$2"
                shift 2
                ;;
            -a|--auth-db)
                AUTH_DB="$2"
                shift 2
                ;;
            -o|--output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            -f|--filename)
                FILENAME_PREFIX="$2"
                shift 2
                ;;
            -c|--connection-string)
                CONNECTION_STRING="$2"
                shift 2
                ;;
            -q|--query)
                QUERY="$2"
                shift 2
                ;;
            -i|--interval)
                INTERVAL="$2"
                shift 2
                ;;
            -n|--count)
                COUNT="$2"
                shift 2
                ;;
            -t|--timeout)
                TIMEOUT="$2"
                shift 2
                ;;
            -s|--ssl)
                USE_SSL=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                log_message "ERROR" "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Validate numeric parameters
    if [[ -n $INTERVAL ]] && ! [[ $INTERVAL =~ ^[0-9]+$ ]]; then
        log_message "ERROR" "Interval must be a positive integer"
        exit 1
    fi
    
    if [[ -n $COUNT ]] && ! [[ $COUNT =~ ^[0-9]+$ ]]; then
        log_message "ERROR" "Count must be a positive integer"
        exit 1
    fi
    
    # Check for required tools
    check_mongo_client
    
    # Build connection string
    build_connection
    
    # Create output directory
    create_output_dir
    
    # Test connection
    if ! test_connection; then
        exit 1
    fi
    
    # Main collection logic
    if [[ -n $INTERVAL ]]; then
        # Continuous mode
        log_message "INFO" "Starting continuous collection (interval: ${INTERVAL}s${COUNT:+, count: $COUNT})"
        
        local collection_count=0
        while true; do
            collection_count=$((collection_count + 1))
            local output_file=$(generate_filename $collection_count)
            
            if collect_currentop "$output_file" "$collection_count"; then
                log_message "INFO" "Collection $collection_count completed"
            else
                log_message "WARN" "Collection $collection_count failed"
            fi
            
            # Check if we've reached the count limit
            if [[ -n $COUNT ]] && [[ $collection_count -ge $COUNT ]]; then
                log_message "INFO" "Completed $COUNT collections. Exiting."
                break
            fi
            
            log_message "DEBUG" "Waiting ${INTERVAL}s before next collection..."
            sleep "$INTERVAL"
        done
    else
        # Single collection mode
        local output_file=$(generate_filename)
        
        if collect_currentop "$output_file"; then
            log_message "INFO" "Collection completed successfully"
            echo -e "\n${GREEN}✓${NC} CurrentOp data collected and saved to: $output_file"
            echo -e "${BLUE}ℹ${NC} You can now upload this file to the CurrentOp Analyzer in the web interface"
        else
            log_message "ERROR" "Collection failed"
            exit 1
        fi
    fi
}

# Trap to handle script interruption
cleanup() {
    log_message "WARN" "Script interrupted by user"
    exit 130
}

trap cleanup SIGINT SIGTERM

# Run main function
main "$@"