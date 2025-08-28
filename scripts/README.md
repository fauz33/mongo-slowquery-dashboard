# MongoDB CurrentOp Collection Scripts

This directory contains bash scripts to automate the collection of `db.currentOp()` output from MongoDB for analysis.

## Scripts Available

### 1. `collect_currentop.sh` - Full-Featured Script
Comprehensive script with extensive options for production environments.

### 2. `simple_currentop.sh` - Quick & Simple Script  
Lightweight script for basic currentOp collection.

---

## Quick Start

### Simple Collection (Recommended for most users)

```bash
# Collect from localhost
./simple_currentop.sh

# Collect from remote host
./simple_currentop.sh "mongohost:27017"

# Specify output file
./simple_currentop.sh "localhost:27017" "my_currentop.json"
```

### Advanced Collection

```bash
# Basic collection
./collect_currentop.sh

# Collect from remote MongoDB with authentication
./collect_currentop.sh -h mongo.example.com -p 27017 -u admin -P password

# Collect only active operations
./collect_currentop.sh -q '{"active": true}'

# Continuous collection every 30 seconds, 10 times
./collect_currentop.sh -i 30 -n 10
```

---

## Detailed Usage

### Simple Script: `simple_currentop.sh`

**Syntax:**
```bash
./simple_currentop.sh [HOST:PORT] [OUTPUT_FILE]
```

**Parameters:**
- `HOST:PORT` - MongoDB host and port (default: localhost:27017)
- `OUTPUT_FILE` - Output filename (default: currentop_YYYYMMDD_HHMMSS.json)

**Examples:**
```bash
# Default collection
./simple_currentop.sh

# Remote collection
./simple_currentop.sh "prod-mongo:27017"

# Custom output file
./simple_currentop.sh "localhost:27017" "current_operations.json"
```

---

### Advanced Script: `collect_currentop.sh`

**Full Options:**
```
Options:
  -h, --host HOST          MongoDB host (default: localhost)
  -p, --port PORT          MongoDB port (default: 27017)
  -d, --database DB        Database name (default: admin)
  -u, --username USER      MongoDB username
  -P, --password PASS      MongoDB password
  -a, --auth-db AUTH_DB    Authentication database
  -o, --output-dir DIR     Output directory (default: ./currentop_data)
  -f, --filename PREFIX    Output filename prefix (default: currentop)
  -c, --connection-string URI  MongoDB connection string
  -q, --query QUERY        Custom currentOp query
  -i, --interval SECONDS   Collect every N seconds (continuous mode)
  -n, --count NUM          Number of collections in continuous mode
  -t, --timeout SECONDS    Connection timeout (default: 10)
  -s, --ssl                Use SSL/TLS connection
  -v, --verbose            Verbose output
  --help                   Show help message
```

**Common Use Cases:**

#### Basic Collection
```bash
# Local MongoDB, default settings
./collect_currentop.sh

# Remote MongoDB
./collect_currentop.sh -h prod-mongo -p 27017
```

#### Authentication
```bash
# Username/password authentication
./collect_currentop.sh -h mongo.example.com -u admin -P secretpass -a admin

# Connection string authentication
./collect_currentop.sh -c "mongodb://user:pass@mongo.example.com:27017/admin?authSource=admin"
```

#### Custom Queries
```bash
# Only active operations
./collect_currentop.sh -q '{"active": true}'

# Long-running operations (>30 seconds)
./collect_currentop.sh -q '{"secs_running": {"$gte": 30}}'

# Operations on specific database
./collect_currentop.sh -q '{"ns": /^myapp\./}'

# Operations waiting for locks
./collect_currentop.sh -q '{"waitingForLock": true}'
```

#### Continuous Collection
```bash
# Collect every 30 seconds, unlimited
./collect_currentop.sh -i 30

# Collect every 60 seconds, 10 times
./collect_currentop.sh -i 60 -n 10

# Monitor during maintenance window
./collect_currentop.sh -i 15 -n 40 -v  # 15s intervals, 10 minutes total
```

#### SSL/TLS Connections
```bash
# SSL connection
./collect_currentop.sh -h secure-mongo.example.com -s -u user -P pass

# Full SSL connection string
./collect_currentop.sh -c "mongodb://user:pass@mongo.example.com:27017/admin?ssl=true&authSource=admin"
```

---

## Output Files

### File Naming Convention
- **Simple script**: `currentop_YYYYMMDD_HHMMSS.json`
- **Advanced script**: `currentop_YYYYMMDD_HHMMSS.json` (single) or `currentop_YYYYMMDD_HHMMSS_N.json` (continuous)

### File Location
- **Simple script**: Current directory
- **Advanced script**: `./currentop_data/` (configurable with `-o`)

### File Format
Output files contain valid JSON that can be directly uploaded to the CurrentOp Analyzer web interface.

---

## Integration with Web Analyzer

1. **Run Collection Script**
   ```bash
   ./simple_currentop.sh
   ```

2. **Upload to Web Interface**
   - Open the CurrentOp Analyzer page
   - Go to the "Upload File" tab
   - Select the generated JSON file
   - Click "Upload and Analyze"

---

## Troubleshooting

### Common Issues

**1. "mongosh/mongo not found"**
```bash
# Install MongoDB shell
# Ubuntu/Debian:
wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -
sudo apt-get install -y mongodb-mongosh

# CentOS/RHEL:
sudo yum install -y mongodb-mongosh

# macOS:
brew tap mongodb/brew
brew install mongosh
```

**2. "Connection refused"**
```bash
# Check if MongoDB is running
sudo systemctl status mongod

# Check if port is open
netstat -lan | grep :27017
```

**3. "Authentication failed"**
```bash
# Verify credentials
./collect_currentop.sh -h host -u username -P password -a admin -v

# Use connection string for complex auth
./collect_currentop.sh -c "mongodb://user:pass@host:27017/admin?authSource=admin"
```

**4. "Empty output file"**
```bash
# Run with verbose mode to debug
./collect_currentop.sh -v

# Test connection first
mongosh "mongodb://host:27017/admin" --eval "db.runCommand({ping: 1})"
```

### Permission Issues
```bash
# Make scripts executable
chmod +x *.sh

# Check current directory permissions
ls -la
```

---

## Automation Examples

### Cron Job for Regular Collection
```bash
# Edit crontab
crontab -e

# Collect every hour
0 * * * * /path/to/collect_currentop.sh -o /var/log/mongodb/currentop

# Collect every 5 minutes during business hours
*/5 9-17 * * 1-5 /path/to/simple_currentop.sh prod-mongo:27017 /var/log/currentop/currentop_$(date +\%Y\%m\%d_\%H\%M).json
```

### Monitoring Script
```bash
#!/bin/bash
# Monitor for long-running operations
while true; do
    if ./collect_currentop.sh -q '{"secs_running": {"$gte": 120}}' -f "longrunning"; then
        # Alert if operations found
        if [ -s "./currentop_data/longrunning_*.json" ]; then
            echo "Alert: Long-running operations detected!"
            # Send notification, email, etc.
        fi
    fi
    sleep 300  # Check every 5 minutes
done
```

---

## Security Best Practices

1. **Avoid passwords in command line**
   ```bash
   # Use connection string or config file
   ./collect_currentop.sh -c "mongodb://user:$MONGO_PASSWORD@host:27017/admin"
   ```

2. **Use read-only users**
   ```javascript
   // Create read-only user in MongoDB
   use admin
   db.createUser({
     user: "currentop_reader",
     pwd: "secure_password",
     roles: ["clusterMonitor", "read"]
   })
   ```

3. **Secure output files**
   ```bash
   # Set restrictive permissions
   chmod 600 currentop_*.json
   ```

---

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Run scripts with `-v` (verbose) flag for debugging
3. Verify MongoDB connection manually with `mongosh`
4. Check MongoDB logs for connection/authentication issues