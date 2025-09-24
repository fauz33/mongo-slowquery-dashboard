# MongoDB Log Analyzer

A comprehensive web-based tool for analyzing MongoDB log files, monitoring database operations, and identifying performance issues.

## Quick Installation

### Prerequisites
- **Python 3.8+** (Python 3.9+ recommended)
- **pip** (Python package installer)

### Installation Steps

1. **Clone or Download the Project**
   ```bash
   # Clone repository (if using git)
   git clone <repository-url>
   cd mongo-slow-query
   
   # OR download and extract the ZIP file
   ```

2. **Create Virtual Environment (Recommended)**
   ```bash
   # Create virtual environment
   python -m venv mongodb-analyzer
   
   # Activate virtual environment
   # On Windows:
   mongodb-analyzer\Scripts\activate
   
   # On macOS/Linux:
   source mongodb-analyzer/bin/activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Start the Application**
   ```bash
   # Development mode (recommended for testing)
   python app.py
   ```

5. **Access the Application**
   - Open your web browser
   - Navigate to: **http://localhost:5000**

## Production Installation

For production deployments with multiple users:

1. **Install Production Server**
   ```bash
   pip install gunicorn
   ```

2. **Run with Gunicorn**
   ```bash
   # Basic production setup
   gunicorn --workers 4 --bind 0.0.0.0:5000 app:app
   
   # Advanced production setup (recommended)
   gunicorn --workers 4 --bind 0.0.0.0:5000 --timeout 300 --worker-class sync app:app
   ```

3. **Optional: Install Process Manager**
   ```bash
   # For systemd service management
   sudo apt install systemd
   
   # For supervisor process management  
   pip install supervisor
   ```

## Docker Installation

1. **Create Dockerfile**
   ```dockerfile
   FROM python:3.9-slim
   
   WORKDIR /app
   COPY requirements.txt .
   RUN pip install -r requirements.txt
   
   COPY . .
   EXPOSE 5000
   
   CMD ["gunicorn", "--workers", "4", "--bind", "0.0.0.0:5000", "app:app"]
   ```

2. **Build and Run**
   ```bash
   # Build Docker image
   docker build -t mongodb-analyzer .
   
   # Run container
   docker run -p 5000:5000 -v $(pwd)/uploads:/app/uploads mongodb-analyzer
   ```

## Verification

After installation, verify everything works:

1. **Check Dependencies**
   ```bash
   pip list | grep -E "(Flask|pandas|Werkzeug|Jinja2)"
   ```

2. **Test Application**
   - Visit: http://localhost:5000
   - You should see the MongoDB Log Analyzer homepage
   - Try uploading a small test log file

3. **Check Logs**
   ```bash
   # Look for startup messages
   tail -f logs/app.log  # If logging is configured
   ```

## Feature Flags & Rollback

The v2 DuckDB/Parquet stack can be toggled at runtime via environment variables:

- `ENABLE_V2_UI=0` forces Flask to render the legacy HTML pages (the new `/v2` API endpoints remain available for tooling/testing).
- `MONGO_SLOWQ_ENABLE_V2=0` disables the v2 blueprint entirely, returning the application to its original SQLite-backed routes.

Set either flag before launching `app.py` or `gunicorn` to roll back quickly if production issues arise.

## Troubleshooting Installation

### Common Issues

**1. Python Version Error**
```bash
# Check Python version
python --version  # Should be 3.8+

# If too old, install newer Python or use python3
python3 app.py
```

**2. Permission Denied**
```bash
# Fix permissions
chmod +x app.py
sudo chown -R $USER:$USER .
```

**3. Port Already in Use**
```bash
# Find process using port 5000
lsof -i :5000

# Kill process or use different port
python app.py --port 5001
```

**4. Missing Dependencies**
```bash
# Reinstall requirements
pip install -r requirements.txt --force-reinstall

# Check for missing system packages
sudo apt install python3-dev build-essential  # Ubuntu/Debian
```

### System-Specific Installation

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv
```

**CentOS/RHEL:**
```bash
sudo yum install python3 python3-pip
```

**macOS:**
```bash
# Install Homebrew first: https://brew.sh
brew install python3
```

**Windows:**
```bash
# Download Python from: https://www.python.org/downloads/windows/
# Make sure to check "Add Python to PATH"
```

## Directory Structure

After installation, you'll have:
```
mongo-slow-query/
├── app.py                    # Main application
├── requirements.txt          # Dependencies
├── README.md                # This file
├── uploads/                 # Auto-created for file uploads
├── temp/                   # Auto-created for processing
├── templates/              # HTML templates
├── static/                # CSS, JS, images
└── scripts/               # Collection scripts
    ├── collect_currentop.sh
    ├── simple_currentop.sh
    └── README.md
```

## Next Steps

Once installed successfully:

1. **Upload Log Files** - Try the main upload feature
2. **Explore Features** - Check Dashboard, Slow Query Analysis, Search
3. **Use CurrentOp Analyzer** - Monitor live MongoDB operations  
4. **Collection Scripts** - Use provided bash scripts for automation

## Configuration

The application runs with default settings. For customization, modify `app.py`:

```python
# Basic configuration
app.config['SECRET_KEY'] = 'your-secret-key-change-this'
app.config['UPLOAD_FOLDER'] = 'uploads'  
app.config['TEMP_FOLDER'] = 'temp'

# For production, also set:
app.config['DEBUG'] = False
```

## Support

If you encounter installation issues:
1. Check the troubleshooting section above
2. Ensure all prerequisites are met
3. Try the virtual environment approach
4. Check file permissions and directory access
- **v2 Experimental Stack**
  - `app_v2.py` hosts the Flask entrypoint for the Parquet/DuckDB backend. Set `SLOWQ_DATASET_ROOT` to point at the generated dataset (default `out/`).
  - `log_analyzer_v2/cli/analyze.py` ingests logs into the new columnar store; run `python -m log_analyzer_v2.cli.analyze ingest-log <logfile> --out out_v2`.
  - Lightweight integration tests for the v2 endpoints live in `tests/test_v2_endpoints.py` (requires `pytest`).
  - Visit `/upload/v2` to ingest new logs, then explore `/dashboard/v2`, `/slow-queries/v2`, `/slow-query-analysis/v2`, `/query-trend/v2`, `/workload-summary/v2`, `/current-op/v2`, `/search-logs/v2`, `/search-user-access/v2`, and `/index-suggestions/v2` to try the client-side dashboards backed by the new JSON APIs (legacy pages remain available). Set `ENABLE_V2_UI=1` in the Flask config to surface the new nav links.
  - CLI quick reference:

    ```bash
    python -m log_analyzer_v2.cli.analyze ingest-log /path/to/mongodb.log --out out_v2
    python -m log_analyzer_v2.cli.analyze status --out out_v2
    python -m log_analyzer_v2.cli.analyze summaries --out out_v2 --limit 10
    python -m log_analyzer_v2.cli.analyze clean --out out_v2 --force
    ```

    Use `status` to review manifest entries, `summaries` for quick workload stats, and `clean` to remove a dataset (helpful for retention/rollovers).
  - When the flag is enabled, main navigation links for "Queries Analysis" and "Queries Trends" point to the v2 pages while legacy counterparts stay accessible under the “v2” subitems.
