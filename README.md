# MongoDB Log Analyzer Dashboard

A Flask-based web dashboard for analyzing MongoDB log files to identify database connections and user activity patterns.

## Features

- **File Upload**: Upload MongoDB log files (.log, .txt) up to 50MB
- **Connection Analysis**: Parse and analyze database connection events
- **User Identification**: List all IP addresses that connected to the database
- **Visual Dashboard**: Charts and statistics showing connection patterns
- **Slow Query Detection**: Identify queries taking longer than 100ms
- **Responsive Design**: Bootstrap-based responsive interface

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
python app.py
```

3. Open your browser and navigate to `http://localhost:5000`

## Usage

1. **Upload Log File**: Click "Upload and Analyze" on the main page and select your MongoDB log file
2. **View Dashboard**: After upload, you'll be redirected to the analysis dashboard
3. **Analyze Results**: View connection statistics, IP addresses, and connection timeline

## Supported Log Formats

The analyzer can parse standard MongoDB log formats including:

- Connection accepted events: `connection accepted from IP:PORT #connXXX`
- Connection ended events: `end connection IP:PORT (X connections now open)`  
- Slow query logs: Command execution times in milliseconds

## Dashboard Features

### Statistics Cards
- **Total Connections**: Number of connection events found
- **Unique IPs**: Number of distinct IP addresses
- **Slow Queries**: Queries exceeding 100ms threshold
- **Analyzed Events**: Total events processed

### Visualizations
- **Pie Chart**: Connection distribution by IP address
- **Timeline**: Recent connection/disconnection events
- **Summary Table**: IP addresses with connection counts and types

### IP Classification
- **Local**: localhost/127.0.0.1 connections
- **Internal**: Private network IPs (192.168.x.x, 10.x.x.x, 172.x.x.x)
- **External**: Public IP addresses

## File Structure

```
mongo-slow-query/
├── app.py                 # Main Flask application
├── requirements.txt       # Python dependencies
├── uploads/              # Uploaded log files (created automatically)
├── templates/            # HTML templates
│   ├── base.html         # Base template with navigation
│   ├── dashboard.html    # Upload page
│   └── dashboard_results.html  # Analysis results
└── static/
    └── css/
        └── dashboard.css  # Custom styles
```

## Security Notes

- File uploads are limited to .log and .txt extensions only
- File size is capped at 50MB to prevent abuse
- Uploaded files are stored locally in the uploads/ directory
- No user authentication is implemented (add as needed for production use)

## API Endpoints

- `GET /` - Upload page
- `POST /upload` - File upload and processing
- `GET /dashboard` - Analysis results dashboard
- `GET /api/stats` - JSON API for connection statistics