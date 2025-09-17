# MongoDB Log Analyzer - Comprehensive Testing Workflow

## Overview

This testing framework provides comprehensive validation of the MongoDB Log Analyzer application, including functionality testing, performance monitoring, and data accuracy validation against baseline truth data.

## Testing Framework Components

### 📁 Directory Structure
```
testing-workflow/
├── run_tests.py              # Main test orchestrator
├── test_config.py            # Configuration and parameters
├── baseline_validator.py     # Raw log file analysis for baseline data
├── performance_monitor.py    # Performance tracking and monitoring
├── web_ui_tester.py         # Automated web interface testing
├── data_validator.py        # UI vs baseline data comparison
├── test_reports/            # Generated test reports and baselines
├── logs/                    # Test execution logs
└── README.md               # This documentation
```

## Quick Start

### Prerequisites
- Python virtual environment at: `/data/GDrive/fauz33/dashboard-opsmanager/proj_env/bin/activate`
- Test log file at: `/data/GDrive/fauz33/mongo-slow-query/mongodb_logs/mongodb_ip-10-178-141-71.eu-west-2.compute.internall.logs`
- MongoDB Log Analyzer application in working state

### Running the Complete Test Suite

```bash
cd /data/GDrive/fauz33/mongo-slow-query/testing-workflow
python run_tests.py
```

This will execute all 6 testing phases automatically:
1. **Baseline Analysis** - Parse raw log file to establish ground truth
2. **Application Startup** - Start Flask app and begin monitoring
3. **File Upload** - Upload test file and measure processing performance
4. **Web UI Testing** - Test all web interface functionality
5. **Data Validation** - Compare UI data with baseline truth
6. **Performance Analysis** - Analyze metrics and generate reports

## Testing Phases Detailed

### Phase 1: Baseline Data Analysis
- **Purpose**: Establish ground truth by directly parsing the raw log file
- **Output**: Baseline data with exact counts and metrics
- **Key Metrics**:
  - Total slow queries, connections, authentications
  - Unique databases, collections, namespaces
  - Performance statistics (duration, docs examined, etc.)
  - Time ranges and data quality metrics

### Phase 2: Application Startup
- **Purpose**: Start the web application and verify it's accessible
- **Features**:
  - Activates specified Python virtual environment
  - Starts Flask app on dedicated test port (5001)
  - Begins real-time performance monitoring
  - Verifies application responsiveness

### Phase 3: File Upload and Processing
- **Purpose**: Test file upload functionality and measure processing performance
- **Measurements**:
  - Upload duration and memory usage
  - File parsing performance
  - Database creation and sizing
  - Processing throughput (MB/second)

### Phase 4: Web UI Functionality Testing
- **Purpose**: Comprehensive testing of all web interface features
- **Test Coverage**:
  - Dashboard loading and statistics display
  - Slow queries page (pagination, filtering)
  - Search functionality (keyword, field-based)
  - Analysis pages (index suggestions, patterns, etc.)
  - Export functionality (JSON, CSV)
  - API endpoints validation

### Phase 5: Data Validation
- **Purpose**: Validate accuracy of web UI data against baseline truth
- **Validation Areas**:
  - Dashboard statistics accuracy
  - Query count consistency
  - Search result accuracy
  - Filter functionality correctness
  - Export data integrity
- **Tolerance**: 1% variance allowed for data count differences

### Phase 6: Performance Analysis
- **Purpose**: Analyze system performance and generate recommendations
- **Metrics Tracked**:
  - CPU and memory usage (system and application)
  - File I/O performance
  - Database size and efficiency
  - Response times for different operations
  - Threshold violation detection

## Configuration

### Test Parameters (`test_config.py`)

```python
# Environment Settings
VENV_PATH = "/data/GDrive/fauz33/dashboard-opsmanager/proj_env/bin/activate"
TEST_LOG_FILE = "/data/GDrive/fauz33/mongo-slow-query/mongodb_logs/mongodb_ip-10-178-141-71.eu-west-2.compute.internall.logs"
APP_PORT = 5001  # Dedicated test port

# Performance Thresholds
PERFORMANCE_THRESHOLDS = {
    'file_upload_time': 60.0,        # seconds
    'parsing_time_per_mb': 5.0,      # seconds per MB
    'dashboard_load_time': 3.0,      # seconds
    'search_response_time': 5.0,     # seconds
    'memory_usage_mb': 1000,         # MB
}

# Data Validation
VALIDATION_TOLERANCE = 0.01  # 1% tolerance for count differences
```

### Test Routes and Endpoints
The framework tests all major application routes:
- Core pages: `/`, `/dashboard`, `/slow-queries`, `/search-logs`
- Analysis pages: `/index-suggestions`, `/slow-query-analysis`, `/workload-summary`
- Export endpoints: `/export-slow-queries`, `/export-search-results`
- API endpoints: `/api/stats`

## Test Reports

### Generated Reports
All reports are saved to `test_reports/` with timestamps:

1. **`baseline_data_YYYYMMDD_HHMMSS.json`**
   - Raw log file analysis results
   - Ground truth data for validation

2. **`performance_report_YYYYMMDD_HHMMSS.json`**
   - System and application performance metrics
   - Threshold violation analysis

3. **`web_ui_test_results_YYYYMMDD_HHMMSS.json`**
   - Web interface functionality test results
   - Individual test case outcomes

4. **`data_validation_results_YYYYMMDD_HHMMSS.json`**
   - Data accuracy validation results
   - UI vs baseline comparison analysis

5. **`comprehensive_test_report_YYYYMMDD_HHMMSS.json`**
   - Complete test suite summary
   - All phases consolidated with recommendations

### Sample Report Structure
```json
{
  "session_id": "20231215_143022",
  "overall_status": "PASSED",
  "summary": {
    "total_phases": 6,
    "passed_phases": 6,
    "success_rate": 100.0,
    "total_duration_minutes": 8.5,
    "baseline_summary": {
      "file_size_mb": 45.2,
      "slow_queries": 1247,
      "connections": 89,
      "authentications": 34
    }
  },
  "phases": {
    "phase_1": { "name": "Baseline Analysis", "status": "PASSED" },
    "phase_2": { "name": "Application Startup", "status": "PASSED" },
    "phase_3": { "name": "File Upload", "status": "PASSED" },
    "phase_4": { "name": "Web UI Testing", "status": "PASSED" },
    "phase_5": { "name": "Data Validation", "status": "PASSED" },
    "phase_6": { "name": "Performance Analysis", "status": "PASSED" }
  }
}
```

## Running Individual Components

### Baseline Analysis Only
```bash
python baseline_validator.py
```

### Web UI Testing Only
```bash
python web_ui_tester.py
```

### Performance Monitoring Only
```bash
python performance_monitor.py
```

### Data Validation Only
```bash
python data_validator.py
```

## Performance Benchmarks

### Expected Performance Ranges
- **File Upload**: < 60 seconds for files up to 100MB
- **Dashboard Load**: < 3 seconds
- **Search Response**: < 5 seconds
- **Memory Usage**: < 1GB peak usage
- **Parsing Speed**: ~5 seconds per MB of log data

### Performance Monitoring Features
- Real-time CPU and memory tracking
- File I/O throughput measurement
- Database size optimization analysis
- Network usage monitoring
- Threshold violation alerts

## Data Validation Methodology

### Baseline Truth Establishment
1. **Direct Log Parsing**: Raw log file parsed line-by-line using same logic as application
2. **Event Counting**: Precise counting of slow queries, connections, authentications
3. **Metric Extraction**: Performance statistics, database/collection lists, time ranges
4. **Data Quality**: Error rate tracking and content validation

### UI Data Validation
1. **API Comparison**: Dashboard stats API vs baseline counts
2. **Search Accuracy**: Search result counts vs expected from baseline
3. **Filter Validation**: Filtered result counts vs logical expectations
4. **Export Integrity**: Exported data count and structure validation
5. **Pagination Accuracy**: Total count consistency across paginated views

### Validation Tolerance
- **Count Accuracy**: ±1% tolerance for event counts
- **Performance Metrics**: ±5% tolerance for duration/size metrics
- **Data Structure**: 100% accuracy required for data types and formats

## Troubleshooting

### Common Issues

#### Application Won't Start
```bash
# Check if port is in use
netstat -tlnp | grep 5001

# Check virtual environment
source /data/GDrive/fauz33/dashboard-opsmanager/proj_env/bin/activate
which python

# Check dependencies
pip list | grep flask
```

#### Test File Not Found
```bash
# Verify test file exists and is readable
ls -la /data/GDrive/fauz33/mongo-slow-query/mongodb_logs/
file /data/GDrive/fauz33/mongo-slow-query/mongodb_logs/mongodb_ip-10-178-141-71.eu-west-2.compute.internall.logs
```

#### Performance Threshold Violations
- Review `performance_report_*.json` for detailed metrics
- Check system resource availability during testing
- Adjust thresholds in `test_config.py` if needed

#### Data Validation Failures
- Compare `baseline_data_*.json` with `data_validation_results_*.json`
- Check for recent application changes that might affect data processing
- Verify database state and content

### Debug Mode
Set environment variable for verbose output:
```bash
export TEST_DEBUG=1
python run_tests.py
```

## Test Environment Requirements

### System Requirements
- **Memory**: Minimum 2GB available during testing
- **Disk**: 500MB free space for reports and temporary files
- **CPU**: Multi-core recommended for parallel processing tests

### Python Dependencies
- `requests` - Web interface testing
- `psutil` - Performance monitoring
- `json` - Data processing and reporting

### Application Dependencies
- Flask application must be functional and all dependencies installed
- Database write permissions required
- File upload functionality enabled

## Continuous Integration

### Automated Testing
The framework can be integrated into CI/CD pipelines:

```bash
#!/bin/bash
# CI Test Script
cd testing-workflow
python run_tests.py

# Check exit code
if [ $? -eq 0 ]; then
    echo "✅ All tests passed"
    exit 0
else
    echo "❌ Tests failed"
    exit 1
fi
```

### Regression Testing
- Run after each code change to detect regressions
- Compare performance metrics against previous baselines
- Track data accuracy trends over time

## Best Practices

### Before Testing
1. Ensure clean environment (no previous test artifacts)
2. Verify test log file integrity and accessibility
3. Check available system resources
4. Stop any running instances of the application

### During Testing
1. Avoid running other resource-intensive applications
2. Monitor test output for early failure detection
3. Let the complete suite run uninterrupted for best results

### After Testing
1. Review comprehensive test report for all phases
2. Investigate any failed tests or performance issues
3. Archive test reports for historical comparison
4. Update baseline data if application behavior legitimately changes

## Contributing

### Adding New Tests
1. Add test configuration to `test_config.py`
2. Implement test logic in appropriate module
3. Update test orchestrator in `run_tests.py`
4. Document new test in this README

### Modifying Validation Logic
1. Update validation methods in `data_validator.py`
2. Adjust tolerance settings if needed
3. Test validation logic against known good/bad data
4. Update documentation with validation changes

---

## Support

For questions or issues with the testing framework:

1. Check test reports for detailed error information
2. Review configuration in `test_config.py`
3. Verify environment setup and dependencies
4. Run individual components to isolate issues

The testing framework is designed to be comprehensive yet maintainable, providing confidence in the MongoDB Log Analyzer application's functionality, performance, and data accuracy.