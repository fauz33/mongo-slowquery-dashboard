# MongoDB Log Analyzer - Testing Guide

## 🚀 Quick Start

### Run All Tests
```bash
./run_tests.sh
```

### Run with Coverage
```bash
./run_tests.sh --coverage
```

### Run with Verbose Output
```bash
./run_tests.sh --verbose
```

## 📋 What Gets Tested

### Core Functionality ✅
- **Log File Parsing**: Can the analyzer read MongoDB logs correctly?
- **Selectivity Calculation**: Is the math working (fixes the 0% issue)?

### All New Features ✅
- **Enhanced Recommendations**: Generates advanced index recommendations
- **Trend Analysis**: Analyzes query performance trends over time  
- **Resource Impact**: Identifies memory/CPU/IO intensive queries
- **Workload Hotspots**: Finds peak usage periods

### Web Routes ✅
- Validates that all route functions exist and are accessible

## 🎯 Test Results

### Success Output
```
🎉 ALL TESTS PASSED!
✅ MongoDB Log Analyzer is fully functional
📊 Tests: 10/10 passed
🎯 Overall Success Rate: 100.0%
```

### Generated Files
- `test_report.json` - Detailed results with timing and error messages
- `htmlcov/index.html` - Code coverage report (with --coverage flag)

## 🔧 Adding Tests for New Features

### 1. Add Test Method
```python
def test_my_new_feature(self) -> TestSuite:
    """Test My New Feature"""
    suite = TestSuite("My New Feature")
    
    start_time = time.time()
    try:
        # Test your new analyzer/feature
        result = self.app.MyNewAnalyzer(self.app.analyzer.slow_queries)
        analysis = result.get_analysis()
        
        # Validate results
        passed = len(analysis) > 0 and 'required_field' in analysis
        message = f"Generated {len(analysis)} results"
        
    except Exception as e:
        passed = False
        message = f"Error: {str(e)}"
    
    suite.add_result(TestResult(
        "My New Feature Test", 
        passed, 
        message, 
        time.time() - start_time
    ))
    
    return suite
```

### 2. Register Test in Framework
```python
# In run_all_tests method
test_functions = [
    # ... existing tests ...
    self.test_my_new_feature,  # Add this line
]
```

### 3. Test Your Addition
```bash
./run_tests.sh --verbose
```

## 🏗️ CI/CD Integration

### GitHub Actions
The framework includes `.github/workflows/test.yml` for automatic testing on:
- Every push to main/develop
- Pull requests  
- Daily at 2 AM UTC
- Multiple Python versions (3.9-3.12)

### Pre-commit Hooks
```bash
# Install pre-commit hooks
make dev-setup

# Tests will run automatically before commits
git commit -m "Add new feature"
```

### Make Commands
```bash
make test           # Run tests
make test-coverage  # Run with coverage
make test-verbose   # Run with verbose output
make dev-setup      # Setup development environment
make lint           # Code quality checks
make clean          # Clean temporary files
```

## 📊 Understanding Test Reports

### Console Output Meaning
```
🧪 Running Enhanced Recommendations...
   ✅ 1/1 tests passed (100.0%)     # All tests in suite passed
   ❌ Failed Test: Error message    # Shows failures immediately

📋 AUTOMATED TEST REPORT
🎯 Overall Success Rate: 90.0%     # Overall health score
📊 Tests: 9/10 passed              # Total test count
```

### JSON Report Structure
```json
{
  "overall_success_rate": 90.0,
  "total_tests": 10,
  "total_passed": 9,
  "test_suites": [
    {
      "name": "Enhanced Recommendations",
      "success_rate": 100.0,
      "total_time": 0.0001626
    }
  ]
}
```

## 🐛 Troubleshooting

### Common Issues

**"Required file missing"**
- Ensure `mongodb_log_correct.txt` exists
- Run from project root directory

**"Syntax errors found"**
- Check Python syntax in `app.py`
- Run `python -m py_compile app.py`

**"Import errors"**  
- Activate virtual environment: `source test_env/bin/activate`
- Install dependencies: `pip install flask`

**"No slow queries found"**
- This indicates the global analyzer issue we fixed
- Test framework loads data automatically

### Running Individual Components
```bash
# Just syntax check
python -m py_compile app.py

# Just the test framework
source test_env/bin/activate
python test_framework.py

# Check test data
python -c "
import sys
sys.modules['pandas'] = type('', (), {'to_datetime': lambda self, x: x})()
import app
print(f'Queries: {len(app.analyzer.slow_queries)}')
"
```

## 🎯 Best Practices

### When Adding New Features
1. ✅ Write test first (TDD approach)
2. ✅ Implement feature
3. ✅ Run `./run_tests.sh --verbose`
4. ✅ Fix any failures
5. ✅ Check coverage with `--coverage`
6. ✅ Commit only when all tests pass

### Test Quality Guidelines
- **Fast**: Each test should run in < 1 second
- **Isolated**: Tests don't depend on each other
- **Repeatable**: Same results every time
- **Clear**: Good error messages
- **Comprehensive**: Cover happy path + edge cases

### Monitoring
- Set up daily runs with `cron`
- Monitor success rate trends
- Alert on failures in CI/CD
- Review coverage reports regularly

This automated framework ensures every feature works correctly and catches regressions early! 🎉