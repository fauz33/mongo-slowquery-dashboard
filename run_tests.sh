#!/bin/bash
# Automated Test Runner for MongoDB Log Analyzer
# Usage: ./run_tests.sh [--ci] [--coverage] [--verbose]

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
CI_MODE=false
COVERAGE=false
VERBOSE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --ci)
      CI_MODE=true
      shift
      ;;
    --coverage)
      COVERAGE=true
      shift
      ;;
    --verbose)
      VERBOSE=true
      shift
      ;;
    *)
      echo "Unknown option $1"
      echo "Usage: $0 [--ci] [--coverage] [--verbose]"
      exit 1
      ;;
  esac
done

echo -e "${BLUE}🚀 MongoDB Log Analyzer - Automated Test Runner${NC}"
echo "================================================="

# Check if virtual environment exists, create if not
if [ ! -d "test_env" ]; then
    echo -e "${YELLOW}📦 Creating virtual environment...${NC}"
    python -m venv test_env
fi

# Activate virtual environment
echo -e "${BLUE}🔧 Activating virtual environment...${NC}"
source test_env/bin/activate

# Install/upgrade dependencies
echo -e "${BLUE}📋 Installing dependencies...${NC}"
pip install flask > /dev/null 2>&1

if [ "$COVERAGE" = true ]; then
    pip install coverage > /dev/null 2>&1
fi

# Check required files exist
REQUIRED_FILES=(
    "app.py"
    "mongodb_log_correct.txt"
    "test_framework.py"
)

echo -e "${BLUE}📁 Checking required files...${NC}"
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}❌ Required file missing: $file${NC}"
        exit 1
    fi
done

echo -e "${GREEN}✅ All required files found${NC}"

# Run syntax check first
echo -e "${BLUE}🔍 Running syntax check...${NC}"
python -m py_compile app.py
python -m py_compile test_framework.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Syntax check passed${NC}"
else
    echo -e "${RED}❌ Syntax errors found${NC}"
    exit 1
fi

# Run the main test framework
echo -e "${BLUE}🧪 Running automated test framework...${NC}"
echo ""

if [ "$COVERAGE" = true ]; then
    echo -e "${BLUE}📊 Running with coverage analysis...${NC}"
    coverage run --source=. test_framework.py
    TEST_EXIT_CODE=$?
    
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        echo -e "${BLUE}📈 Generating coverage report...${NC}"
        coverage report
        coverage html
        echo -e "${GREEN}📊 Coverage report generated in htmlcov/index.html${NC}"
    fi
else
    python test_framework.py
    TEST_EXIT_CODE=$?
fi

echo ""
echo "================================================="

# Process results
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}🎉 ALL TESTS PASSED!${NC}"
    echo -e "${GREEN}✅ MongoDB Log Analyzer is fully functional${NC}"
    
    if [ "$CI_MODE" = true ]; then
        echo "::notice::All tests passed - MongoDB Log Analyzer is functional"
    fi
    
    # Generate badge
    echo -e "${BLUE}📊 Test Results Summary:${NC}"
    if [ -f "test_report.json" ]; then
        python3 -c "
import json
with open('test_report.json') as f:
    report = json.load(f)
    print(f'   Total Tests: {report[\"total_tests\"]}')
    print(f'   Passed: {report[\"total_passed\"]}')
    print(f'   Success Rate: {report[\"overall_success_rate\"]:.1f}%')
    print(f'   Test Suites: {report[\"total_test_suites\"]}')
"
    fi
    
else
    echo -e "${RED}❌ SOME TESTS FAILED${NC}"
    echo -e "${YELLOW}⚠️  Check test_report.json for detailed information${NC}"
    
    if [ "$CI_MODE" = true ]; then
        echo "::error::Some tests failed - check logs for details"
    fi
    
    if [ "$VERBOSE" = true ] && [ -f "test_report.json" ]; then
        echo -e "${BLUE}📋 Failed Tests Details:${NC}"
        python3 -c "
import json
with open('test_report.json') as f:
    report = json.load(f)
    for suite in report['detailed_results']:
        failed_tests = [t for t in suite['tests'] if not t['passed']]
        if failed_tests:
            print(f'\\n{suite[\"suite_name\"]}:')
            for test in failed_tests:
                print(f'   ❌ {test[\"name\"]}: {test[\"message\"]}')
"
    fi
fi

# Cleanup
if [ "$CI_MODE" = false ]; then
    echo -e "${BLUE}🧹 Deactivating virtual environment...${NC}"
    deactivate
fi

echo -e "${BLUE}✅ Test run completed${NC}"
exit $TEST_EXIT_CODE