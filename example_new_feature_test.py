#!/usr/bin/env python3
"""
Example: How to add automated tests for new features

This shows you exactly how to add tests when you create new analysis features.
Copy this pattern for any new analyzer or feature you add.
"""

import time
from test_framework import TestSuite, TestResult

def test_example_new_feature(self) -> TestSuite:
    """Example test for a hypothetical new feature"""
    suite = TestSuite("Example New Feature")
    
    # Test 1: Basic functionality
    start_time = time.time()
    try:
        # Replace this with your actual new feature code
        # Example: new_analyzer = self.app.YourNewAnalyzer(self.app.analyzer.slow_queries)
        # Example: result = new_analyzer.get_analysis()
        
        # For demonstration, we'll simulate a new feature
        mock_result = {
            'analysis_type': 'query_optimization',
            'recommendations': [
                {'type': 'index', 'field': 'clientId', 'impact': 'high'},
                {'type': 'query_rewrite', 'suggestion': 'use $lookup', 'impact': 'medium'}
            ],
            'performance_gain': 75.5
        }
        
        # Test validation logic
        passed = (
            len(mock_result.get('recommendations', [])) > 0 and
            'analysis_type' in mock_result and
            mock_result.get('performance_gain', 0) > 0
        )
        
        message = f"Generated {len(mock_result['recommendations'])} recommendations with {mock_result['performance_gain']}% potential gain"
        
    except Exception as e:
        passed = False
        message = f"Error: {str(e)}"
    
    suite.add_result(TestResult(
        "Example Feature Analysis", 
        passed, 
        message, 
        time.time() - start_time
    ))
    
    # Test 2: Edge case handling
    start_time = time.time()
    try:
        # Test with empty data
        empty_queries = []
        # Example: empty_analyzer = self.app.YourNewAnalyzer(empty_queries)
        # Example: empty_result = empty_analyzer.get_analysis()
        
        # Simulate handling empty data
        empty_result = {'recommendations': [], 'performance_gain': 0}
        
        passed = isinstance(empty_result.get('recommendations'), list)
        message = "Handles empty data correctly"
        
    except Exception as e:
        passed = False
        message = f"Error with empty data: {str(e)}"
    
    suite.add_result(TestResult(
        "Example Feature Edge Cases", 
        passed, 
        message, 
        time.time() - start_time
    ))
    
    return suite

# How to integrate this into the main test framework:
"""
1. Add your test method to the MongoAnalyzerTestFramework class in test_framework.py:

   def test_your_new_feature(self) -> TestSuite:
       # Copy the pattern above, replace with your actual feature code
       pass

2. Register it in the run_all_tests method:

   test_functions = [
       self.test_core_functionality,
       self.test_enhanced_recommendations,
       self.test_trend_analysis,
       self.test_resource_impact,
       self.test_workload_hotspots,
       self.test_web_routes,
       self.test_your_new_feature,  # Add this line
   ]

3. Run the tests:
   ./run_tests.sh --verbose

4. Your feature will now be automatically tested every time!
"""

# Real-world example for a Query Performance Analyzer:
def test_query_performance_analyzer_example(self) -> TestSuite:
    """Real example of testing a query performance analyzer"""
    suite = TestSuite("Query Performance Analyzer")
    
    start_time = time.time()
    try:
        # Your actual analyzer code would go here:
        # performance_analyzer = self.app.QueryPerformanceAnalyzer(self.app.analyzer.slow_queries)
        # analysis = performance_analyzer.analyze_query_patterns()
        
        # Mock for demonstration
        analysis = {
            'slow_patterns': [
                {'pattern': 'collection_scan', 'count': 5, 'avg_duration': 2500},
                {'pattern': 'unindexed_sort', 'count': 3, 'avg_duration': 1800}
            ],
            'optimization_opportunities': [
                {'collection': 'users', 'suggested_index': 'email_1', 'impact': 'high'}
            ]
        }
        
        # Validation
        required_fields = ['slow_patterns', 'optimization_opportunities']
        missing_fields = [f for f in required_fields if f not in analysis]
        
        passed = len(missing_fields) == 0 and len(analysis['slow_patterns']) > 0
        
        if passed:
            message = f"Found {len(analysis['slow_patterns'])} performance patterns and {len(analysis['optimization_opportunities'])} optimization opportunities"
        else:
            message = f"Missing required fields: {missing_fields}" if missing_fields else "No performance patterns found"
        
    except Exception as e:
        passed = False
        message = f"Analysis failed: {str(e)}"
    
    suite.add_result(TestResult(
        "Query Performance Pattern Analysis", 
        passed, 
        message, 
        time.time() - start_time
    ))
    
    return suite

print("""
🎯 Example New Feature Testing Pattern

To add automated tests for your new MongoDB analyzer features:

1. Copy the test pattern above
2. Replace mock code with your actual analyzer
3. Add validation logic for your specific results
4. Register the test in test_framework.py
5. Run ./run_tests.sh to validate

This ensures every new feature is automatically tested! 🚀
""")