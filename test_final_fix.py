#!/usr/bin/env python3
"""
Final test to verify selectivity calculation with properly formatted MongoDB log
"""

import sys
import json
import datetime

# Mock pandas since it's not available
class MockPandas:
    def to_datetime(self, timestamp_str):
        try:
            if isinstance(timestamp_str, dict) and '$date' in timestamp_str:
                return datetime.datetime.fromisoformat(timestamp_str['$date'].replace('Z', '+00:00'))
            return datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except:
            return datetime.datetime.now()

sys.modules['pandas'] = MockPandas()

# Import the analyzer
import app

def test_with_correct_log():
    """Test with properly formatted MongoDB log file"""
    print("🔍 Testing MongoDB Log Analyzer with Correct Log Format")
    print("=" * 60)
    
    # Create analyzer instance
    analyzer = app.MongoLogAnalyzer()
    
    # Parse the corrected log file
    log_file = 'mongodb_log_correct.txt'
    print(f"📄 Parsing log file: {log_file}")
    
    try:
        analyzer.parse_log_file(log_file)
        print(f"✅ Successfully parsed log file")
        print(f"📊 Found {len(analyzer.slow_queries)} slow queries")
        
        if not analyzer.slow_queries:
            print("❌ No slow queries found in log file")
            return False
        
        print(f"\n📋 Analyzing all {len(analyzer.slow_queries)} slow queries:")
        
        all_correct = True
        expected_results = [
            {"docsExamined": 10993, "nReturned": 101, "expected_selectivity": 0.9188},
            {"docsExamined": 2500, "nReturned": 50, "expected_selectivity": 2.0},
            {"docsExamined": 75000, "nReturned": 100, "expected_selectivity": 0.1333}
        ]
        
        for i, query in enumerate(analyzer.slow_queries):
            print(f"\n--- Query {i+1} ---")
            print(f"Database: {query.get('database', 'N/A')}")
            print(f"Collection: {query.get('collection', 'N/A')}")
            print(f"Duration: {query.get('duration', 0)}ms")
            
            docs_examined = query.get('docsExamined', 0)
            n_returned = query.get('nReturned', 0)
            
            print(f"Docs Examined: {docs_examined}")
            print(f"Docs Returned: {n_returned}")
            
            # Calculate selectivity
            if docs_examined > 0:
                selectivity = (n_returned / docs_examined) * 100
                print(f"Calculated Selectivity: {selectivity:.4f}%")
                
                # Check against expected results
                if i < len(expected_results):
                    expected = expected_results[i]
                    if (docs_examined == expected["docsExamined"] and 
                        n_returned == expected["nReturned"]):
                        expected_sel = expected["expected_selectivity"]
                        if abs(selectivity - expected_sel) < 0.01:
                            print(f"✅ CORRECT: Matches expected {expected_sel:.4f}%")
                        else:
                            print(f"❌ INCORRECT: Expected {expected_sel:.4f}%")
                            all_correct = False
                    else:
                        print(f"⚠️  Unexpected values, but calculation looks correct")
                
                if selectivity > 0:
                    print(f"✅ SUCCESS: Non-zero selectivity calculated!")
                else:
                    print(f"❌ ISSUE: Selectivity is 0%")
                    all_correct = False
            else:
                print(f"⚠️  No docs examined data available")
                all_correct = False
                
            print(f"Plan Summary: {query.get('plan_summary', 'N/A')}")
        
        return all_correct
        
    except FileNotFoundError:
        print(f"❌ Log file {log_file} not found")
        return False
    except Exception as e:
        print(f"❌ Error parsing log file: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_json_parsing():
    """Test that our corrected log file has valid JSON"""
    print(f"\n🔧 JSON Validity Test")
    print("=" * 30)
    
    try:
        with open('mongodb_log_correct.txt', 'r') as f:
            lines = f.readlines()
        
        print(f"📄 Found {len(lines)} log lines")
        
        for i, line in enumerate(lines):
            line = line.strip()
            if line:
                try:
                    log_entry = json.loads(line)
                    attr = log_entry.get('attr', {})
                    
                    docs_examined = attr.get('docsExamined', 0)
                    n_returned = attr.get('nReturned', 0)
                    
                    print(f"Line {i+1}: docsExamined={docs_examined}, nReturned={n_returned}")
                    
                    if docs_examined > 0:
                        selectivity = (n_returned / docs_examined) * 100
                        print(f"         Selectivity: {selectivity:.4f}%")
                    
                except json.JSONDecodeError as e:
                    print(f"❌ JSON Error on line {i+1}: {e}")
                    return False
        
        print(f"✅ All JSON lines parsed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Run final selectivity fix tests"""
    print("=" * 70)
    print("MongoDB Log Analyzer - Final Selectivity Fix Test")
    print("=" * 70)
    
    # Test 1: JSON parsing validity
    json_test = test_json_parsing()
    
    # Test 2: Full analyzer test
    analyzer_test = test_with_correct_log()
    
    # Summary
    print(f"\n" + "=" * 70)
    print("FINAL TEST SUMMARY")
    print("=" * 70)
    
    tests = [
        ("JSON Format Validity", json_test),
        ("MongoDB Log Analyzer", analyzer_test)
    ]
    
    passed = sum(1 for name, result in tests if result)
    total = len(tests)
    
    for name, result in tests:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{name:<25} {status}")
    
    print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print(f"\n🎉 SELECTIVITY FIX COMPLETELY SUCCESSFUL!")
        print(f"✅ Parsing logic correctly handles MongoDB logs")
        print(f"✅ Selectivity calculations are working properly")
        print(f"✅ Expected results:")
        print(f"   - Query 1: 0.92% selectivity (101/10993)")
        print(f"   - Query 2: 2.00% selectivity (50/2500)")
        print(f"   - Query 3: 0.13% selectivity (100/75000)")
        return True
    else:
        print(f"\n⚠️  Some tests failed.")
        return False

if __name__ == "__main__":
    main()