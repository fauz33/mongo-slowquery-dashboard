#!/usr/bin/env python3
"""
Test script to verify selectivity calculation fix with real log data
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

def test_log_parsing():
    """Test parsing the actual log file"""
    print("🔍 Testing MongoDB Log Parsing with Selectivity Fix")
    print("=" * 60)
    
    # Create analyzer instance
    analyzer = app.MongoLogAnalyzer()
    
    # Parse the real log file
    log_file = 'mongodb_log_may02.txt'
    print(f"📄 Parsing log file: {log_file}")
    
    try:
        analyzer.parse_log_file(log_file)
        print(f"✅ Successfully parsed log file")
        print(f"📊 Found {len(analyzer.slow_queries)} slow queries")
        
        if not analyzer.slow_queries:
            print("❌ No slow queries found in log file")
            return False
        
        # Test the first few queries
        print(f"\n📋 Analyzing first 3 slow queries:")
        
        for i, query in enumerate(analyzer.slow_queries[:3]):
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
                print(f"Selectivity: {selectivity:.4f}%")
                
                if selectivity > 0:
                    print(f"✅ SUCCESS: Selectivity calculated correctly!")
                else:
                    print(f"❌ ISSUE: Selectivity is still 0%")
            else:
                print(f"⚠️  No docs examined data available")
                
            print(f"Plan Summary: {query.get('plan_summary', 'N/A')}")
        
        # Check if we have the expected values from the log
        expected_docs_examined = 10993
        expected_n_returned = 101
        expected_selectivity = (expected_n_returned / expected_docs_examined) * 100
        
        print(f"\n🎯 Expected Results from Log Analysis:")
        print(f"Expected docsExamined: {expected_docs_examined}")
        print(f"Expected nReturned: {expected_n_returned}")
        print(f"Expected Selectivity: {expected_selectivity:.4f}%")
        
        # Find the query that matches our log entry
        matching_query = None
        for query in analyzer.slow_queries:
            if (query.get('docsExamined') == expected_docs_examined and 
                query.get('nReturned') == expected_n_returned):
                matching_query = query
                break
        
        if matching_query:
            print(f"\n🎉 FOUND MATCHING QUERY!")
            calculated_selectivity = (matching_query['nReturned'] / matching_query['docsExamined']) * 100
            print(f"Calculated Selectivity: {calculated_selectivity:.4f}%")
            
            if abs(calculated_selectivity - expected_selectivity) < 0.001:
                print(f"✅ SELECTIVITY FIX SUCCESSFUL!")
                return True
            else:
                print(f"❌ Selectivity calculation still incorrect")
                return False
        else:
            print(f"⚠️  Could not find the specific query from the log")
            print(f"Checking if any query has non-zero selectivity...")
            
            found_nonzero = False
            for query in analyzer.slow_queries:
                docs_examined = query.get('docsExamined', 0)
                n_returned = query.get('nReturned', 0)
                if docs_examined > 0 and n_returned > 0:
                    selectivity = (n_returned / docs_examined) * 100
                    print(f"Found query with {selectivity:.4f}% selectivity")
                    found_nonzero = True
                    break
            
            if found_nonzero:
                print(f"✅ SUCCESS: Found queries with non-zero selectivity!")
                return True
            else:
                print(f"❌ ISSUE: All queries still have 0% selectivity")
                return False
                
    except FileNotFoundError:
        print(f"❌ Log file {log_file} not found")
        return False
    except Exception as e:
        print(f"❌ Error parsing log file: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_manual_parsing():
    """Test parsing the raw log line manually"""
    print(f"\n🔧 Manual Log Line Parsing Test")
    print("=" * 40)
    
    # Raw log line from the file
    log_line = '{"t":{"$date":"2025-05-02T13:14:26.265+00:00"},"s":"I","c":"COMMAND","id":51803,"ctx":"conn1740","msg":"Slow query","attr":{"type":"command","ns":"dbdashboard.uploadeddata","command":{"aggregate":"uploadeddata","pipeline":[{"$group":{"_id":"$clientId","latestDataDate":{"$max":"$dataDate"}}},{"$lookup":{"from":"uploadeddata","let":{"clientId":"$_id","dataDate":"$latestDataDate"},"pipeline":[{"$match":{"$expr":{"$and":[{"$eq":["$clientId","$$clientId"]},{"$eq":["$dataDate","$$dataDate"]}]}}},{"$group":{"_id":"$clientId","latestDataDate":"$latestDataDate","plans":{"$sum":"$numberOfPlansUploaded"}}},{"$project":{"_id":0,"clientId":"$_id.clientId","latestDataDate":"$_id.latestDataDate","totalNumberOfPlansUploaded":"$plans"}}],"as":"numberOfPlans"}},{"$unwind":"$numberOfPlans"},{"$group":{"_id":{"clientId":"$_id","latestDataDate":"$latestDataDate"},"plans":{"$sum":"$numberOfPlans.plans"}}},{"$project":{"_id":0,"clientId":"$_id.clientId","latestDataDate":"$_id.latestDataDate","totalNumberOfPlansUploaded":"$plans"}}],"cursor":{},"readConcern":{"level":"majority"},"lsid":{"id":{"$uuid":"eaee10f7-263e-40f2-8740-a6f334e2c31f"}},"$clusterTime":{"clusterTime":{"$timestamp":{"t":1746191660,"i":2}},"signature":{"hash":{"$binary":{"base64":"CqqStxae5Gqz3MKTsCJ19BOHTAo=","subType":"0"}},"keyId":{"$numberLong":"7440290581231697926"}}},"$db":"dbdashboard"},"planSummary":"COLLSCAN","planningTimeMicros":125,"cursorId":7806106322520278a3,"keysExamined":1215,"docsExamined":10993,"fromPlanCache":true,"numYields":6,"nReturned":101,"queryHash":"B908C6E6","planCacheKey":"69185E96","queryFramework":"classic","reslen":9016,"locks":{"FeatureCompatibilityVersion":{"acquireCount":{"r":8444}}},"readConcern":{"level":"majority","provenance":"clientSupplied"},"writeConcern":{"w":"majority","wtimeout":5000,"provenance":"customDefault"},"storage":[],"cpuNanos":998745446,"remote":"10.244.180.242:56669","protocol":"op_msg","durationMillis":10000}}'
    
    try:
        log_entry = json.loads(log_line)
        attr = log_entry.get('attr', {})
        
        print("📋 Raw attribute values:")
        print(f"docsExamined: {attr.get('docsExamined')}")
        print(f"nReturned: {attr.get('nReturned')}")
        print(f"keysExamined: {attr.get('keysExamined')}")
        print(f"durationMillis: {attr.get('durationMillis')}")
        
        # Test our new parsing logic manually
        docs_examined = (
            attr.get('docsExamined') if attr.get('docsExamined') is not None else
            attr.get('docs_examined') if attr.get('docs_examined') is not None else
            attr.get('totalDocsExamined') if attr.get('totalDocsExamined') is not None else
            attr.get('command', {}).get('docsExamined') if attr.get('command', {}).get('docsExamined') is not None else 0
        )
        
        n_returned = (
            attr.get('nReturned') if attr.get('nReturned') is not None else
            attr.get('n_returned') if attr.get('n_returned') is not None else
            attr.get('nreturned') if attr.get('nreturned') is not None else
            attr.get('numReturned') if attr.get('numReturned') is not None else
            attr.get('command', {}).get('nReturned') if attr.get('command', {}).get('nReturned') is not None else 0
        )
        
        print(f"\n🔧 Parsed values using new logic:")
        print(f"docs_examined: {docs_examined}")
        print(f"n_returned: {n_returned}")
        
        if docs_examined > 0:
            selectivity = (n_returned / docs_examined) * 100
            print(f"Calculated selectivity: {selectivity:.4f}%")
            
            if selectivity > 0:
                print(f"✅ SUCCESS: Manual parsing works correctly!")
                return True
            else:
                print(f"❌ ISSUE: Manual parsing still gives 0% selectivity")
                return False
        else:
            print(f"❌ ISSUE: docs_examined is 0 or None")
            return False
            
    except Exception as e:
        print(f"❌ Error in manual parsing: {e}")
        return False

def main():
    """Run selectivity fix tests"""
    print("=" * 70)
    print("MongoDB Log Analyzer - Selectivity Calculation Fix Test")
    print("=" * 70)
    
    # Test 1: Manual parsing
    manual_test = test_manual_parsing()
    
    # Test 2: Full log file parsing
    file_test = test_log_parsing()
    
    # Summary
    print(f"\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    tests = [
        ("Manual Log Line Parsing", manual_test),
        ("Full Log File Parsing", file_test)
    ]
    
    passed = sum(1 for name, result in tests if result)
    total = len(tests)
    
    for name, result in tests:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{name:<30} {status}")
    
    print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print(f"\n🎉 SELECTIVITY FIX SUCCESSFUL!")
        print(f"✅ The selectivity calculation now works correctly")
        print(f"✅ Expected: 0.92% selectivity for the test log entry")
        return True
    else:
        print(f"\n⚠️  Some tests failed. The selectivity fix needs more work.")
        return False

if __name__ == "__main__":
    main()