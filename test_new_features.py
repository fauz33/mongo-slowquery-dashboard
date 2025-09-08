#!/usr/bin/env python3
"""
Test the new analysis features with uploaded data
"""

import sys
import datetime

# Mock pandas
class MockPandas:
    def to_datetime(self, timestamp_str):
        try:
            if isinstance(timestamp_str, dict) and '$date' in timestamp_str:
                return datetime.datetime.fromisoformat(timestamp_str['$date'].replace('Z', '+00:00'))
            return datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except:
            return datetime.datetime.now()

sys.modules['pandas'] = MockPandas()

import app

def test_global_analyzer_population():
    """Test if we can populate the global analyzer"""
    print("🔍 Testing Global Analyzer Population")
    print("=" * 50)
    
    print(f"Initial slow queries count: {len(app.analyzer.slow_queries)}")
    
    # Load test data into global analyzer
    if len(app.analyzer.slow_queries) == 0:
        print("📁 Loading test data into global analyzer...")
        app.analyzer.parse_log_file('mongodb_log_correct.txt')
        print(f"After loading: {len(app.analyzer.slow_queries)} slow queries")
    
    return len(app.analyzer.slow_queries) > 0

def test_enhanced_recommendations():
    """Test Enhanced Recommendations feature"""
    print("\n🔍 Testing Enhanced Recommendations")
    print("=" * 40)
    
    if not app.analyzer.slow_queries:
        print("❌ No slow queries available")
        return False
    
    try:
        # Create enhanced analyzer instance (same as in route)
        enhanced_analyzer = app.EnhancedIndexAnalyzer(app.analyzer.slow_queries)
        enhanced_recommendations = enhanced_analyzer.analyze_queries_advanced()
        
        print(f"✅ Enhanced recommendations generated: {len(enhanced_recommendations)}")
        
        if enhanced_recommendations:
            rec = enhanced_recommendations[0]
            print(f"   First recommendation pattern: {rec.get('pattern_hash', 'N/A')}")
            print(f"   Urgency: {rec.get('urgency', 'N/A')}")
            print(f"   Impact: {rec.get('performance_impact', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in Enhanced Recommendations: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_trend_analysis():
    """Test Trend Analysis feature"""
    print("\n🔍 Testing Trend Analysis")
    print("=" * 30)
    
    if not app.analyzer.slow_queries:
        print("❌ No slow queries available")
        return False
    
    try:
        trend_analyzer = app.TrendAnalyzer(app.analyzer.slow_queries)
        trends = trend_analyzer.get_trend_analysis()
        
        print(f"✅ Trend analysis generated: {len(trends)} unique queries")
        
        if trends:
            trend = trends[0]  # trends is a list, not a dict
            print(f"   First trend direction: {trend.get('duration_trend', 'N/A')}")
            print(f"   Total executions: {trend.get('total_executions', 0)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in Trend Analysis: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_resource_impact():
    """Test Resource Impact feature"""
    print("\n🔍 Testing Resource Impact")
    print("=" * 30)
    
    if not app.analyzer.slow_queries:
        print("❌ No slow queries available")
        return False
    
    try:
        resource_analyzer = app.ResourceImpactAnalyzer(app.analyzer.slow_queries)
        analysis = resource_analyzer.get_resource_analysis()
        
        print(f"✅ Resource impact analysis generated")
        print(f"   Memory intensive queries: {len(analysis.get('memory_intensive_queries', []))}")
        print(f"   IO intensive queries: {len(analysis.get('io_intensive_queries', []))}")
        print(f"   CPU intensive queries: {len(analysis.get('cpu_intensive_queries', []))}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in Resource Impact: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_workload_hotspots():
    """Test Workload Hotspots feature"""
    print("\n🔍 Testing Workload Hotspots")
    print("=" * 30)
    
    if not app.analyzer.slow_queries:
        print("❌ No slow queries available")
        return False
    
    try:
        workload_analyzer = app.WorkloadHotspotAnalyzer(app.analyzer.slow_queries)
        analysis = workload_analyzer.get_hotspot_analysis()
        
        print(f"✅ Workload hotspot analysis generated")
        print(f"   Peak periods: {len(analysis.get('peak_periods', []))}")
        print(f"   Time hotspots: {len(analysis.get('time_hotspots', {}))}")
        print(f"   Collection hotspots: {len(analysis.get('collection_hotspots', {}))}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in Workload Hotspots: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Test all new analysis features"""
    print("=" * 70)
    print("MongoDB Log Analyzer - New Features Test")
    print("=" * 70)
    
    # Step 1: Populate global analyzer
    populated = test_global_analyzer_population()
    
    if not populated:
        print("\n❌ CRITICAL: Cannot populate global analyzer with test data")
        print("All new features will fail with 'No slow queries found' error")
        return
    
    # Step 2: Test all new features
    tests = [
        ("Enhanced Recommendations", test_enhanced_recommendations),
        ("Trend Analysis", test_trend_analysis),
        ("Resource Impact", test_resource_impact),
        ("Workload Hotspots", test_workload_hotspots)
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"❌ {name} failed with error: {e}")
            results.append((name, False))
    
    # Summary
    print(f"\n" + "=" * 70)
    print("NEW FEATURES TEST SUMMARY")
    print("=" * 70)
    
    passed = sum(1 for name, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ WORKING" if result else "❌ BROKEN"
        print(f"{name:<25} {status}")
    
    print(f"\nResults: {passed}/{total} new features working")
    
    if passed == total:
        print(f"\n🎉 ALL NEW FEATURES ARE WORKING!")
        print(f"✅ The issue is that global analyzer is empty in web interface")
        print(f"✅ File upload process is not populating analyzer.slow_queries")
    else:
        print(f"\n⚠️  Some features have issues beyond the empty analyzer")
    
    print(f"\n📋 ROOT CAUSE:")
    print(f"❌ Global analyzer has 0 slow queries in web interface")
    print(f"✅ All new features require analyzer.slow_queries to have data") 
    print(f"✅ File upload logic needs to be debugged")

if __name__ == "__main__":
    main()