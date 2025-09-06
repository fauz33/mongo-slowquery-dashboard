#!/usr/bin/env python3

from app import MongoLogAnalyzer
import json

def debug_selectivity():
    analyzer = MongoLogAnalyzer()
    
    print("=== SELECTIVITY DEBUG TOOL ===")
    print("This will help identify why selectivity shows 0%")
    print()
    
    # You can modify this to point to your real log file
    log_file = input("Enter your MongoDB log file path: ").strip()
    
    if not log_file:
        print("No file provided, exiting...")
        return
    
    try:
        print(f"Parsing log file: {log_file}")
        analyzer.parse_log_file(log_file)
        
        print(f"Found {len(analyzer.slow_queries)} slow queries")
        
        if not analyzer.slow_queries:
            print("❌ No slow queries found!")
            return
            
        # Check first few queries
        print("\n=== SAMPLE QUERY ANALYSIS ===")
        for i, query in enumerate(analyzer.slow_queries[:3]):
            print(f"\n--- Query #{i+1} ---")
            print(f"Database: {query.get('database')}")
            print(f"Collection: {query.get('collection')}")
            print(f"Duration: {query.get('duration')}ms")
            print(f"Plan Summary: {query.get('plan_summary')}")
            
            # Check raw extracted values
            print("Raw extracted metrics:")
            print(f"  docsExamined: {query.get('docsExamined')} (type: {type(query.get('docsExamined'))})")
            print(f"  nReturned: {query.get('nReturned')} (type: {type(query.get('nReturned'))})")
            print(f"  keysExamined: {query.get('keysExamined')} (type: {type(query.get('keysExamined'))})")
            
            # Check detailed metrics extraction
            detailed_metrics = analyzer._extract_detailed_metrics(query)
            print("Detailed metrics extraction:")
            print(f"  docsExamined: {detailed_metrics.get('docsExamined')}")
            print(f"  nReturned: {detailed_metrics.get('nReturned')}")
            print(f"  keysExamined: {detailed_metrics.get('keysExamined')}")
            
            # Check original log line
            original_line = analyzer.get_original_log_line(query.get('file_path'), query.get('line_number'))
            if original_line:
                try:
                    original_json = json.loads(original_line)
                    attr = original_json.get('attr', {})
                    print("Original log structure check:")
                    print(f"  attr.docsExamined: {attr.get('docsExamined')}")
                    print(f"  attr.nReturned: {attr.get('nReturned')}")
                    print(f"  attr.command.docsExamined: {attr.get('command', {}).get('docsExamined')}")
                    print(f"  attr.command.nReturned: {attr.get('command', {}).get('nReturned')}")
                except json.JSONDecodeError as e:
                    print(f"  JSON parse error: {e}")
            else:
                print("  Could not retrieve original log line")
                
        # Test pattern analysis
        print("\n=== PATTERN ANALYSIS DEBUG ===")
        patterns = analyzer.analyze_query_patterns()
        
        for pattern_key, pattern in list(patterns.items())[:2]:
            print(f"\n--- Pattern: {pattern_key} ---")
            print(f"Total executions: {pattern.get('total_count')}")
            print(f"Total docs examined: {pattern.get('total_docs_examined')}")
            print(f"Total returned: {pattern.get('total_returned')}")
            print(f"Calculated selectivity: {pattern.get('avg_selectivity')}%")
            
            # Debug individual executions
            print("Individual executions:")
            for j, execution in enumerate(pattern.get('executions', [])[:3]):
                print(f"  Execution {j+1}: docs_examined={execution.get('docs_examined')}, returned={execution.get('returned')}")
                
        print("\n=== RECOMMENDATIONS ===")
        print("If all selectivity values are 0%:")
        print("1. Check if your log format includes docsExamined/nReturned fields")
        print("2. Verify the JSON structure matches what the parser expects")
        print("3. Look for field names with different casing or locations")
        print("4. Check if metrics are nested in different objects")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_selectivity()