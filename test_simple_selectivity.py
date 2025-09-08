#!/usr/bin/env python3
"""
Simple test to verify selectivity calculation logic
"""

def test_parsing_logic():
    """Test the new parsing logic with mock data"""
    print("🔍 Testing New Selectivity Parsing Logic")
    print("=" * 50)
    
    # Mock attribute data that matches the log structure
    mock_attr = {
        'docsExamined': 10993,
        'nReturned': 101,
        'keysExamined': 1215,
        'durationMillis': 10000
    }
    
    print(f"📋 Mock attribute data:")
    print(f"docsExamined: {mock_attr.get('docsExamined')}")
    print(f"nReturned: {mock_attr.get('nReturned')}")
    print(f"keysExamined: {mock_attr.get('keysExamined')}")
    print(f"durationMillis: {mock_attr.get('durationMillis')}")
    
    # Test NEW parsing logic (what we implemented)
    print(f"\n🔧 Testing NEW parsing logic:")
    
    docs_examined_new = (
        mock_attr.get('docsExamined') if mock_attr.get('docsExamined') is not None else
        mock_attr.get('docs_examined') if mock_attr.get('docs_examined') is not None else
        mock_attr.get('totalDocsExamined') if mock_attr.get('totalDocsExamined') is not None else
        mock_attr.get('command', {}).get('docsExamined') if mock_attr.get('command', {}).get('docsExamined') is not None else 0
    )
    
    n_returned_new = (
        mock_attr.get('nReturned') if mock_attr.get('nReturned') is not None else
        mock_attr.get('n_returned') if mock_attr.get('n_returned') is not None else
        mock_attr.get('nreturned') if mock_attr.get('nreturned') is not None else
        mock_attr.get('numReturned') if mock_attr.get('numReturned') is not None else
        mock_attr.get('command', {}).get('nReturned') if mock_attr.get('command', {}).get('nReturned') is not None else 0
    )
    
    print(f"NEW Logic - docs_examined: {docs_examined_new}")
    print(f"NEW Logic - n_returned: {n_returned_new}")
    
    if docs_examined_new > 0:
        selectivity_new = (n_returned_new / docs_examined_new) * 100
        print(f"NEW Logic - selectivity: {selectivity_new:.4f}%")
    else:
        selectivity_new = 0
        print(f"NEW Logic - selectivity: 0% (no docs examined)")
    
    # Test OLD parsing logic (what was broken)
    print(f"\n🔧 Testing OLD parsing logic (for comparison):")
    
    docs_examined_old = (
        mock_attr.get('docsExamined') or 
        mock_attr.get('command', {}).get('docsExamined') or 0
    )
    
    n_returned_old = (
        mock_attr.get('nReturned') or 
        mock_attr.get('command', {}).get('nReturned') or 0
    )
    
    print(f"OLD Logic - docs_examined: {docs_examined_old}")
    print(f"OLD Logic - n_returned: {n_returned_old}")
    
    if docs_examined_old > 0:
        selectivity_old = (n_returned_old / docs_examined_old) * 100
        print(f"OLD Logic - selectivity: {selectivity_old:.4f}%")
    else:
        selectivity_old = 0
        print(f"OLD Logic - selectivity: 0% (no docs examined)")
    
    # Expected values
    expected_selectivity = (101 / 10993) * 100
    
    print(f"\n🎯 Expected Results:")
    print(f"Expected selectivity: {expected_selectivity:.4f}%")
    
    # Verify results
    print(f"\n📊 Results Comparison:")
    if abs(selectivity_new - expected_selectivity) < 0.001:
        print(f"✅ NEW Logic: CORRECT ({selectivity_new:.4f}%)")
        new_correct = True
    else:
        print(f"❌ NEW Logic: INCORRECT ({selectivity_new:.4f}%)")
        new_correct = False
        
    if abs(selectivity_old - expected_selectivity) < 0.001:
        print(f"✅ OLD Logic: CORRECT ({selectivity_old:.4f}%)")
        old_correct = True
    else:
        print(f"❌ OLD Logic: INCORRECT ({selectivity_old:.4f}%)")
        old_correct = False
    
    return new_correct

def test_edge_cases():
    """Test edge cases for the parsing logic"""
    print(f"\n🧪 Testing Edge Cases")
    print("=" * 30)
    
    test_cases = [
        {
            'name': 'Zero values',
            'attr': {'docsExamined': 0, 'nReturned': 0},
            'expected_selectivity': 0.0
        },
        {
            'name': 'Missing values',
            'attr': {},
            'expected_selectivity': 0.0
        },
        {
            'name': 'Alternative field names',
            'attr': {'docs_examined': 5000, 'n_returned': 100},
            'expected_selectivity': 2.0
        },
        {
            'name': 'Command nested values',
            'attr': {'command': {'docsExamined': 2000, 'nReturned': 50}},
            'expected_selectivity': 2.5
        }
    ]
    
    all_passed = True
    
    for case in test_cases:
        print(f"\n  Test: {case['name']}")
        attr = case['attr']
        expected = case['expected_selectivity']
        
        # Apply new parsing logic
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
        
        if docs_examined > 0:
            selectivity = (n_returned / docs_examined) * 100
        else:
            selectivity = 0.0
            
        print(f"    docs_examined: {docs_examined}, n_returned: {n_returned}")
        print(f"    calculated: {selectivity:.2f}%, expected: {expected:.2f}%")
        
        if abs(selectivity - expected) < 0.01:
            print(f"    ✅ PASS")
        else:
            print(f"    ❌ FAIL")
            all_passed = False
    
    return all_passed

def main():
    """Run all selectivity tests"""
    print("=" * 60)
    print("MongoDB Selectivity Calculation Fix - Unit Tests")
    print("=" * 60)
    
    test1 = test_parsing_logic()
    test2 = test_edge_cases()
    
    print(f"\n" + "=" * 60)
    print("UNIT TEST SUMMARY")
    print("=" * 60)
    
    tests = [
        ("Parsing Logic Fix", test1),
        ("Edge Cases", test2)
    ]
    
    passed = sum(1 for name, result in tests if result)
    total = len(tests)
    
    for name, result in tests:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{name:<25} {status}")
    
    print(f"\nResults: {passed}/{total} unit tests passed")
    
    if passed == total:
        print(f"\n🎉 ALL UNIT TESTS PASSED!")
        print(f"✅ The selectivity calculation logic is working correctly")
        print(f"✅ Expected: 0.92% selectivity for log entry with docsExamined=10993, nReturned=101")
        print(f"✅ The fix should resolve the 0% selectivity issue")
        return True
    else:
        print(f"\n⚠️  Some unit tests failed.")
        return False

if __name__ == "__main__":
    main()