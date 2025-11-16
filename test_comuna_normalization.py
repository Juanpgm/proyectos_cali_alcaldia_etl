"""
Test script for comuna normalization functionality
"""
import sys
import os

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

from data_transformation_unidades_proyecto import normalize_comuna_value

def test_comuna_normalization():
    """Test the comuna normalization function"""
    print("\n=== Testing normalize_comuna_value function ===")
    
    # Test cases: (input, expected_output, description)
    test_cases = [
        ("COMUNA 1", "COMUNA 01", "Single digit 1"),
        ("COMUNA 2", "COMUNA 02", "Single digit 2"),
        ("COMUNA 3", "COMUNA 03", "Single digit 3"),
        ("COMUNA 4", "COMUNA 04", "Single digit 4"),
        ("COMUNA 5", "COMUNA 05", "Single digit 5"),
        ("COMUNA 6", "COMUNA 06", "Single digit 6"),
        ("COMUNA 7", "COMUNA 07", "Single digit 7"),
        ("COMUNA 8", "COMUNA 08", "Single digit 8"),
        ("COMUNA 9", "COMUNA 09", "Single digit 9"),
        ("COMUNA 10", "COMUNA 10", "Double digit (no change)"),
        ("COMUNA 11", "COMUNA 11", "Double digit (no change)"),
        ("COMUNA 15", "COMUNA 15", "Double digit (no change)"),
        ("COMUNA 20", "COMUNA 20", "Double digit (no change)"),
        ("comuna 1", "COMUNA 01", "Lowercase input"),
        ("Comuna 5", "COMUNA 05", "Mixed case input"),
        ("CORREGIMIENTO PANCE", "CORREGIMIENTO PANCE", "Corregimiento (no change)"),
        ("RURAL LA BUITRERA", "RURAL LA BUITRERA", "Rural (no change)"),
        (None, None, "None input"),
        ("", "", "Empty string"),
    ]
    
    passed = 0
    failed = 0
    
    for input_val, expected, description in test_cases:
        result = normalize_comuna_value(input_val)
        if result == expected:
            print(f"  ✓ {description}: '{input_val}' → '{result}'")
            passed += 1
        else:
            print(f"  ✗ {description}: '{input_val}' → '{result}' (expected '{expected}')")
            failed += 1
    
    print(f"\n  Results: {passed} passed, {failed} failed")
    return failed == 0

if __name__ == "__main__":
    print("Testing Comuna Normalization Functionality")
    print("=" * 60)
    
    try:
        success = test_comuna_normalization()
        
        print("\n" + "=" * 60)
        if success:
            print("✓ All tests passed!")
        else:
            print("✗ Some tests failed!")
        
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
