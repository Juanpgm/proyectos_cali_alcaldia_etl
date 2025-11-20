# -*- coding: utf-8 -*-
"""
Quick test script for export_to_drive_by_centro_gestor module.
Tests Firebase connection, data fetching, and grouping without uploading to Drive.
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.export_to_drive_by_centro_gestor import (
    fetch_unidades_proyecto_from_firebase,
    group_by_centro_gestor,
    dataframe_to_excel_buffer,
    clean_filename
)


def test_firebase_connection():
    """Test Firebase connection and data fetching."""
    print("\n" + "="*80)
    print("TEST 1: FIREBASE CONNECTION")
    print("="*80)
    
    df = fetch_unidades_proyecto_from_firebase()
    
    if df is None:
        print("❌ Failed to fetch data from Firebase")
        return False
    
    print(f"✅ Successfully fetched {len(df):,} records")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Sample columns: {', '.join(list(df.columns)[:5])}")
    
    return True


def test_grouping():
    """Test data grouping by centro_gestor."""
    print("\n" + "="*80)
    print("TEST 2: DATA GROUPING")
    print("="*80)
    
    df = fetch_unidades_proyecto_from_firebase()
    if df is None:
        print("❌ Cannot test grouping - no data")
        return False
    
    grouped = group_by_centro_gestor(df)
    
    if not grouped:
        print("❌ Grouping failed or returned empty")
        return False
    
    print(f"✅ Successfully grouped into {len(grouped)} centro gestores")
    
    # Show top 5 by record count
    top_5 = sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True)[:5]
    print("\n📊 Top 5 centro gestores by record count:")
    for i, (centro, df_centro) in enumerate(top_5, 1):
        print(f"   {i}. {centro}: {len(df_centro)} registros")
    
    return True


def test_excel_creation():
    """Test Excel file creation in memory."""
    print("\n" + "="*80)
    print("TEST 3: EXCEL FILE CREATION")
    print("="*80)
    
    df = fetch_unidades_proyecto_from_firebase()
    if df is None:
        print("❌ Cannot test Excel creation - no data")
        return False
    
    grouped = group_by_centro_gestor(df)
    if not grouped:
        print("❌ Cannot test Excel creation - no groups")
        return False
    
    # Test with first centro_gestor
    first_centro = list(grouped.keys())[0]
    df_centro = grouped[first_centro]
    
    print(f"Testing Excel creation for: {first_centro}")
    print(f"Records: {len(df_centro)}")
    
    excel_buffer = dataframe_to_excel_buffer(df_centro, sheet_name=first_centro[:31])
    
    if not excel_buffer:
        print("❌ Failed to create Excel buffer")
        return False
    
    buffer_size = len(excel_buffer.getvalue())
    print(f"✅ Successfully created Excel file in memory")
    print(f"   Buffer size: {buffer_size / 1024:.2f} KB")
    
    # Optionally save to disk for inspection
    test_output = Path(__file__).parent.parent / "app_outputs" / "test_excel_export"
    test_output.mkdir(parents=True, exist_ok=True)
    
    safe_name = clean_filename(first_centro)
    test_file = test_output / f"{safe_name}_test.xlsx"
    
    with open(test_file, 'wb') as f:
        excel_buffer.seek(0)
        f.write(excel_buffer.read())
    
    print(f"   Test file saved: {test_file}")
    
    return True


def test_filename_cleaning():
    """Test filename cleaning function."""
    print("\n" + "="*80)
    print("TEST 4: FILENAME CLEANING")
    print("="*80)
    
    test_cases = [
        ("Secretaría de Salud Pública", "Secretaria_de_Salud_Publica"),
        ("Centro Gestor / División", "Centro_Gestor___Division"),
        ("Test: Special * Characters?", "Test__Special___Characters_"),
        ("", "sin_nombre"),
        (None, "sin_nombre")
    ]
    
    all_passed = True
    for original, expected in test_cases:
        cleaned = clean_filename(original)
        status = "✅" if cleaned else "❌"
        print(f"{status} '{original}' -> '{cleaned}'")
        if not cleaned:
            all_passed = False
    
    return all_passed


def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("EXPORT TO DRIVE - TEST SUITE")
    print("="*80)
    print("\nThis script tests the export functionality without uploading to Drive.")
    print("It will test Firebase connection, data grouping, and Excel creation.\n")
    
    results = []
    
    # Test 1: Firebase Connection
    results.append(("Firebase Connection", test_firebase_connection()))
    
    # Test 2: Data Grouping
    results.append(("Data Grouping", test_grouping()))
    
    # Test 3: Excel Creation
    results.append(("Excel Creation", test_excel_creation()))
    
    # Test 4: Filename Cleaning
    results.append(("Filename Cleaning", test_filename_cleaning()))
    
    # Summary
    print("\n" + "="*80)
    print("TEST RESULTS SUMMARY")
    print("="*80)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status}: {test_name}")
    
    total_passed = sum(1 for _, passed in results if passed)
    total_tests = len(results)
    
    print(f"\nTotal: {total_passed}/{total_tests} tests passed")
    
    if total_passed == total_tests:
        print("\n✅ ALL TESTS PASSED - Ready to upload to Drive!")
    else:
        print("\n⚠️  Some tests failed - Please fix issues before uploading to Drive")
    
    return total_passed == total_tests


if __name__ == "__main__":
    """Entry point for test script."""
    success = main()
    sys.exit(0 if success else 1)
