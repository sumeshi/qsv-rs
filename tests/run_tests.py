#!/usr/bin/env python3
"""
Run all QSV module tests
"""
import unittest
import sys
import os

# Import all test classes
# Initializers
from test_initializers_load import TestLoad

# Chainables
from test_chainables_select import TestSelect
from test_chainables_head_tail import TestHeadTail
from test_chainables_contains import TestContains
from test_chainables_grep import TestGrep
from test_chainables_changetz import TestChangetz
from test_chainables_isin import TestIsin
from test_chainables_sed import TestSed
from test_chainables_sort import TestSort
from test_chainables_count import TestCount
from test_chainables_uniq import TestUniq
from test_chainables_renamecol import TestRenamecol
from test_chainables_timeline import TestTimeline
from test_chainables_timeslice import TestTimeslice

# Finalizers
from test_finalizers_headers import TestHeaders
from test_finalizers_dump import TestDump
from test_finalizers_stats import TestStats
from test_finalizers_showquery import TestShowquery
from test_finalizers_showtable import TestShowtable

def run_test_suite():
    """Run the complete test suite"""
    # Create a test suite with all tests
    suite = unittest.TestSuite()
    
    print("Adding test cases to suite...")
    
    # Add all test cases
    # Initializers
    suite.addTest(unittest.makeSuite(TestLoad))
    print("  ✓ Added TestLoad")
    
    # Chainables
    suite.addTest(unittest.makeSuite(TestSelect))
    print("  ✓ Added TestSelect")
    suite.addTest(unittest.makeSuite(TestHeadTail))
    print("  ✓ Added TestHeadTail")
    suite.addTest(unittest.makeSuite(TestContains))
    print("  ✓ Added TestContains")
    suite.addTest(unittest.makeSuite(TestGrep))
    print("  ✓ Added TestGrep")
    suite.addTest(unittest.makeSuite(TestChangetz))
    print("  ✓ Added TestChangetz")
    suite.addTest(unittest.makeSuite(TestIsin))
    print("  ✓ Added TestIsin")
    suite.addTest(unittest.makeSuite(TestSed))
    print("  ✓ Added TestSed")
    suite.addTest(unittest.makeSuite(TestSort))
    print("  ✓ Added TestSort")
    suite.addTest(unittest.makeSuite(TestCount))
    print("  ✓ Added TestCount")
    suite.addTest(unittest.makeSuite(TestUniq))
    print("  ✓ Added TestUniq")
    suite.addTest(unittest.makeSuite(TestRenamecol))
    print("  ✓ Added TestRenamecol")
    suite.addTest(unittest.makeSuite(TestTimeline))
    print("  ✓ Added TestTimeline")
    suite.addTest(unittest.makeSuite(TestTimeslice))
    print("  ✓ Added TestTimeslice")
    
    # Finalizers
    suite.addTest(unittest.makeSuite(TestHeaders))
    print("  ✓ Added TestHeaders")
    suite.addTest(unittest.makeSuite(TestDump))
    print("  ✓ Added TestDump")
    suite.addTest(unittest.makeSuite(TestStats))
    print("  ✓ Added TestStats")
    suite.addTest(unittest.makeSuite(TestShowquery))
    print("  ✓ Added TestShowquery")
    suite.addTest(unittest.makeSuite(TestShowtable))
    print("  ✓ Added TestShowtable")
    
    print(f"\nTotal test cases added: {suite.countTestCases()}")
    
    # Run the tests
    print("\nRunning tests...")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print(f"\nFAILURES ({len(result.failures)}):")
        for test, traceback in result.failures:
            print(f"  - {test}")
    
    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for test, traceback in result.errors:
            print(f"  - {test}")
    
    success_rate = (result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100 if result.testsRun > 0 else 0
    print(f"\nSuccess rate: {success_rate:.1f}%")
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_test_suite()
    sys.exit(0 if success else 1)