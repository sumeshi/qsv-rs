#!/usr/bin/env python3
import unittest
import sys
import os

# Initializers
from test_initializers_load import TestLoad

# Chainables
from test_chainables_select import TestSelect
from test_chainables_head import TestHead
from test_chainables_tail import TestTail
from test_chainables_contains import TestContains
from test_chainables_grep import TestGrep
from test_chainables_changetz import TestChangetz
from test_chainables_isin import TestIsin
from test_chainables_sed import TestSed
from test_chainables_sort import TestSort
from test_chainables_count import TestCount
from test_chainables_uniq import TestUniq
from test_chainables_renamecol import TestRenamecol
from test_chainables_convert import TestConvert
from test_chainables_pivot import TestPivot
from test_chainables_timeline import TestTimeline
from test_chainables_timeslice import TestTimeslice
from test_chainables_timeround import TestTimeround

# Finalizers
from test_finalizers_headers import TestHeaders
from test_finalizers_dump import TestDump
from test_finalizers_stats import TestStats
from test_finalizers_partition import TestPartition
from test_finalizers_showquery import TestShowquery
from test_finalizers_showtable import TestShowtable

def run_test_suite():
    suite = unittest.TestSuite()
    
    # Initializers
    initializers = [
        TestLoad,
    ]
    for initializer in initializers:
        suite.addTest(unittest.makeSuite(initializer))
    
    # Chainables
    chainables = [
        TestSelect,
        TestHead,
        TestTail,
        TestContains,
        TestGrep,
        TestChangetz,
        TestIsin,
        TestSed,
        TestSort,
        TestCount,
        TestUniq,
        TestRenamecol,
        TestConvert,
        TestPivot,
        TestTimeline,
        TestTimeslice,
        TestTimeround,
    ]
    for chainable in chainables:
        suite.addTest(unittest.makeSuite(chainable))
    
    # Finalizers
    finalizers = [
        TestHeaders,
        TestDump,
        TestStats,
        TestPartition,
        TestShowquery,
        TestShowtable,
    ]
    for finalizer in finalizers:
        suite.addTest(unittest.makeSuite(finalizer))
    
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