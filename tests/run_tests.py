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
from test_chainables_isin import TestIsin
from test_chainables_contains import TestContains
from test_chainables_grep import TestGrep
from test_chainables_sort import TestSort
from test_chainables_count import TestCount
from test_chainables_uniq import TestUniq
from test_chainables_sed import TestSed
from test_chainables_renamecol import TestRenamecol
from test_chainables_changetz import TestChangetz

# Finalizers
from test_finalizers_headers import TestHeaders
from test_finalizers_stats import TestStats
from test_finalizers_show import TestShow
from test_finalizers_showtable import TestShowtable
from test_finalizers_showquery import TestShowquery
from test_finalizers_dump import TestDump

# Quilters
from test_quilters_quilt import TestQuilt
from test_quilters_quilt_visualize import TestQuiltVisualize

if __name__ == "__main__":
    # Create a test suite with all tests
    suite = unittest.TestSuite()
    
    # Add all test cases
    # Initializers
    suite.addTest(unittest.makeSuite(TestLoad))
    
    # Chainables
    suite.addTest(unittest.makeSuite(TestSelect))
    suite.addTest(unittest.makeSuite(TestHeadTail))
    suite.addTest(unittest.makeSuite(TestIsin))
    suite.addTest(unittest.makeSuite(TestContains))
    suite.addTest(unittest.makeSuite(TestGrep))
    suite.addTest(unittest.makeSuite(TestSort))
    suite.addTest(unittest.makeSuite(TestCount))
    suite.addTest(unittest.makeSuite(TestUniq))
    suite.addTest(unittest.makeSuite(TestSed))
    suite.addTest(unittest.makeSuite(TestRenamecol))
    suite.addTest(unittest.makeSuite(TestChangetz))
    
    # Finalizers
    suite.addTest(unittest.makeSuite(TestHeaders))
    suite.addTest(unittest.makeSuite(TestStats))
    suite.addTest(unittest.makeSuite(TestShow))
    suite.addTest(unittest.makeSuite(TestShowtable))
    suite.addTest(unittest.makeSuite(TestShowquery))
    suite.addTest(unittest.makeSuite(TestDump))
    
    # Quilters
    suite.addTest(unittest.makeSuite(TestQuilt))
    suite.addTest(unittest.makeSuite(TestQuiltVisualize))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with failure code if any tests failed
    sys.exit(0 if result.wasSuccessful() else 1)