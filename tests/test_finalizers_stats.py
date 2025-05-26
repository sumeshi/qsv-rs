import unittest
from test_base import QsvTestBase

class TestStats(QsvTestBase):
    """
    Test stats finalizer module
    """
    
    def test_stats_basic(self):
        """Test displaying statistics for all columns"""
        output = self.run_qsv_command("load sample/simple.csv - stats - show")
        
        # Stats should display information about the column types and null counts
        # Check for column headers in the actual stats output
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "datatype")
        self.assert_output_contains(output, "count")
        self.assert_output_contains(output, "null_count")
        
        # Column names should be present
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        
        # Type information should be present
        self.assert_output_contains(output, "i64")
        
        # The Non-Null Count should be 3 for each column (as seen in the actual output)
        self.assert_output_contains(output, " 3      ")

if __name__ == "__main__":
    unittest.main()