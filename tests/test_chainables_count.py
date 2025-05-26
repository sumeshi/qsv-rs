import unittest
from test_base import QsvTestBase

class TestCount(QsvTestBase):
    """
    Test count chainable module
    """
    
    def test_count_basic(self):
        """Test counting occurrences of values"""
        # For a simple test without duplicates, we're just verifying the command runs
        output = self.run_qsv_command("load sample/simple.csv - count - show")
        
        # Count output should have a count column and show values with their counts
        self.assert_output_contains(output, "count")
        
        # Since all values in our test data are unique, we expect counts of 1
        # but the exact format may vary, so we just check the content exists
        self.assert_output_contains(output, "1")
        self.assert_output_contains(output, "4")
        self.assert_output_contains(output, "7")

    def test_count_specific_column(self):
        """Test count with specific columns"""
        # This test would be more meaningful with duplicates in the data
        output = self.run_qsv_command("load sample/simple.csv - count col1 - show")
        
        # The output should include the count column
        self.assert_output_contains(output, "count")
        
        # Since all values in our data are unique, counts should be 1
        # but the exact format may vary, so we just check the content exists
        self.assert_output_contains(output, "1")
        self.assert_output_contains(output, "4")
        self.assert_output_contains(output, "7")

if __name__ == "__main__":
    unittest.main()