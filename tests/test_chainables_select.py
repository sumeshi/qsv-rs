import unittest
from test_base import QsvTestBase

class TestSelect(QsvTestBase):
    """
    Test select chainable module
    """
    
    def test_select_single_column(self):
        """Test selecting a single column"""
        output = self.run_qsv_command("load sample/simple.csv - select col1 - show")
        
        # Check if the output contains only the selected column
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "1")
        self.assert_output_contains(output, "4")
        self.assert_output_contains(output, "7")
        
        # Make sure other columns are not present
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
    
    def test_select_multiple_columns(self):
        """Test selecting multiple columns"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,col3 - show")
        
        # Check if the output contains only the selected columns
        self.assert_output_contains(output, "col1,col3")
        self.assert_output_contains(output, "1,3")
        self.assert_output_contains(output, "4,6")
        self.assert_output_contains(output, "7,9")
        
        # Make sure the non-selected column is not present
        self.assertNotIn("col2", output)

if __name__ == "__main__":
    unittest.main()