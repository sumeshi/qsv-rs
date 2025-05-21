import unittest
from test_base import QsvTestBase

class TestIsin(QsvTestBase):
    """
    Test isin chainable module
    """
    
    def test_isin_single_value(self):
        """Test filtering with a single value"""
        output = self.run_qsv_command("load sample/simple.csv - isin col1 1 - show")
        
        # Check if the output contains only rows where col1 is 1
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        
        # Make sure other rows are not present
        self.assertNotIn("4,5,6", output)
        self.assertNotIn("7,8,9", output)
    
    def test_isin_multiple_values(self):
        """Test filtering with multiple values"""
        output = self.run_qsv_command("load sample/simple.csv - isin col1 1 7 - show")
        
        # Check if the output contains only rows where col1 is 1 or 7
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "7,8,9")
        
        # Make sure other rows are not present
        self.assertNotIn("4,5,6", output)

if __name__ == "__main__":
    unittest.main()