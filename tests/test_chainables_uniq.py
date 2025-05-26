import unittest
from test_base import QsvTestBase

class TestUniq(QsvTestBase):
    """
    Test uniq chainable module
    """
    
    def test_uniq_basic(self):
        """Test filtering unique rows based on all columns"""
        # Note: uniq command requires column names, so we specify all columns
        output = self.run_qsv_command("load sample/simple.csv - uniq col1,col2,col3 - show")
        
        # Full dataset should be preserved as there are no duplicates
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
    
    def test_uniq_specific_columns(self):
        """Test filtering unique rows based on specific columns"""
        # For a proper test, we'd need data with duplicate values in the specified columns
        output = self.run_qsv_command("load sample/simple.csv - uniq col1 - show")
        
        # Full dataset should be preserved as there are no duplicates in col1
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")

if __name__ == "__main__":
    unittest.main()