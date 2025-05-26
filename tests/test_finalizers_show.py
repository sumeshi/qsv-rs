import unittest
from test_base import QsvTestBase

class TestShow(QsvTestBase):
    """
    Test show finalizer module
    """
    
    def test_show_basic(self):
        """Test displaying data in a standard format"""
        output = self.run_qsv_command("load sample/simple.csv - show")
        
        # Show command should display the data as CSV
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
    
    def test_show_after_transform(self):
        """Test displaying data after a transformation"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,col3 - show")
        
        # Check if the transformed data is correctly displayed
        self.assert_output_contains(output, "col1,col3")
        self.assert_output_contains(output, "1,3")
        self.assert_output_contains(output, "4,6")
        self.assert_output_contains(output, "7,9")
        
        # col2 should not be present after select
        self.assertNotIn("col2", output)

if __name__ == "__main__":
    unittest.main()