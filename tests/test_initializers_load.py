import unittest
from test_base import QsvTestBase

class TestLoad(QsvTestBase):
    """
    Test load initializer module
    """
    
    def test_load_simple(self):
        """Test loading a simple CSV file"""
        output = self.run_qsv_command("load sample/simple.csv - show")
        
        # Check if the output contains the expected data
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
    
    def test_load_with_separator(self):
        """Test loading with a custom separator"""
        output = self.run_qsv_command("load sample/simple.csv -s=, - show")
        
        # Check if the output contains the expected data
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")

if __name__ == "__main__":
    unittest.main()