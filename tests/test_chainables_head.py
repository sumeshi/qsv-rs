import unittest
from test_base import QsvTestBase

class TestHead(QsvTestBase):
    """
    Test head chainable module
    """
    
    def test_head_default(self):
        """Test head with default number of rows (5)"""
        # Since our sample only has 3 rows, we should get all rows
        output = self.run_qsv_command("load sample/simple.csv - head - show")
        
        # Check if the output contains all rows
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
    
    def test_head_with_count(self):
        """Test head with specific number of rows"""
        output = self.run_qsv_command("load sample/simple.csv - head 2 - show")
        
        # Check if the output contains only the first 2 rows
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        
        # Make sure the third row is not present
        self.assertNotIn("7,8,9", output)

if __name__ == "__main__":
    unittest.main()