import unittest
from test_base import QsvTestBase

class TestTail(QsvTestBase):
    """
    Test tail chainable module
    """
    
    def test_tail_default(self):
        """Test tail with default number of rows (5)"""
        # Since our sample only has 3 rows, we should get all rows
        output = self.run_qsv_command("load sample/simple.csv - tail - show")
        
        # Check if the output contains all rows
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
    
    def test_tail_with_count(self):
        """Test tail with specific number of rows"""
        output = self.run_qsv_command("load sample/simple.csv - tail 2 - show")
        
        # Check if the output contains only the last 2 rows
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
        
        # Make sure the first row is not present
        self.assertNotIn("1,2,3", output)

if __name__ == "__main__":
    unittest.main()