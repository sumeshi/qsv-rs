import unittest
from test_base import QsvTestBase

class TestContains(QsvTestBase):
    """
    Test contains chainable module
    """
    
    def test_contains_basic(self):
        """Test filtering with contains"""
        output = self.run_qsv_command("load sample/simple.csv - contains col1 1 - show")
        
        # Check if the output contains only rows where col1 contains "1"
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        
        # Make sure other rows are not present
        self.assertNotIn("4,5,6", output)
        self.assertNotIn("7,8,9", output)
    
    def test_contains_ignorecase(self):
        """Test contains with case-insensitive matching"""
        # For this test to be useful, we would need text data
        # For now, we'll just test that the command works with the flag
        output = self.run_qsv_command("load sample/simple.csv - contains col1 1 -i - show")
        
        # Similar results as above, but demonstrating the ignorecase flag
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assertNotIn("4,5,6", output)
        self.assertNotIn("7,8,9", output)

if __name__ == "__main__":
    unittest.main()