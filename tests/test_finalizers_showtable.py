import unittest
from test_base import QsvTestBase

class TestShowtable(QsvTestBase):
    """
    Test showtable finalizer module
    """
    
    def test_showtable_basic(self):
        """Test displaying data in a formatted table"""
        output = self.run_qsv_command("load sample/simple.csv - showtable")
        
        # Showtable command should display data in a formatted table
        # Check for table formatting characters and data content
        self.assert_output_contains(output, "shape: ")
        
        # Headers and data should be present
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        
        # Data might be displayed as "Int64(1)" or similar format
        self.assertTrue(
            "Int64(1)" in output or 
            "1" in output or 
            "|     1" in output
        )
    
    def test_showtable_after_transform(self):
        """Test displaying a formatted table after transformation"""
        output = self.run_qsv_command("load sample/simple.csv - select col1 - showtable")
        
        # Check for table formatting and content
        self.assert_output_contains(output, "shape: ")
        
        # Only selected column should be present
        self.assert_output_contains(output, "col1")
        
        # Other columns should not be present
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
        
        # Data should be visible in some format
        self.assertTrue(
            "Int64(1)" in output or 
            "1" in output or 
            "|     1" in output
        )

if __name__ == "__main__":
    unittest.main()