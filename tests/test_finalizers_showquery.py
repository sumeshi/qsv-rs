import unittest
from test_base import QsvTestBase

class TestShowquery(QsvTestBase):
    """
    Test showquery finalizer module
    """
    
    def test_showquery_basic(self):
        """Test displaying the query plan"""
        output = self.run_qsv_command("load sample/simple.csv - showquery")
        
        # Showquery should display the query plan
        # This may vary in format, but should contain information about the query
        self.assertNotEqual("", output)
        
        # Check for common query plan elements
        # The actual output format may depend on the underlying engine
        self.assertTrue(
            "plan" in output.lower() or 
            "query" in output.lower() or
            "scan" in output.lower() or
            "dataframe" in output.lower()
        )
    
    def test_showquery_after_operations(self):
        """Test displaying the query plan after operations"""
        output = self.run_qsv_command("load sample/simple.csv - select col1 - sort col1 - showquery")
        
        # Should contain query plan with additional operations
        self.assertNotEqual("", output)
        
        # Check for operation-related terms
        self.assertTrue(
            "select" in output.lower() or 
            "sort" in output.lower() or
            "col1" in output.lower() or
            "projection" in output.lower()
        )

if __name__ == "__main__":
    unittest.main()