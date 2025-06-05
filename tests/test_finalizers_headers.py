import unittest
from test_base import QsvTestBase

class TestHeaders(QsvTestBase):
    """
    Test headers finalizer module
    """
    
    def test_headers_basic(self):
        """Test basic headers functionality"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - headers")
        output = result.stdout.strip()
        
        # Should return headers in table format
        self.assertIn("Column Name", output)
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        self.assertIn("str", output)
    
    def test_headers_with_select(self):
        """Test headers after column selection"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,str - headers")
        output = result.stdout.strip()
        
        # Should return only selected column headers in table format
        self.assertIn("Column Name", output)
        self.assertIn("col1", output)
        self.assertIn("str", output)
        self.assertNotIn("datetime", output)
    
    def test_headers_with_renamecol(self):
        """Test headers after column renaming"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - renamecol str text - headers")
        output = result.stdout.strip()
        
        # Should return headers with renamed column in table format
        self.assertIn("Column Name", output)
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        self.assertIn("text", output)
        self.assertNotIn("str", output)
    
    def test_headers_single_column(self):
        """Test headers with single column selection"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select datetime - headers")
        output = result.stdout.strip()
        
        # Should return single column header in table format
        self.assertIn("Column Name", output)
        self.assertIn("datetime", output)
        self.assertNotIn("col1", output)
        
    def test_headers_with_complex_operations(self):
        """Test headers after complex operations (shouldn't affect headers)"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort str - head 2 - headers")
        output = result.stdout.strip()
        
        # Should return all original headers in table format (operations don't change headers)
        self.assertIn("Column Name", output)
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        self.assertIn("str", output)

if __name__ == "__main__":
    unittest.main()