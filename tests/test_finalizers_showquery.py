import unittest
from test_base import QsvTestBase

class TestShowquery(QsvTestBase):
    
    def test_showquery_basic(self):
        """Test basic showquery functionality"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - showquery")
        output = result.stdout.strip()
        
        # Should show the query plan in Polars format
        self.assertIn("Csv SCAN", output)
        self.assertIn("simple.csv", output)
        self.assertIn("PROJECT", output)
    
    def test_showquery_with_filtering(self):
        """Test showquery with filtering operations"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep 'foo' - showquery")
        output = result.stdout.strip()
        
        # Should show filtering in query plan
        self.assertIn("FILTER", output)
        self.assertIn("Csv SCAN", output)
        self.assertIn("simple.csv", output)
    
    def test_showquery_with_sort(self):
        """Test showquery with sorting operations"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort str - showquery")
        output = result.stdout.strip()
        
        # Should show sorting in query plan
        self.assertIn("SORT BY", output)
        self.assertIn("Csv SCAN", output)
        self.assertIn("simple.csv", output)
    
    def test_showquery_with_head(self):
        """Test showquery with head operation"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - head 2 - showquery")
        output = result.stdout.strip()
        
        # Should show limit/slice in query plan
        self.assertIn("SLICE", output)
        self.assertIn("Csv SCAN", output)
        self.assertIn("simple.csv", output)
    
    def test_showquery_complex_chain(self):
        """Test showquery with complex operation chain"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,str - grep 'foo' - sort str - head 1 - showquery")
        output = result.stdout.strip()
        
        # Should show all operations in query plan
        self.assertIn("SLICE", output)  # head operation
        self.assertIn("SORT BY", output)  # sort operation
        self.assertIn("FILTER", output)  # grep operation
        self.assertIn("Csv SCAN", output)
        self.assertIn("simple.csv", output)

if __name__ == "__main__":
    unittest.main() 