import unittest
from test_base import QsvTestBase

class TestStats(QsvTestBase):
    """
    Test stats finalizer module
    """
    
    def test_stats_basic(self):
        """Test basic stats functionality"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - stats")
        output = result.stdout.strip()
        
        # Should return statistics in table format
        self.assertIn("Statistic", output)
        self.assertIn("count", output)
        self.assertIn("mean", output)
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("str", output)
    
    def test_stats_with_select(self):
        """Test stats after column selection"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,col2 - stats")
        output = result.stdout.strip()
        
        # Should return stats for selected columns only
        self.assertIn("Statistic", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertNotIn("datetime", output)
        self.assertNotIn("str", output)
    
    def test_stats_numeric_columns(self):
        """Test stats for numeric columns only"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,col2,col3 - stats")
        output = result.stdout.strip()
        
        # Should return stats with numeric data types
        self.assertIn("Statistic", output)
        self.assertIn("i64", output)  # Integer data type
        self.assertIn("mean", output)
        self.assertIn("std", output)
        lines = output.split('\n')
        self.assertGreater(len(lines), 5)  # Should have multiple stat rows
    
    def test_stats_string_column(self):
        """Test stats for string column"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select str - stats")
        output = result.stdout.strip()
        
        # Should return stats for string column
        self.assertIn("Statistic", output)
        self.assertIn("str", output)
        self.assertIn("count", output)
        # String columns should show min/max values
        self.assertIn("min", output)
        self.assertIn("max", output)
    
    def test_stats_after_filtering(self):
        """Test stats after filtering operations"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep 'ba' - stats")
        output = result.stdout.strip()
        
        # Should return stats for filtered data
        self.assertIn("Statistic", output)
        self.assertIn("count", output)
        # Should show reduced counts due to filtering
        lines = output.split('\n')
        self.assertGreater(len(lines), 3)  # Should have header + multiple stat rows
    
    def test_stats_with_head(self):
        """Test stats after head operation"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - head 2 - stats")
        output = result.stdout.strip()
        
        # Should return stats for first 2 rows only
        self.assertIn("Statistic", output)
        self.assertIn("count", output)
        self.assertIn("2", output)  # Count should be 2
        lines = output.split('\n')
        self.assertGreater(len(lines), 3)  # Should have header + multiple stat rows

if __name__ == "__main__":
    unittest.main() 