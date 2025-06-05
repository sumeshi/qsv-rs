import unittest
from test_base import QsvTestBase

class TestPivot(QsvTestBase):
    
    def test_pivot_basic_rows_cols(self):
        """Test basic pivot functionality with rows and cols"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --cols product --values sales --agg sum - show")
        self.assertEqual(result.returncode, 0)
        # The output should be CSV with pivoted data
        lines = result.stdout.strip().split('\n')
        self.assertTrue(len(lines) >= 2)  # Header + at least one data row
        self.assertIn('region', lines[0])  # Should contain region column

    def test_pivot_rows_only(self):
        """Test pivot with only rows specified"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --values sales --agg sum - show")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split('\n')
        self.assertTrue(len(lines) >= 2)
        self.assertIn('region', lines[0])

    def test_pivot_cols_only(self):
        """Test pivot with only cols specified"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --cols product --values sales --agg sum - show")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split('\n')
        self.assertTrue(len(lines) >= 2)
        
    def test_pivot_mean_aggregation(self):
        """Test pivot with mean aggregation"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --cols product --values sales --agg mean - show")
        self.assertEqual(result.returncode, 0)
        
    def test_pivot_count_aggregation(self):
        """Test pivot with count aggregation"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --cols product --values sales --agg count - show")
        self.assertEqual(result.returncode, 0)

    def test_pivot_min_aggregation(self):
        """Test pivot with min aggregation"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --cols product --values sales --agg min - show")
        self.assertEqual(result.returncode, 0)

    def test_pivot_max_aggregation(self):
        """Test pivot with max aggregation"""  
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --cols product --values sales --agg max - show")
        self.assertEqual(result.returncode, 0)

    def test_pivot_median_aggregation(self):
        """Test pivot with median aggregation"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --cols product --values sales --agg median - show")
        self.assertEqual(result.returncode, 0)

    def test_pivot_std_aggregation(self):
        """Test pivot with std aggregation"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --cols product --values sales --agg std - show")
        self.assertEqual(result.returncode, 0)

    def test_pivot_multiple_grouping_columns(self):
        """Test pivot with multiple columns for grouping"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region,quarter --cols product --values sales --agg sum - show")
        self.assertEqual(result.returncode, 0)
        
    def test_pivot_missing_values_option(self):
        """Test pivot without --values option should fail"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --cols product - show")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Error:", result.stderr)

    def test_pivot_missing_rows_and_cols(self):
        """Test pivot without both --rows and --cols should fail"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --values sales - show")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Error:", result.stderr)

    def test_pivot_with_specific_expected_output(self):
        """Test pivot with expected output format"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_pivot.csv')} - pivot --rows region --values sales --agg sum - show")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split('\n')
        # Check header
        self.assertIn('region', lines[0])
        self.assertIn('sales_sum', lines[0])
        # Should have data for 4 regions (North, South, East, West)
        self.assertEqual(len(lines), 5)  # Header + 4 regions

if __name__ == "__main__":
    unittest.main() 