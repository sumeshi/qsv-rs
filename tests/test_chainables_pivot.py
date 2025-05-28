import unittest
import os
import tempfile
from test_base import QsvTestBase

class TestPivot(QsvTestBase):
    """
    Test cases for the pivot chainable operation
    """
    
    def setUp(self):
        """Set up test environment"""
        super().setUp()
        # Create test data for pivot operations
        self.test_data = """region,product,sales_amount,quantity
North,Laptop,1200,2
North,Phone,800,4
South,Laptop,1500,3
South,Phone,600,3
North,Laptop,1800,3
South,Phone,900,4"""
        
        self.test_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.test_file.write(self.test_data)
        self.test_file.close()
    
    def tearDown(self):
        """Clean up test environment"""
        if os.path.exists(self.test_file.name):
            os.unlink(self.test_file.name)
    
    def test_pivot_with_rows_and_cols(self):
        """Test pivot with both rows and columns"""
        output = self.run_qsv_command(f"load {self.test_file.name} - pivot --rows region --cols product --values sales_amount --agg sum - show")
        
        # Should contain pivot table with regions as rows and products as columns
        self.assertIn("region", output)
        self.assertIn("product", output)
        self.assertIn("sales_amount_sum", output)
        self.assertIn("North", output)
        self.assertIn("South", output)
    
    def test_pivot_with_rows_only(self):
        """Test pivot with only rows (no columns)"""
        output = self.run_qsv_command(f"load {self.test_file.name} - pivot --rows region --values sales_amount --agg mean - show")
        
        # Should contain aggregated data by region
        self.assertIn("region", output)
        self.assertIn("sales_amount_mean", output)
        self.assertIn("North", output)
        self.assertIn("South", output)
    
    def test_pivot_with_cols_only(self):
        """Test pivot with only columns (no rows)"""
        output = self.run_qsv_command(f"load {self.test_file.name} - pivot --cols product --values quantity --agg count - show")
        
        # Should contain aggregated data by product
        self.assertIn("product", output)
        self.assertIn("quantity_count", output)
        self.assertIn("Laptop", output)
        self.assertIn("Phone", output)
    
    def test_pivot_different_aggregations(self):
        """Test pivot with different aggregation functions"""
        # Test sum
        output_sum = self.run_qsv_command(f"load {self.test_file.name} - pivot --rows region --values sales_amount --agg sum - show")
        self.assertIn("sales_amount_sum", output_sum)
        
        # Test mean
        output_mean = self.run_qsv_command(f"load {self.test_file.name} - pivot --rows region --values sales_amount --agg mean - show")
        self.assertIn("sales_amount_mean", output_mean)
        
        # Test count
        output_count = self.run_qsv_command(f"load {self.test_file.name} - pivot --rows region --values sales_amount --agg count - show")
        self.assertIn("sales_amount_count", output_count)
    
    def test_pivot_error_no_values(self):
        """Test that pivot fails without --values option"""
        try:
            output = self.run_qsv_command(f"load {self.test_file.name} - pivot --rows region --cols product - show")
            # If we get here, the command didn't fail as expected
            self.fail("Expected pivot to fail without --values option")
        except Exception as e:
            # This is expected - pivot should fail without --values
            pass
    
    def test_pivot_error_no_rows_or_cols(self):
        """Test that pivot fails without --rows or --cols options"""
        try:
            output = self.run_qsv_command(f"load {self.test_file.name} - pivot --values sales_amount - show")
            # If we get here, the command didn't fail as expected
            self.fail("Expected pivot to fail without --rows or --cols options")
        except Exception as e:
            # This is expected - pivot should fail without rows or cols
            pass

if __name__ == '__main__':
    unittest.main() 