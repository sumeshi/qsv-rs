import unittest
from test_base import QsvTestBase

class TestShowtable(QsvTestBase):
    """
    Test class for showtable finalizer functionality
    """
    
    def test_showtable_basic(self):
        """Test basic showtable functionality"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - showtable")
        output = result.stdout.strip()
        
        # Should display data in table format
        self.assertTrue(len(output) > 0, "Showtable should produce output")
        
        # Should contain column headers
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        self.assertIn("str", output)
        
        # Should contain data values
        self.assertIn("foo", output)
        self.assertIn("bar", output)
        self.assertIn("baz", output)
        
        # Table format typically includes borders or separators
        table_chars = ["|", "+", "-", "┌", "┐", "└", "┘", "├", "┤", "┬", "┴", "┼"]
        has_table_char = any(char in output for char in table_chars)
        self.assertTrue(has_table_char, f"Output should contain table formatting characters. Got: {output}")
    
    def test_showtable_displays_table_size_info(self):
        """Test that showtable displays table size information like Python Polars"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - showtable")
        output = result.stdout.strip()
        
        # Should display shape information like "shape: (3, 5)" for 3 rows × 5 columns
        self.assertTrue(
            any("shape:" in line and "3" in line and "5" in line for line in output.split('\n')),
            f"Output should contain shape information. Got: {output}"
        )
    
    def test_showtable_automatic_default_finalizer(self):
        """Test that showtable is automatically used as default finalizer when no explicit finalizer is specified"""
        # Test without explicit finalizer - should automatically use showtable
        result_implicit = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,col2")
        output_implicit = result_implicit.stdout.strip()
        
        # Test with explicit showtable
        result_explicit = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,col2 - showtable")
        output_explicit = result_explicit.stdout.strip()
        
        # Both should produce similar formatted output
        self.assertTrue(len(output_implicit) > 0, "Implicit showtable should produce output")
        self.assertTrue(len(output_explicit) > 0, "Explicit showtable should produce output")
        
        # Both should contain table formatting
        table_chars = ["|", "+", "-", "┌", "┐", "└", "┘", "├", "┤", "┬", "┴", "┼"]
        has_table_char_implicit = any(char in output_implicit for char in table_chars)
        has_table_char_explicit = any(char in output_explicit for char in table_chars)
        
        self.assertTrue(has_table_char_implicit, "Implicit showtable should contain table formatting")
        self.assertTrue(has_table_char_explicit, "Explicit showtable should contain table formatting")
    
    def test_showtable_small_dataset_no_truncation(self):
        """Test that datasets with 7 or fewer rows show all rows without truncation"""
        # Our sample data has 3 rows, so should show all without truncation
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - showtable")
        output = result.stdout.strip()
        
        # Should contain all 3 data rows
        self.assertIn("foo", output)
        self.assertIn("bar", output)
        self.assertIn("baz", output)
        
        # Should not contain truncation indicators
        self.assertNotIn("…", output)
        self.assertNotIn("...", output)
    
    def test_showtable_after_select(self):
        """Test showtable after column selection"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,str - showtable")
        output = result.stdout.strip()
        
        # Should display only selected columns in table format
        self.assertIn("col1", output)
        self.assertIn("str", output)
        
        # Should display shape information for 3 rows × 2 columns
        self.assertTrue(
            any("shape:" in line and "3" in line and "2" in line for line in output.split('\n')),
            f"Output should contain shape (3, 2). Got: {output}"
        )
        
        # Should not contain non-selected columns
        self.assertNotIn("datetime", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
        
        # Should contain data values
        self.assertIn("foo", output)
        self.assertIn("bar", output)
        self.assertIn("baz", output)
    
    def test_showtable_after_filtering(self):
        """Test showtable after filtering operations"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep 'ba' - showtable")
        output = result.stdout.strip()
        
        # Should display filtered data in table format
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        self.assertIn("str", output)
        
        # Should display shape information for 2 rows × 5 columns (filtered to 'bar' and 'baz')
        self.assertTrue(
            any("shape:" in line and "2" in line and "5" in line for line in output.split('\n')),
            f"Output should contain shape (2, 5). Got: {output}"
        )
        
        # Should contain filtered data
        self.assertIn("bar", output)
        self.assertIn("baz", output)
        
        # Should not contain non-matching data
        self.assertNotIn("foo", output)
    
    def test_showtable_after_head(self):
        """Test showtable after head operation"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - head 2 - showtable")
        output = result.stdout.strip()
        
        # Should display first 2 rows in table format
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        self.assertIn("str", output)
        
        # Should display shape information for 2 rows × 5 columns
        self.assertTrue(
            any("shape:" in line and "2" in line and "5" in line for line in output.split('\n')),
            f"Output should contain shape (2, 5). Got: {output}"
        )
        
        # Should contain first 2 rows
        self.assertIn("foo", output)
        self.assertIn("bar", output)
        
        # Should not contain third row
        self.assertNotIn("baz", output)
    
    def test_showtable_after_sort(self):
        """Test showtable after sorting operations"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort str - showtable")
        output = result.stdout.strip()
        
        # Should display sorted data in table format
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        self.assertIn("str", output)
        
        # Should display shape information for 3 rows × 5 columns
        self.assertTrue(
            any("shape:" in line and "3" in line and "5" in line for line in output.split('\n')),
            f"Output should contain shape (3, 5). Got: {output}"
        )
        
        # Should contain all data (sorted by str: bar, baz, foo)
        self.assertIn("foo", output)
        self.assertIn("bar", output)
        self.assertIn("baz", output)
    
    def test_showtable_empty_result(self):
        """Test showtable with no matching rows"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep 'xyz' - showtable")
        output = result.stdout.strip()
        
        # Should display headers even with no data
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        self.assertIn("str", output)
        
        # Should display shape information for 0 rows × 5 columns
        self.assertTrue(
            any("shape:" in line and "0" in line and "5" in line for line in output.split('\n')),
            f"Output should contain shape (0, 5). Got: {output}"
        )
        
        # Should not contain any data rows
        self.assertNotIn("foo", output)
        self.assertNotIn("bar", output)
        self.assertNotIn("baz", output)
    
    def test_showtable_after_renamecol(self):
        """Test showtable after column renaming"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - renamecol str text - showtable")
        output = result.stdout.strip()
        
        # Should display data with renamed column
        self.assertIn("datetime", output)
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        self.assertIn("text", output)
        
        # Should not contain old column name
        self.assertNotIn("str", output.split('\n')[0])  # Check header line only
        
        # Should contain data values
        self.assertIn("foo", output)
        self.assertIn("bar", output)
        self.assertIn("baz", output)
    
    def test_showtable_complex_chain(self):
        """Test showtable after complex operation chain"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,str - grep 'ba' - sort str - showtable")
        output = result.stdout.strip()
        
        # Should display result of complex operations in table format
        self.assertIn("col1", output)
        self.assertIn("str", output)
        
        # Should display shape information for 2 rows × 2 columns (selected col1,str then filtered 'ba')
        self.assertTrue(
            any("shape:" in line and "2" in line and "2" in line for line in output.split('\n')),
            f"Output should contain shape (2, 2). Got: {output}"
        )
        
        # Should contain filtered and sorted data (bar, baz)
        self.assertIn("bar", output)
        self.assertIn("baz", output)
        
        # Should not contain non-matching data or non-selected columns
        self.assertNotIn("foo", output)
        self.assertNotIn("datetime", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
    
    def test_showtable_numeric_data(self):
        """Test showtable with numeric data"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,col2,col3 - showtable")
        output = result.stdout.strip()
        
        # Should display numeric columns
        self.assertIn("col1", output)
        self.assertIn("col2", output)
        self.assertIn("col3", output)
        
        # Should display shape information for 3 rows × 3 columns
        self.assertTrue(
            any("shape:" in line and "3" in line and "3" in line for line in output.split('\n')),
            f"Output should contain shape (3, 3). Got: {output}"
        )
        
        # Should contain numeric values
        self.assertIn("1", output)
        self.assertIn("2", output)
        self.assertIn("3", output)
        self.assertIn("4", output)
        self.assertIn("5", output)
        self.assertIn("6", output)
        self.assertIn("7", output)
        self.assertIn("8", output)
        self.assertIn("9", output)
    
    def test_showtable_datetime_data(self):
        """Test showtable with datetime data"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select datetime - showtable")
        output = result.stdout.strip()
        
        # Should display datetime column
        self.assertIn("datetime", output)
        
        # Should display shape information for 3 rows × 1 column
        self.assertTrue(
            any("shape:" in line and "3" in line and "1" in line for line in output.split('\n')),
            f"Output should contain shape (3, 1). Got: {output}"
        )
        
        # Should contain datetime values
        self.assertIn("2023-01-01 12:00:00", output)
        self.assertIn("2023-01-01 13:00:00", output)
        self.assertIn("2023-01-01 14:00:00", output)

if __name__ == "__main__":
    unittest.main() 