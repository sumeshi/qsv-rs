import unittest
from test_base import QsvTestBase

class TestShowtable(QsvTestBase):
    """
    Test showtable finalizer module
    """
    
    def test_showtable_basic(self):
        """Test basic showtable functionality"""
        output = self.run_qsv_command("load sample/simple.csv - showtable")
        
        # Should display data in table format
        self.assertTrue(len(output) > 0, "Showtable should produce output")
        
        # Should contain column headers
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
        
        # Should contain data values
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
        
        # Table format typically includes borders or separators
        table_chars = ["|", "+", "-", "┌", "┐", "└", "┘", "├", "┤", "┬", "┴", "┼"]
        has_table_char = any(char in output for char in table_chars)
        self.assertTrue(has_table_char, f"Output should contain table formatting characters. Got: {output}")
    
    def test_showtable_after_select(self):
        """Test showtable after column selection"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,str - showtable")
        
        # Should display only selected columns in table format
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "str")
        
        # Should not contain non-selected columns
        self.assertNotIn("datetime", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
        
        # Should contain data values
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
    
    def test_showtable_after_filtering(self):
        """Test showtable after filtering operations"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'ba' - showtable")
        
        # Should display filtered data in table format
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
        
        # Should contain filtered data
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
        
        # Should not contain non-matching data
        self.assertNotIn("foo", output)
    
    def test_showtable_after_head(self):
        """Test showtable after head operation"""
        output = self.run_qsv_command("load sample/simple.csv - head 2 - showtable")
        
        # Should display first 2 rows in table format
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
        
        # Should contain first 2 rows
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        
        # Should not contain third row
        self.assertNotIn("baz", output)
    
    def test_showtable_after_sort(self):
        """Test showtable after sorting operations"""
        output = self.run_qsv_command("load sample/simple.csv - sort str - showtable")
        
        # Should display sorted data in table format
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
        
        # Should contain all data (sorted by str: bar, baz, foo)
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
    
    def test_showtable_empty_result(self):
        """Test showtable with no matching rows"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'xyz' - showtable")
        
        # Should display headers even with no data
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
        
        # Should not contain any data rows
        self.assertNotIn("foo", output)
        self.assertNotIn("bar", output)
        self.assertNotIn("baz", output)
    
    def test_showtable_after_renamecol(self):
        """Test showtable after column renaming"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol str text - showtable")
        
        # Should display data with renamed column
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "text")
        
        # Should not contain old column name
        self.assertNotIn("str", output.split('\n')[0])  # Check header line only
        
        # Should contain data values
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
    
    def test_showtable_complex_chain(self):
        """Test showtable after complex operation chain"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,str - grep 'ba' - sort str - showtable")
        
        # Should display result of complex chain in table format
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "str")
        
        # Should contain filtered and sorted data
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
        
        # Should not contain non-matching data or non-selected columns
        self.assertNotIn("foo", output)
        self.assertNotIn("datetime", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
    
    def test_showtable_numeric_data(self):
        """Test showtable with numeric data"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,col2,col3 - showtable")
        
        # Should display numeric columns in table format
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        
        # Should contain numeric values
        self.assert_output_contains(output, "1")
        self.assert_output_contains(output, "2")
        self.assert_output_contains(output, "3")
        self.assert_output_contains(output, "4")
        self.assert_output_contains(output, "5")
        self.assert_output_contains(output, "6")
        self.assert_output_contains(output, "7")
        self.assert_output_contains(output, "8")
        self.assert_output_contains(output, "9")
    
    def test_showtable_datetime_data(self):
        """Test showtable with datetime data"""
        output = self.run_qsv_command("load sample/simple.csv - select datetime - showtable")
        
        # Should display datetime column in table format
        self.assert_output_contains(output, "datetime")
        
        # Should contain datetime values
        self.assert_output_contains(output, "2023-01-01 12:00:00")
        self.assert_output_contains(output, "2023-01-01 13:00:00")
        self.assert_output_contains(output, "2023-01-01 14:00:00")

if __name__ == "__main__":
    unittest.main() 