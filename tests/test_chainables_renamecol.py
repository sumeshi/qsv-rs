import unittest
from test_base import QsvTestBase

class TestRenamecol(QsvTestBase):
    """
    Test renamecol chainable module
    """
    
    def test_renamecol_basic(self):
        """Test basic column renaming functionality"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol str string_column - show")
        
        # Should rename "str" column to "string_column"
        self.assert_output_contains(output, "datetime,col1,col2,col3,string_column")
        
        # Data should remain the same
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Old column name should not appear in header (check for exact column name)
        lines = output.split('\n')
        header_line = lines[0] if lines else ""
        self.assertNotIn(",str,", header_line)  # Check for exact column match
        self.assertFalse(header_line.endswith(",str"))  # Check if it's the last column
    
    def test_renamecol_numeric_column(self):
        """Test renaming numeric column"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol col1 first_column - show")
        
        # Should rename "col1" to "first_column"
        self.assert_output_contains(output, "datetime,first_column,col2,col3,str")
        
        # Data should remain the same
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_renamecol_datetime_column(self):
        """Test renaming datetime column"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol datetime timestamp - show")
        
        # Should rename "datetime" to "timestamp"
        self.assert_output_contains(output, "timestamp,col1,col2,col3,str")
        
        # Data should remain the same
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_renamecol_with_spaces(self):
        """Test renaming column to name with spaces"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol str 'string value' - show")
        
        # Should rename "str" to "string value"
        self.assert_output_contains(output, "datetime,col1,col2,col3,string value")
        
        # Data should remain the same
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
    
    def test_renamecol_with_special_characters(self):
        """Test renaming column to name with special characters"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol col1 'col_1_new' - show")
        
        # Should rename "col1" to "col_1_new"
        self.assert_output_contains(output, "datetime,col_1_new,col2,col3,str")
        
        # Data should remain the same
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
    
    def test_renamecol_after_column_selection(self):
        """Test renaming column after selecting specific columns"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,str - renamecol str text - show")
        
        # Should select columns then rename "str" to "text"
        self.assert_output_contains(output, "col1,text")
        
        # Should have only selected columns with renamed header
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assert_output_contains(output, "1,foo")
        self.assert_output_contains(output, "4,bar")
        self.assert_output_contains(output, "7,baz")
    
    def test_renamecol_after_filtering(self):
        """Test renaming column after filtering operations"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'ba' - renamecol str text - show")
        
        # Should filter then rename "str" to "text"
        self.assert_output_contains(output, "datetime,col1,col2,col3,text")
        
        # Should have filtered data with renamed column
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 3)  # header + 2 data rows
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
        self.assertNotIn("foo", output)
    
    def test_renamecol_multiple_operations(self):
        """Test multiple column renaming operations"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol col1 first - renamecol str text - show")
        
        # Should rename both columns
        self.assert_output_contains(output, "datetime,first,col2,col3,text")
        
        # Data should remain the same
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_renamecol_nonexistent_column(self):
        """Test renaming non-existent column should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol nonexistent new_name - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_renamecol_same_name(self):
        """Test renaming column to the same name"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol str str - show")
        
        # Should work but effectively do nothing
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        
        # Data should remain the same
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_renamecol_with_head_operation(self):
        """Test renaming column combined with head operation"""
        output = self.run_qsv_command("load sample/simple.csv - head 2 - renamecol str text - show")
        
        # Should take first 2 rows then rename column
        self.assert_output_contains(output, "datetime,col1,col2,col3,text")
        
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 3)  # header + 2 data rows
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        self.assertNotIn("baz", output)
    
    def test_renamecol_preserves_data_types(self):
        """Test that renaming preserves data types and values"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol col1 number - show")
        
        # Should preserve numeric values in renamed column
        self.assert_output_contains(output, "datetime,number,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")

if __name__ == "__main__":
    unittest.main() 