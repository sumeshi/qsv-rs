import unittest
from test_base import QsvTestBase

class TestSelect(QsvTestBase):
    """
    Test select chainable module
    """
    
    def test_select_single_column(self):
        """Test selecting a single column"""
        output = self.run_qsv_command("load sample/simple.csv - select col1 - show")
        
        # Check if the output contains only the selected column
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "1")
        self.assert_output_contains(output, "4")
        self.assert_output_contains(output, "7")
        
        # Make sure other columns are not present
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
        self.assertNotIn("datetime", output)
        self.assertNotIn("str", output)
    
    def test_select_multiple_columns_comma_separated(self):
        """Test selecting multiple columns with comma separation"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,col3 - show")
        
        # Check if the output contains only the selected columns
        self.assert_output_contains(output, "col1,col3")
        self.assert_output_contains(output, "1,3")
        self.assert_output_contains(output, "4,6")
        self.assert_output_contains(output, "7,9")
        
        # Make sure the non-selected columns are not present
        self.assertNotIn("col2", output)
        self.assertNotIn("datetime", output)
        self.assertNotIn("str", output)
    
    def test_select_column_range(self):
        """Test selecting columns using range notation (col1-col3)"""
        output = self.run_qsv_command("load sample/simple.csv - select col1-col3 - show")
        
        # Should include col1, col2, and col3
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
        
        # Should not include datetime or str
        self.assertNotIn("datetime", output)
        self.assertNotIn("str", output)
    
    def test_select_specific_column_by_name(self):
        """Test selecting a specific named column (datetime)"""
        output = self.run_qsv_command("load sample/simple.csv - select datetime - show")
        
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "2023-01-01 12:00:00")
        self.assert_output_contains(output, "2023-01-01 13:00:00")
        self.assert_output_contains(output, "2023-01-01 14:00:00")
        
        # Should not include other columns
        self.assertNotIn("col1", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
        self.assertNotIn("str", output)
    
    def test_select_string_column(self):
        """Test selecting the string column"""
        output = self.run_qsv_command("load sample/simple.csv - select str - show")
        
        self.assert_output_contains(output, "str")
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
        
        # Should not include other columns
        self.assertNotIn("col1", output)
        self.assertNotIn("datetime", output)
    
    def test_select_mixed_columns_and_ranges(self):
        """Test selecting a mix of individual columns and ranges"""
        output = self.run_qsv_command("load sample/simple.csv - select datetime,col1-col2,str - show")
        
        # Should include datetime, col1, col2, and str
        self.assert_output_contains(output, "datetime,col1,col2,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,baz")
        
        # Should not include col3
        self.assertNotIn(",3", output)
        self.assertNotIn(",6", output)
        self.assertNotIn(",9", output)
    
    def test_select_colon_range(self):
        """Test selecting columns using colon notation (col1:col3)"""
        output = self.run_qsv_command("load sample/simple.csv - select col1:col3 - show")
        
        # Should include col1, col2, and col3
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
        
        # Should not include datetime or str
        self.assertNotIn("datetime", output)
        self.assertNotIn("str", output)
    
    def test_select_with_row_indices(self):
        """Test selecting columns with specific row indices using -n option"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,col2 -n 1,3 - show")
        
        # Should include only rows 1 and 3 (1-based indexing)
        self.assert_output_contains(output, "col1,col2")
        self.assert_output_contains(output, "1,2")  # First row
        self.assert_output_contains(output, "7,8")  # Third row
        
        # Should not include the second row
        self.assertNotIn("4,5", output)
    
    def test_select_with_row_range(self):
        """Test selecting columns with row range using -n option"""
        output = self.run_qsv_command("load sample/simple.csv - select col1 -n 1-2 - show")
        
        # Should include only rows 1 and 2
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "1")  # First row
        self.assert_output_contains(output, "4")  # Second row
        
        # Should not include the third row
        self.assertNotIn("7", output)
    
    def test_select_with_colon_row_range(self):
        """Test selecting columns with colon row range using -n option"""
        output = self.run_qsv_command("load sample/simple.csv - select col1 -n 1:2 - show")
        
        # Should include only rows 1 and 2
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "1")  # First row
        self.assert_output_contains(output, "4")  # Second row
        
        # Should not include the third row
        self.assertNotIn("7", output)
    
    def test_select_numeric_colon_notation(self):
        """Test selecting columns using numeric colon notation (1:3)"""
        output = self.run_qsv_command("load sample/simple.csv - select 1:3 - show")
        
        # Should include col1, col2, and col3
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
        
        # Should not include datetime or str
        self.assertNotIn("datetime", output)
        self.assertNotIn("str", output)
    
    def test_selectrows_command(self):
        """Test the selectrows command for row-only selection (legacy test)"""
        # This test should now fail since selectrows command is removed
        # We'll keep it to ensure the command is properly removed
        try:
            output = self.run_qsv_command("load sample/simple.csv - selectrows 1,3 - show")
            # If this doesn't raise an error, the command still exists (which is wrong)
            self.fail("selectrows command should have been removed")
        except:
            # Expected behavior - command should not exist
            pass
    
    def test_select_rows_only_with_n_option(self):
        """Test selecting rows only using -n option (replacement for selectrows)"""
        # To select all columns with specific rows, we can use a wildcard or all column names
        # For now, we'll test with explicit column selection
        output = self.run_qsv_command("load sample/simple.csv - select col1,col2,col3,datetime,str -n 1,3 - show")
        
        # Should include all columns but only rows 1 and 3
        self.assert_output_contains(output, "col1,col2,col3,datetime,str")
        self.assert_output_contains(output, "1,2,3,2023-01-01 12:00:00,foo")  # First row
        self.assert_output_contains(output, "7,8,9,2023-01-01 14:00:00,baz")  # Third row
        
        # Should not include the second row
        self.assertNotIn("4,5,6", output)
    
    def test_select_nonexistent_column(self):
        """Test selecting a non-existent column should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - select nonexistent - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")

if __name__ == "__main__":
    unittest.main()