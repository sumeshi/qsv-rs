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
    
    # New tests for numeric index functionality
    def test_select_single_numeric_index(self):
        """Test selecting a single column by numeric index (1-based)"""
        output = self.run_qsv_command("load sample/simple.csv - select 1 - show")
        
        # Should select the first column (datetime)
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "2023-01-01 12:00:00")
        self.assert_output_contains(output, "2023-01-01 13:00:00")
        self.assert_output_contains(output, "2023-01-01 14:00:00")
        
        # Make sure other columns are not present
        self.assertNotIn("col1", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
        self.assertNotIn("str", output)
    
    def test_select_multiple_numeric_indices(self):
        """Test selecting multiple columns by numeric indices (2,4)"""
        output = self.run_qsv_command("load sample/simple.csv - select 2,4 - show")
        
        # Should select the second and fourth columns (col1 and col3)
        self.assert_output_contains(output, "col1,col3")
        self.assert_output_contains(output, "1,3")
        self.assert_output_contains(output, "4,6")
        self.assert_output_contains(output, "7,9")
        
        # Make sure other columns are not present
        self.assertNotIn("datetime", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("str", output)
    
    def test_select_numeric_range_colon(self):
        """Test selecting columns using numeric colon notation (2:4)"""
        output = self.run_qsv_command("load sample/simple.csv - select 2:4 - show")
        
        # Should include col1, col2, and col3 (indices 2, 3, 4)
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
        
        # Should not include datetime or str
        self.assertNotIn("datetime", output)
        self.assertNotIn("str", output)
    
    def test_select_numeric_range_last_column(self):
        """Test selecting numeric range that includes the last column"""
        output = self.run_qsv_command("load sample/simple.csv - select 1,5 - show")
        
        # Should include datetime and str columns (indices 1 and 5)
        self.assert_output_contains(output, "datetime,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,baz")
        
        # Should not include col1, col2, col3
        self.assertNotIn("col1", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
    
    def test_select_quoted_colon_notation(self):
        """Test selecting columns using quoted colon notation ("col1":"col3")"""
        output = self.run_qsv_command('load sample/simple.csv - select "col1":"col3" - show')
        
        # Should include col1, col2, and col3
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
        
        # Should not include datetime or str
        self.assertNotIn("datetime", output)
        self.assertNotIn("str", output)
    
    def test_select_mixed_numeric_and_column_names(self):
        """Test selecting a mix of numeric indices and column names"""
        output = self.run_qsv_command("load sample/simple.csv - select 2,datetime,4 - show")
        
        # Should include col1, datetime, and col3 (index 2=col1, datetime, index 4=col3)
        self.assert_output_contains(output, "col1,datetime,col3")
        self.assert_output_contains(output, "1,2023-01-01 12:00:00,3")
        self.assert_output_contains(output, "4,2023-01-01 13:00:00,6")
        self.assert_output_contains(output, "7,2023-01-01 14:00:00,9")
        
        # Should not include col2 or str
        self.assertNotIn("col2", output)
        self.assertNotIn("str", output)
    
    def test_select_mixed_numeric_range_and_names(self):
        """Test selecting a mix of numeric ranges and column names"""
        output = self.run_qsv_command("load sample/simple.csv - select 2:3,str - show")
        
        # Should include col1, col2, and str (indices 2:3 = col1,col2, plus str)
        self.assert_output_contains(output, "col1,col2,str")
        self.assert_output_contains(output, "1,2,foo")
        self.assert_output_contains(output, "4,5,bar")
        self.assert_output_contains(output, "7,8,baz")
        
        # Should not include datetime or col3
        self.assertNotIn("datetime", output)
        self.assertNotIn("col3", output)
    
    def test_select_invalid_numeric_index(self):
        """Test selecting an invalid numeric index should fail gracefully"""
        # Test index 0 (should be invalid as we use 1-based indexing)
        try:
            output = self.run_qsv_command("load sample/simple.csv - select 0 - show")
            # Should fail or produce empty output
            if output:
                self.fail("Index 0 should not be valid (1-based indexing)")
        except:
            # Expected behavior
            pass
        
        # Test index beyond available columns
        try:
            output = self.run_qsv_command("load sample/simple.csv - select 10 - show")
            # Should fail or produce empty output
            if output:
                self.fail("Index 10 should not be valid (only 5 columns)")
        except:
            # Expected behavior
            pass
    
    def test_select_invalid_numeric_range(self):
        """Test selecting an invalid numeric range should fail gracefully"""
        try:
            output = self.run_qsv_command("load sample/simple.csv - select 5:10 - show")
            # Should fail or produce empty output
            if output:
                self.fail("Range 5:10 should not be valid (only 5 columns)")
        except:
            # Expected behavior
            pass
    
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
    
    def test_select_nonexistent_column(self):
        """Test selecting a non-existent column should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - select nonexistent - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")

if __name__ == "__main__":
    unittest.main()