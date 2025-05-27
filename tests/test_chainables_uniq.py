import unittest
from test_base import QsvTestBase

class TestUniq(QsvTestBase):
    """
    Test uniq chainable module
    """
    
    def test_uniq_all_columns_default(self):
        """Test uniq with all columns (default behavior)"""
        output = self.run_qsv_command("load sample/simple.csv - uniq - show")
        
        # Since all rows in sample data are unique, should return all rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows (all unique)
        self.assertEqual(len(lines), 4)
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_uniq_single_column(self):
        """Test uniq based on single column"""
        output = self.run_qsv_command("load sample/simple.csv - uniq str - show")
        
        # Should return unique rows based on str column only
        # Since str values are unique (foo, bar, baz), should return all rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows
        self.assertEqual(len(lines), 4)
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
    
    def test_uniq_multiple_columns(self):
        """Test uniq based on multiple columns"""
        output = self.run_qsv_command("load sample/simple.csv - uniq col1,col2 - show")
        
        # Should return unique rows based on col1,col2 combination
        # Since combinations are unique (1,2), (4,5), (7,8), should return all rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows
        self.assertEqual(len(lines), 4)
        self.assert_output_contains(output, "1,2,3,foo")
        self.assert_output_contains(output, "4,5,6,bar")
        self.assert_output_contains(output, "7,8,9,baz")
    
    def test_uniq_after_column_selection(self):
        """Test uniq after selecting specific columns"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,str - uniq col1 - show")
        
        # Should select col1,str then find unique based on col1
        # Since col1 values are unique (1, 4, 7), should return all rows
        self.assert_output_contains(output, "col1,str")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows
        self.assertEqual(len(lines), 4)
        self.assert_output_contains(output, "1,foo")
        self.assert_output_contains(output, "4,bar")
        self.assert_output_contains(output, "7,baz")
    
    def test_uniq_after_filtering(self):
        """Test uniq after filtering operations"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'ba' - uniq str - show")
        
        # Should filter to "bar" and "baz" rows, then find unique by str
        # Since both str values are unique, should return both rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        lines = output.strip().split('\n')
        
        # Should have header + 2 data rows
        self.assertEqual(len(lines), 3)
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
        self.assertNotIn("foo", output)
    
    def test_uniq_numeric_column(self):
        """Test uniq based on numeric column"""
        output = self.run_qsv_command("load sample/simple.csv - uniq col1 - show")
        
        # Should return unique rows based on col1 values
        # Since col1 values are unique (1, 4, 7), should return all rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows
        self.assertEqual(len(lines), 4)
        self.assert_output_contains(output, "1,2,3,foo")
        self.assert_output_contains(output, "4,5,6,bar")
        self.assert_output_contains(output, "7,8,9,baz")
    
    def test_uniq_column_range(self):
        """Test uniq with column range notation"""
        output = self.run_qsv_command("load sample/simple.csv - uniq col1-col3 - show")
        
        # Should return unique rows based on col1,col2,col3 combination
        # Since combinations are unique, should return all rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows
        self.assertEqual(len(lines), 4)
        self.assert_output_contains(output, "1,2,3,foo")
        self.assert_output_contains(output, "4,5,6,bar")
        self.assert_output_contains(output, "7,8,9,baz")
    
    def test_uniq_preserves_order(self):
        """Test that uniq preserves the order of first occurrence"""
        output = self.run_qsv_command("load sample/simple.csv - uniq str - show")
        
        # Should preserve original order: foo, bar, baz
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)
        
        # Check order is preserved
        self.assertIn("foo", lines[1])  # First data row
        self.assertIn("bar", lines[2])  # Second data row
        self.assertIn("baz", lines[3])  # Third data row
    
    def test_uniq_empty_result(self):
        """Test uniq with no matching rows"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'xyz' - uniq - show")
        
        # Should have only header since no rows match
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 1)  # Only header
    
    def test_uniq_nonexistent_column(self):
        """Test uniq with non-existent column should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - uniq nonexistent - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_uniq_with_head_operation(self):
        """Test uniq combined with head operation"""
        output = self.run_qsv_command("load sample/simple.csv - head 2 - uniq str - show")
        
        # Should take first 2 rows, then find unique by str
        # Since first 2 str values are unique (foo, bar), should return both
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        lines = output.strip().split('\n')
        
        # Should have header + 2 data rows
        self.assertEqual(len(lines), 3)
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        self.assertNotIn("baz", output)

if __name__ == "__main__":
    unittest.main() 