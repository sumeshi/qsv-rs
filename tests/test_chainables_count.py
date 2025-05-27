import unittest
from test_base import QsvTestBase

class TestCount(QsvTestBase):
    """
    Test count chainable module
    """
    
    def test_count_basic(self):
        """Test basic count functionality"""
        output = self.run_qsv_command("load sample/simple.csv - count - show")
        
        # Should add a count column showing frequency of each unique row
        # Since all rows are unique in sample data, each should have count = 1
        self.assert_output_contains(output, "count")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows (all unique)
        self.assertEqual(len(lines), 4)
        
        # Each row should have count = 1
        for line in lines[1:]:  # Skip header
            self.assertIn(",1", line)  # count column should be 1
    
    def test_count_with_duplicates(self):
        """Test count with duplicate data"""
        # Create a command that will generate some duplicates by selecting fewer columns
        output = self.run_qsv_command("load sample/simple.csv - select col1 - count - show")
        
        # Should count unique values in col1 column
        self.assert_output_contains(output, "col1,count")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows (1, 4, 7 are all unique)
        self.assertEqual(len(lines), 4)
        
        # Each unique value should have count = 1
        for line in lines[1:]:  # Skip header
            self.assertIn(",1", line)  # count column should be 1
    
    def test_count_after_filtering(self):
        """Test count after filtering operations"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'ba' - count - show")
        
        # Should filter to "bar" and "baz" rows, then count them
        self.assert_output_contains(output, "count")
        lines = output.strip().split('\n')
        
        # Should have header + 2 data rows (bar and baz)
        self.assertEqual(len(lines), 3)
        
        # Each row should have count = 1 since they're unique
        for line in lines[1:]:  # Skip header
            self.assertIn(",1", line)
    
    def test_count_with_column_selection(self):
        """Test count with specific column selection"""
        output = self.run_qsv_command("load sample/simple.csv - select str - count - show")
        
        # Should count unique values in str column only
        self.assert_output_contains(output, "str,count")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows (foo, bar, baz are all unique)
        self.assertEqual(len(lines), 4)
        
        # Check that we have the expected string values
        output_content = '\n'.join(lines[1:])  # Skip header
        self.assertIn("foo,1", output_content)
        self.assertIn("bar,1", output_content)
        self.assertIn("baz,1", output_content)
    
    def test_count_empty_result(self):
        """Test count with no matching rows"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'xyz' - count - show")
        
        # Should have only header since no rows match
        self.assert_output_contains(output, "count")
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 1)  # Only header
    
    def test_count_preserves_column_order(self):
        """Test that count preserves original column order and adds count at the end"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,str - count - show")
        
        # Should preserve col1,str order and add count column
        lines = output.strip().split('\n')
        header = lines[0]
        self.assertEqual(header, "col1,str,count")
    
    def test_count_with_head_operation(self):
        """Test count combined with head operation"""
        output = self.run_qsv_command("load sample/simple.csv - head 2 - count - show")
        
        # Should take first 2 rows, then count them
        self.assert_output_contains(output, "count")
        lines = output.strip().split('\n')
        
        # Should have header + 2 data rows
        self.assertEqual(len(lines), 3)
        
        # Each row should have count = 1
        for line in lines[1:]:  # Skip header
            self.assertIn(",1", line)
    
    def test_count_numeric_columns(self):
        """Test count with numeric columns"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,col2 - count - show")
        
        # Should count unique combinations of col1,col2
        self.assert_output_contains(output, "col1,col2,count")
        lines = output.strip().split('\n')
        
        # Should have header + 3 data rows (all combinations are unique)
        self.assertEqual(len(lines), 4)
        
        # Check specific combinations
        output_content = '\n'.join(lines[1:])
        self.assertIn("1,2,1", output_content)
        self.assertIn("4,5,1", output_content)
        self.assertIn("7,8,1", output_content)

if __name__ == "__main__":
    unittest.main() 