import unittest
from test_base import QsvTestBase

class TestHeadTail(QsvTestBase):
    """
    Test head and tail chainable modules
    """
    
    def test_head_default(self):
        """Test head with default value (5 rows)"""
        output = self.run_qsv_command("load sample/simple.csv - head - show")
        
        # Should include header and all 3 data rows (less than default 5)
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_head_with_number_argument(self):
        """Test head with specific number as positional argument"""
        output = self.run_qsv_command("load sample/simple.csv - head 2 - show")
        
        # Should include header and first 2 data rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        
        # Should not include the third row
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_head_with_number_option_short(self):
        """Test head with -n option"""
        output = self.run_qsv_command("load sample/simple.csv - head -n 1 - show")
        
        # Should include header and only first data row
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        
        # Should not include other rows
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_head_with_number_option_long(self):
        """Test head with --number option"""
        output = self.run_qsv_command("load sample/simple.csv - head --number 2 - show")
        
        # Should include header and first 2 data rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        
        # Should not include the third row
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_tail_default(self):
        """Test tail with default value (5 rows)"""
        output = self.run_qsv_command("load sample/simple.csv - tail - show")
        
        # Should include header and all 3 data rows (less than default 5)
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_tail_with_number_argument(self):
        """Test tail with specific number as positional argument"""
        output = self.run_qsv_command("load sample/simple.csv - tail 2 - show")
        
        # Should include header and last 2 data rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not include the first row
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
    
    def test_tail_with_number_option_short(self):
        """Test tail with -n option"""
        output = self.run_qsv_command("load sample/simple.csv - tail -n 1 - show")
        
        # Should include header and only last data row
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not include other rows
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)
    
    def test_tail_with_number_option_long(self):
        """Test tail with --number option"""
        output = self.run_qsv_command("load sample/simple.csv - tail --number 2 - show")
        
        # Should include header and last 2 data rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not include the first row
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
    
    def test_head_tail_combination(self):
        """Test combining head and tail operations"""
        output = self.run_qsv_command("load sample/simple.csv - head 3 - tail 1 - show")
        
        # Should get the last row of the first 3 rows (which is the 3rd row overall)
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not include other rows
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)

if __name__ == "__main__":
    unittest.main()