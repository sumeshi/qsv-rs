import unittest
from test_base import QsvTestBase

class TestIsin(QsvTestBase):
    """
    Test isin chainable module
    """
    
    def test_isin_basic_single_value(self):
        """Test basic isin functionality with single value"""
        output = self.run_qsv_command("load sample/simple.csv - isin str foo - show")
        
        # Should match the row containing "foo"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        
        # Should not contain other rows
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_isin_multiple_values(self):
        """Test isin with multiple comma-separated values"""
        output = self.run_qsv_command("load sample/simple.csv - isin str foo,bar - show")
        
        # Should match rows containing "foo" or "bar"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        
        # Should not contain "baz" row
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_isin_all_values(self):
        """Test isin with all possible values"""
        output = self.run_qsv_command("load sample/simple.csv - isin str foo,bar,baz - show")
        
        # Should match all rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_isin_numeric_column(self):
        """Test isin on numeric column"""
        output = self.run_qsv_command("load sample/simple.csv - isin col1 1,4 - show")
        
        # Should match rows with col1 = 1 or 4
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        
        # Should not contain row with col1 = 7
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_isin_no_matches(self):
        """Test isin with values that don't exist"""
        output = self.run_qsv_command("load sample/simple.csv - isin str xyz,abc - show")
        
        # Should only have header, no data rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assertNotIn("2023-01-01", output)
    
    def test_isin_nonexistent_column(self):
        """Test isin on non-existent column should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - isin nonexistent foo,bar - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_isin_case_sensitive(self):
        """Test isin is case-sensitive"""
        output = self.run_qsv_command("load sample/simple.csv - isin str FOO,BAR - show")
        
        # Should not match anything since case doesn't match
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assertNotIn("2023-01-01", output)
    
    def test_isin_with_spaces(self):
        """Test isin with values containing spaces (comma-separated parsing)"""
        output = self.run_qsv_command("load sample/simple.csv - isin str 'foo, bar' - show")
        
        # The command parses 'foo, bar' as two values: 'foo' and 'bar' (trimmed)
        # So it should match both foo and bar rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        
        # Should not contain baz row
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_isin_single_numeric_value(self):
        """Test isin with single numeric value"""
        output = self.run_qsv_command("load sample/simple.csv - isin col2 5 - show")
        
        # Should match row with col2 = 5
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        
        # Should not contain other rows
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)

if __name__ == "__main__":
    unittest.main() 