import unittest
from test_base import QsvTestBase

class TestContains(QsvTestBase):
    """
    Test contains chainable module
    """
    
    def test_contains_basic(self):
        """Test basic contains functionality"""
        output = self.run_qsv_command("load sample/simple.csv - contains str ba - show")
        
        # Should match "bar" and "baz" but not "foo"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not contain "foo" row
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
    
    def test_contains_exact_match(self):
        """Test contains with exact substring match"""
        output = self.run_qsv_command("load sample/simple.csv - contains str foo - show")
        
        # Should match only "foo"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        
        # Should not contain "bar" or "baz" rows
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_contains_case_sensitive_default(self):
        """Test contains is case-sensitive by default"""
        output = self.run_qsv_command("load sample/simple.csv - contains str BA - show")
        
        # Should not match anything since "BA" != "ba"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Should only have header, no data rows
        self.assertNotIn("2023-01-01", output)
    
    def test_contains_case_insensitive_short_flag(self):
        """Test contains with case-insensitive flag (-i)"""
        output = self.run_qsv_command("load sample/simple.csv - contains str BA -i - show")
        
        # Should match "bar" and "baz" with case-insensitive matching
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not contain "foo" row
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
    
    def test_contains_case_insensitive_long_flag(self):
        """Test contains with case-insensitive flag (--ignorecase)"""
        output = self.run_qsv_command("load sample/simple.csv - contains str FOO --ignorecase - show")
        
        # Should match "foo" with case-insensitive matching
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        
        # Should not contain "bar" or "baz" rows
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_contains_numeric_column(self):
        """Test contains on numeric column"""
        output = self.run_qsv_command("load sample/simple.csv - contains col1 4 - show")
        
        # Should match the row with col1=4
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        
        # Should not contain other rows
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_contains_datetime_column(self):
        """Test contains on datetime column"""
        output = self.run_qsv_command("load sample/simple.csv - contains datetime '13:00' - show")
        
        # Should match the row with 13:00 in datetime
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        
        # Should not contain other rows
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_contains_no_matches(self):
        """Test contains with pattern that matches nothing"""
        output = self.run_qsv_command("load sample/simple.csv - contains str xyz - show")
        
        # Should only have header, no data rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assertNotIn("2023-01-01", output)
    
    def test_contains_nonexistent_column(self):
        """Test contains on non-existent column should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - contains nonexistent pattern - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")

if __name__ == "__main__":
    unittest.main()