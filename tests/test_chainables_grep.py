import unittest
from test_base import QsvTestBase

class TestGrep(QsvTestBase):
    """
    Test grep chainable module
    """
    
    def test_grep_basic(self):
        """Test basic grep functionality"""
        output = self.run_qsv_command("load sample/simple.csv - grep foo - show")
        
        # Should match the row containing "foo"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        
        # Should not contain other rows
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_grep_regex_pattern(self):
        """Test grep with regex pattern"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'ba[rz]' - show")
        
        # Should match "bar" and "baz" but not "foo"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not contain "foo" row
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
    
    def test_grep_numeric_pattern(self):
        """Test grep with numeric pattern"""
        output = self.run_qsv_command("load sample/simple.csv - grep '4' - show")
        
        # Should match rows containing "4" in any column
        # This includes both "4" in col1 and "14" in datetime
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not contain the row without "4"
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
    
    def test_grep_datetime_pattern(self):
        """Test grep with datetime pattern"""
        output = self.run_qsv_command("load sample/simple.csv - grep '14:00' - show")
        
        # Should match the row with 14:00 in datetime
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not contain other rows
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)
    
    def test_grep_case_sensitive_default(self):
        """Test grep is case-sensitive by default"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'FOO' - show")
        
        # Should not match anything since "FOO" != "foo"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Should only have header, no data rows
        self.assertNotIn("2023-01-01", output)
    
    def test_grep_case_insensitive_short_flag(self):
        """Test grep with case-insensitive flag (-i)"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'FOO' -i - show")
        
        # Should match "foo" with case-insensitive matching
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        
        # Should not contain other rows
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_grep_case_insensitive_long_flag(self):
        """Test grep with case-insensitive flag (--ignorecase)"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'BAR' --ignorecase - show")
        
        # Should match "bar" with case-insensitive matching
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        
        # Should not contain other rows
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_grep_invert_match_short_flag(self):
        """Test grep with invert match flag (-v)"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'foo' -v - show")
        
        # Should match everything except "foo"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not contain "foo" row
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
    
    def test_grep_invert_match_long_flag(self):
        """Test grep with invert match flag (--invert-match)"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'ba[rz]' --invert-match - show")
        
        # Should match everything except "bar" and "baz"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        
        # Should not contain "bar" or "baz" rows
        self.assertNotIn("2023-01-01 13:00:00,4,5,6,bar", output)
        self.assertNotIn("2023-01-01 14:00:00,7,8,9,baz", output)
    
    def test_grep_combined_flags(self):
        """Test grep with combined case-insensitive and invert flags"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'FOO' -i -v - show")
        
        # Should match everything except "foo" (case-insensitive)
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        
        # Should not contain "foo" row
        self.assertNotIn("2023-01-01 12:00:00,1,2,3,foo", output)
    
    def test_grep_anchor_pattern(self):
        """Test grep with anchor pattern (^)"""
        output = self.run_qsv_command("load sample/simple.csv - grep '^2023' - show")
        
        # Should match all rows since they all start with "2023" in datetime column
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_grep_no_matches(self):
        """Test grep with pattern that matches nothing"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'xyz' - show")
        
        # Should only have header, no data rows
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assertNotIn("2023-01-01", output)

if __name__ == "__main__":
    unittest.main()