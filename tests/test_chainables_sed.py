import unittest
from test_base import QsvTestBase

class TestSed(QsvTestBase):
    """
    Test sed chainable module
    """
    
    def test_sed_basic_replacement(self):
        """Test basic sed functionality with literal replacement"""
        output = self.run_qsv_command("load sample/simple.csv - sed str foo replaced - show")
        
        # Should replace "foo" with "replaced"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,replaced")
        
        # Other rows should remain unchanged
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_sed_regex_pattern(self):
        """Test sed with regex pattern"""
        output = self.run_qsv_command("load sample/simple.csv - sed str 'ba[rz]' REPLACED - show")
        
        # Should replace both "bar" and "baz" with "REPLACED"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,REPLACED")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,REPLACED")
        
        # "foo" should remain unchanged
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
    
    def test_sed_case_sensitive_default(self):
        """Test sed is case-sensitive by default"""
        output = self.run_qsv_command("load sample/simple.csv - sed str FOO replaced - show")
        
        # Should not replace anything since "FOO" != "foo"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_sed_case_insensitive_short_flag(self):
        """Test sed with case-insensitive flag (-i)"""
        output = self.run_qsv_command("load sample/simple.csv - sed str FOO replaced -i - show")
        
        # Should replace "foo" with case-insensitive matching
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,replaced")
        
        # Other rows should remain unchanged
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_sed_case_insensitive_long_flag(self):
        """Test sed with case-insensitive flag (--ignorecase)"""
        output = self.run_qsv_command("load sample/simple.csv - sed str BAR replaced --ignorecase - show")
        
        # Should replace "bar" with case-insensitive matching
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,replaced")
        
        # Other rows should remain unchanged
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_sed_numeric_column(self):
        """Test sed on numeric column"""
        output = self.run_qsv_command("load sample/simple.csv - sed col1 4 999 - show")
        
        # Should replace "4" with "999" in col1
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,999,5,6,bar")
        
        # Other rows should remain unchanged
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_sed_partial_match(self):
        """Test sed with partial string match"""
        output = self.run_qsv_command("load sample/simple.csv - sed str 'o' X - show")
        
        # Should replace all "o" in "foo" with "X", making it "fXX"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,fXX")
        
        # Other rows should remain unchanged
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_sed_no_matches(self):
        """Test sed with pattern that matches nothing"""
        output = self.run_qsv_command("load sample/simple.csv - sed str xyz replaced - show")
        
        # Should not replace anything
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_sed_nonexistent_column(self):
        """Test sed on non-existent column should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - sed nonexistent foo bar - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_sed_empty_replacement(self):
        """Test sed with empty replacement string"""
        output = self.run_qsv_command("load sample/simple.csv - sed str foo '' - show")
        
        # Should replace "foo" with empty string
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,")
        
        # Other rows should remain unchanged
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_sed_wildcard_pattern(self):
        """Test sed with wildcard regex pattern"""
        output = self.run_qsv_command("load sample/simple.csv - sed str '.*o.*' MATCH - show")
        
        # Should replace "foo" (contains 'o') with "MATCH"
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,MATCH")
        
        # Other rows should remain unchanged
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")

if __name__ == "__main__":
    unittest.main() 