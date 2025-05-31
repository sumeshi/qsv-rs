import unittest
from test_base import QsvTestBase

class TestGrep(QsvTestBase):
    
    def test_grep_basic_pattern(self):
        """Test basic grep functionality with simple pattern"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep 'foo' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_grep_partial_match(self):
        """Test grep with partial pattern matching"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep 'ba' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["foo"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_grep_numeric_pattern(self):
        """Test grep with numeric pattern"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep '4' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["foo"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_grep_datetime_pattern(self):
        """Test grep with datetime pattern"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep '12:00' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_grep_case_insensitive(self):
        """Test grep with case insensitive matching"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep -i 'FOO' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_grep_case_insensitive_with_long_name(self):
        """Test grep with case insensitive matching with long name"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep --ignorecase 'FOO' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_grep_invert_match(self):
        """Test grep with invert match"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep -v 'foo' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["foo"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_grep_invert_match_with_case_insensitive(self):
        """Test grep with invert match with case insensitive"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep -v -i 'FOO' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["foo"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_grep_invert_match_with_case_insensitive_with_long_name(self):
        """Test grep with invert match with case insensitive with long name"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep -v --ignorecase 'FOO' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["foo"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_grep_no_matches(self):
        """Test grep with pattern that matches nothing"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep 'xyz' - show")
        self.assertEqual(result.stdout.strip(), "datetime,col1,col2,col3,str")
        for col in ["foo", "bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())

if __name__ == "__main__":
    unittest.main()