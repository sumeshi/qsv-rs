import unittest
from test_base import QsvTestBase

class TestContains(QsvTestBase):
    
    def test_contains_basic(self):
        """Test basic contains functionality"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - contains str foo - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_contains_partial_match(self):
        """Test contains with partial pattern"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - contains str ba - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["foo"]:
            self.assertNotIn(col, result.stdout.strip())
    def test_contains_numeric_column(self):
        """Test contains on numeric column"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - contains col1 4 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
        ]))
        for col in ["foo", "baz"]:
            self.assertNotIn(col, result.stdout.strip())

    def test_contains_datetime_column(self):
        """Test contains on datetime column"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - contains datetime 12:00 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())

    def test_contains_no_matches(self):
        """Test contains with no matches"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - contains str xyz - show")
        self.assertEqual(result.stdout.strip(), "datetime,col1,col2,col3,str")
        for col in ["foo", "bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_contains_case_sensitive(self):
        """Test contains case sensitivity with short name"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - contains str -i FOO - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())

    def test_contains_case_sensitive_with_long_name(self):
        """Test contains case sensitivity with long name"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - contains str --ignorecase FOO - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())

if __name__ == "__main__":
    unittest.main()