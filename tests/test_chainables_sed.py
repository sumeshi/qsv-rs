import unittest
from test_base import QsvTestBase

class TestSed(QsvTestBase):
    
    def test_sed_basic_substitution(self):
        """Test basic sed substitution"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sed foo FOO - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,FOO",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["foo"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_sed_partial_replacement(self):
        """Test sed with partial pattern replacement"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sed ba BA - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,BAr",
            "2023-01-01 14:00:00,7,8,9,BAz",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_sed_column_specific_replacement(self):
        """Test sed with column specific replacement"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sed a A --column str - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bAr",
            "2023-01-01 14:00:00,7,8,9,bAz",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_sed_numeric_replacement(self):
        """Test sed with numeric replacement"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sed 1 ONE - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-0ONE-0ONE ONE2:00:00,ONE,2,3,foo",
            "2023-0ONE-0ONE ONE3:00:00,4,5,6,bar",
            "2023-0ONE-0ONE ONE4:00:00,7,8,9,baz",
        ]))
        for col in ["01", "12", "13", "14"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_sed_regex_replacement(self):
        """Test sed with regex replacement"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sed '..:..:..' 'XX:XX:XX' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 XX:XX:XX,1,2,3,foo",
            "2023-01-01 XX:XX:XX,4,5,6,bar",
            "2023-01-01 XX:XX:XX,7,8,9,baz",
        ]))
        for col in ["12:00:00", "13:00:00", "14:00:00"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_sed_case_insensitive_replacement(self):
        """Test sed with case insensitive replacement"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sed 'FOO' 'FOO' --ignorecase - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,FOO",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["foo"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_sed_no_matches(self):
        """Test sed with pattern that doesn't match"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sed str xyz XYZ - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["xyz"]:
            self.assertNotIn(col, result.stdout.strip())
    
if __name__ == "__main__":
    unittest.main() 