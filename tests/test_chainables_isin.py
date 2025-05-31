import unittest
from test_base import QsvTestBase

class TestIsin(QsvTestBase):
    
    def test_isin_single_value(self):
        """Test isin with single value"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - isin str foo - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_isin_multiple_values(self):
        """Test isin with multiple values"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - isin str foo,bar - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
        ]))
        for col in ["baz"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_isin_numeric_values(self):
        """Test isin with numeric values"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - isin col1 1,7 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        for col in ["bar"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_isin_datetime_values(self):
        """Test isin with datetime values"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - isin datetime '2023-01-01 12:00:00' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
        for col in ["bar", "baz"]:
            self.assertNotIn(col, result.stdout.strip())

if __name__ == "__main__":
    unittest.main() 