import unittest
from test_base import QsvTestBase

class TestSort(QsvTestBase):
    
    def test_sort_by_string_column(self):
        """Test sorting by string column"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort str - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))
    
    def test_sort_by_numeric_column(self):
        """Test sorting by numeric column"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort col1 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
    
    def test_sort_by_datetime_column(self):
        """Test sorting by datetime column"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort datetime - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
    
    def test_sort_reverse_order_with_shortname_option(self):
        """Test sorting in reverse order"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort str -d - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 14:00:00,7,8,9,baz",
            "2023-01-01 13:00:00,4,5,6,bar",
        ]))

    def test_sort_reverse_order_with_longname_option(self):
        """Test sorting in reverse order"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort str --desc - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 14:00:00,7,8,9,baz",
            "2023-01-01 13:00:00,4,5,6,bar",
        ]))

    def test_sort_with_multiple_columns(self):
        """Test sorting with multiple columns"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort str,col1 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
            "2023-01-01 12:00:00,1,2,3,foo",
        ]))

if __name__ == "__main__":
    unittest.main() 