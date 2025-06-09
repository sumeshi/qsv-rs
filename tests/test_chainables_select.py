import unittest
from test_base import QsvTestBase

class TestSelect(QsvTestBase):
    
    def test_select_single_column(self):
        """Test selecting a single column"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "col1",
                "1",
                "4",
                "7",
            ])
        )
        for col in ["col2", "col3", "datetime", "str"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_select_multiple_columns_comma_separated(self):
        """Test selecting multiple columns with comma separation"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,col3 - show")
        output = result.stdout.strip()
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "col1,col3",
                "1,3",
                "4,6",
                "7,9",
            ])
        )
        for col in ["col2", "datetime", "str"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_select_column_range_colon(self):
        """Test selecting columns using range notation (col1-col3)"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1:col3 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "col1,col2,col3",
                "1,2,3",
                "4,5,6",
                "7,8,9",
            ])
        )
        for col in ["datetime", "str"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_select_column_range_hyphen(self):
        """Test selecting columns using range notation (col1-col3)"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1-col3 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "col1,col2,col3",
                "1,2,3",
                "4,5,6",
                "7,8,9",
            ])
        )
        for col in ["datetime", "str"]:
            self.assertNotIn(col, result.stdout.strip())

    def test_select_single_numeric_index(self):
        """Test selecting a single column by numeric index (1-based)"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select 1 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "datetime",
                "2023-01-01 12:00:00",
                "2023-01-01 13:00:00",
                "2023-01-01 14:00:00",
            ])
        )
        for col in ["col1", "col2", "col3", "str"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_select_multiple_numeric_indices(self):
        """Test selecting multiple columns by numeric indices (2,4)"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select 2,4 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "col1,col3",
                "1,3",
                "4,6",
                "7,9",
            ])
        )
        for col in ["datetime", "col2", "str"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_select_numeric_range_colon(self):
        """Test selecting columns using numeric colon notation (2:4)"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select 2:4 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "col1,col2,col3",
                "1,2,3",
                "4,5,6",
                "7,8,9",
            ])
        )
        for col in ["datetime", "str"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_select_mixed_numeric_and_column_names(self):
        """Test selecting a mix of numeric indices and column names"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select 2,datetime,4 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "col1,datetime,col3",
                "1,2023-01-01 12:00:00,3",
                "4,2023-01-01 13:00:00,6",
                "7,2023-01-01 14:00:00,9",
            ])
        )
        for col in ["col2", "str"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_select_mixed_numeric_range_and_names(self):
        """Test selecting a mix of numeric ranges and column names"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select 2:3,str - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "col1,col2,str",
                "1,2,foo",
                "4,5,bar",
                "7,8,baz",
            ])
        )
        for col in ["datetime", "col3"]:
            self.assertNotIn(col, result.stdout.strip())

if __name__ == "__main__":
    unittest.main()