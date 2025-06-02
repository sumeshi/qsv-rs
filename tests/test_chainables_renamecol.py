import unittest
from test_base import QsvTestBase

class TestRenamecol(QsvTestBase):
    
    def test_renamecol_basic(self):
        """Test basic column renaming"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - renamecol str text - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,text",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
    
    def test_renamecol_with_spaces(self):
        """Test renaming to column name with spaces"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - renamecol col2 'second column' - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,second column,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))

        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - renamecol col2 'second column' - renamecol 'second column' 'second column 2' - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,second column 2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))

if __name__ == "__main__":
    unittest.main() 