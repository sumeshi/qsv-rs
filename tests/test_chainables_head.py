import unittest
from test_base import QsvTestBase

class TestHead(QsvTestBase):
    
    def test_head_specific_count(self):
        """Test head with specific count"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,col2,col3,str - head 3 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ]))
        self.assertNotIn("2023-01-01 15:00:00", result.stdout)
    
    def test_head_with_shortname_option(self):
        """Test head with shortname option"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,str - head -n 2 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,str",
            "2023-01-01 12:00:00,1,foo",
            "2023-01-01 13:00:00,4,bar",
        ]))
        self.assertNotIn("baz", result.stdout)

    def test_head_with_longname_option(self):
        """Test head with longname option"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,str - head --number 2 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,str",
            "2023-01-01 12:00:00,1,foo",
            "2023-01-01 13:00:00,4,bar",
        ]))
        self.assertNotIn("baz", result.stdout)
    
    def test_head_zero_rows(self):
        """Test head with count of 0"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,str - head 0 - show")
        self.assertEqual(result.stdout.strip(), "datetime,col1,str")
        self.assertNotIn("foo", result.stdout)
    
    def test_head_more_than_available(self):
        """Test head with count larger than available rows"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,str - head 100 - show")
        # Should return all available rows (20 data rows + header)
        lines = result.stdout.strip().split('\n')
        self.assertEqual(len(lines), 21)  # 1 header + 20 data rows
        self.assertIn("datetime,col1,str", result.stdout)
        self.assertIn("foo", result.stdout)
        self.assertIn("Golf", result.stdout)  # Last row

if __name__ == "__main__":
    unittest.main()