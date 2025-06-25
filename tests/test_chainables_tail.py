import unittest
from test_base import QsvTestBase

class TestTail(QsvTestBase):
    
    def test_tail_specific_count(self):
        """Test tail with specific count"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,col2,col3,str - tail 3 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-02 05:00:00,52,53,54,Echo",
            "2023-01-02 06:00:00,55,56,57,Foxtrot",
            "2023-01-02 07:00:00,58,59,60,Golf",
        ]))
        self.assertNotIn("2023-01-02 04:00:00", result.stdout)
    
    def test_tail_with_shortname_option(self):
        """Test tail with shortname option"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,str - tail -n 2 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,str",
            "2023-01-02 06:00:00,55,Foxtrot",
            "2023-01-02 07:00:00,58,Golf",
        ]))
        self.assertNotIn("Echo", result.stdout)

    def test_tail_with_longname_option(self):
        """Test tail with longname option"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,str - tail --number 2 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,str",
            "2023-01-02 06:00:00,55,Foxtrot",
            "2023-01-02 07:00:00,58,Golf",
        ]))
        self.assertNotIn("Echo", result.stdout)
    
    def test_tail_zero_rows(self):
        """Test tail with count of 0"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,str - tail 0 - show")
        self.assertEqual(result.stdout.strip(), "datetime,col1,str")
        self.assertNotIn("foo", result.stdout)
    
    def test_tail_more_than_available(self):
        """Test tail with count larger than available rows"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('comprehensive.csv')} - select datetime,col1,str - tail 100 - show")
        # Should return all available rows (20 data rows + header)
        lines = result.stdout.strip().split('\n')
        self.assertEqual(len(lines), 21)  # 1 header + 20 data rows
        self.assertIn("datetime,col1,str", result.stdout)
        self.assertIn("foo", result.stdout)
        self.assertIn("Golf", result.stdout)

if __name__ == "__main__":
    unittest.main()