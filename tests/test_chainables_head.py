import unittest
from test_base import QsvTestBase

class TestHead(QsvTestBase):
    
    def test_head_default_count(self):
        """Test head with default count (first 10 rows)"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_manyrows.csv')} - head - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 00:00:00,1,2,3,Alpha",
            "2023-01-01 01:00:00,4,5,6,Bravo",
            "2023-01-01 02:00:00,7,8,9,Charlie",
            "2023-01-01 03:00:00,10,11,12,Delta",
            "2023-01-01 04:00:00,13,14,15,Echo",
        ]))
        for col in ["2023-01-01 05:00:00"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_head_specific_count(self):
        """Test head with specific count"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_manyrows.csv')} - head 3 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 00:00:00,1,2,3,Alpha",
            "2023-01-01 01:00:00,4,5,6,Bravo",
            "2023-01-01 02:00:00,7,8,9,Charlie",
        ]))
        for col in ["2023-01-01 03:00:00"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_head_specific_count_with_shortname_option(self):
        """Test head with specific count and shortname option"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_manyrows.csv')} - head -n 3 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 00:00:00,1,2,3,Alpha",
            "2023-01-01 01:00:00,4,5,6,Bravo",
            "2023-01-01 02:00:00,7,8,9,Charlie",
        ]))
        for col in ["2023-01-01 03:00:00"]:
            self.assertNotIn(col, result.stdout.strip())

    def test_head_specific_count_with_longname_option(self):
        """Test head with specific count and longname option"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_manyrows.csv')} - head --number 3 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 00:00:00,1,2,3,Alpha",
            "2023-01-01 01:00:00,4,5,6,Bravo",
            "2023-01-01 02:00:00,7,8,9,Charlie",
        ]))
        for col in ["2023-01-01 03:00:00"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_head_zero_rows(self):
        """Test head with count of 0"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_manyrows.csv')} - head 0 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
        ]))
        for col in ["2023-01-01 00:00:00"]:
            self.assertNotIn(col, result.stdout.strip())
    
    def test_head_more_than_available(self):
        """Test head with count larger than available rows"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_manyrows.csv')} - head 100 - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 00:00:00,1,2,3,Alpha",
            "2023-01-01 01:00:00,4,5,6,Bravo",
            "2023-01-01 02:00:00,7,8,9,Charlie",
            "2023-01-01 03:00:00,10,11,12,Delta",
            "2023-01-01 04:00:00,13,14,15,Echo",
            "2023-01-01 05:00:00,16,17,18,Foxtrot",
            "2023-01-01 06:00:00,19,20,21,Golf",
            "2023-01-01 07:00:00,22,23,24,Hotel",
            "2023-01-01 08:00:00,25,26,27,India",
            "2023-01-01 09:00:00,28,29,30,Juliett", 
            "2023-01-01 10:00:00,31,32,33,Kilo",
            "2023-01-01 11:00:00,34,35,36,Lima",
            "2023-01-01 12:00:00,37,38,39,Mike",
            "2023-01-01 13:00:00,40,41,42,November",
            "2023-01-01 14:00:00,43,44,45,Oscar",
            "2023-01-01 15:00:00,46,47,48,Papa",
            "2023-01-01 16:00:00,49,50,51,Quebec",
            "2023-01-01 17:00:00,52,53,54,Romeo",
            "2023-01-01 18:00:00,55,56,57,Sierra",
            "2023-01-01 19:00:00,58,59,60,Tango",
            "2023-01-01 20:00:00,61,62,63,Uniform",
            "2023-01-01 21:00:00,64,65,66,Victor",
            "2023-01-01 22:00:00,67,68,69,Whiskey",
            "2023-01-01 23:00:00,70,71,72,Xray",
        ]))

if __name__ == "__main__":
    unittest.main()