import unittest
from test_base import QsvTestBase

class TestCount(QsvTestBase):
    
    def test_count_basic(self):
        """Test basic count functionality"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_timeline.csv')} - count - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,str,count",
            "2023-01-01 12:00:00,Mike,13",
            "2023-01-01 11:00:00,Lima,12",
            "2023-01-01 10:00:00,Kilo,11",
            "2023-01-01 09:00:00,Juliett,10",
            "2023-01-01 08:00:00,India,9",
            "2023-01-01 07:00:00,Hotel,8",
            "2023-01-01 06:00:00,Golf,7",
            "2023-01-01 05:00:00,Foxtrot,6",
            "2023-01-01 04:00:00,Echo,5",
            "2023-01-01 03:00:00,Delta,4",
            "2023-01-01 02:00:00,Charlie,3",
            "2023-01-01 01:00:00,Bravo,2",
            "2023-01-01 00:00:00,Alpha,1",
        ]))
    
if __name__ == "__main__":
    unittest.main() 