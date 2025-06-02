import unittest
from test_base import QsvTestBase

class TestUniq(QsvTestBase):
    
    def test_uniq_all_columns_default(self):
        """Test uniq with all columns (default behavior)"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_timeline.csv')} - uniq - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
            "datetime,str",
            "2023-01-01 00:00:00,Alpha",
            "2023-01-01 01:00:00,Bravo",
            "2023-01-01 02:00:00,Charlie",
            "2023-01-01 03:00:00,Delta",
            "2023-01-01 04:00:00,Echo",
            "2023-01-01 05:00:00,Foxtrot",
            "2023-01-01 06:00:00,Golf",
            "2023-01-01 07:00:00,Hotel",
            "2023-01-01 08:00:00,India",
            "2023-01-01 09:00:00,Juliett",
            "2023-01-01 10:00:00,Kilo",
            "2023-01-01 11:00:00,Lima",
            "2023-01-01 12:00:00,Mike",
        ]))

if __name__ == "__main__":
    unittest.main() 