import unittest
from pathlib import Path
from test_base import QsvTestBase

class TestDump(QsvTestBase):

    def test_dump_to_csv_basic(self):
        """Test dump to CSV"""

        output_file = Path("/tmp/test_output.csv")

        if output_file.exists():
            output_file.unlink()
        
        self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - dump {output_file}")
        self.assertTrue(output_file.exists())

        self.assertEqual(
            output_file.read_text().strip(), 
            '\n'.join([
                "datetime,col1,col2,col3,str",
                "2023-01-01 12:00:00,1,2,3,foo",
                "2023-01-01 13:00:00,4,5,6,bar",
                "2023-01-01 14:00:00,7,8,9,baz",
            ])
        )
        output_file.unlink()

    
    def test_dump_to_tsv(self):
        """Test dump to TSV"""

        output_file = Path("/tmp/test_output.tsv")

        if output_file.exists():
            output_file.unlink()
        
        self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - dump --separator '\t' {output_file}")
        self.assertTrue(output_file.exists())
        
        self.assertEqual(
            output_file.read_text().strip(), 
            '\n'.join([
                "datetime\tcol1\tcol2\tcol3\tstr",
                "2023-01-01 12:00:00\t1\t2\t3\tfoo",
                "2023-01-01 13:00:00\t4\t5\t6\tbar",
                "2023-01-01 14:00:00\t7\t8\t9\tbaz",
            ])
        )
        output_file.unlink()


if __name__ == "__main__":
    unittest.main()