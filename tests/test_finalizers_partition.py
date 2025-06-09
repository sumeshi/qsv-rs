import unittest
import shutil
from pathlib import Path
from test_base import QsvTestBase

class TestPartition(QsvTestBase):
    
    def test_partition_by_string_column(self):
        """Test partitioning by string column"""
        output_dir = Path("/tmp/test_partition")
        
        if output_dir.exists():
            shutil.rmtree(output_dir)
        
        output_dir.mkdir(parents=True)
        
        self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - partition str {output_dir}")
        expected_files = ["foo.csv", "bar.csv", "baz.csv"]
        for filename in expected_files:
            filepath = output_dir / filename
            self.assertTrue(filepath.exists())
            
        self.assertEqual(
            Path(output_dir / "foo.csv").read_text().strip(),
            "\n".join([
                "datetime,col1,col2,col3,str",
                "2023-01-01 12:00:00,1,2,3,foo",
            ])
        )

        shutil.rmtree(output_dir)

    
    def test_partition_by_numeric_column(self):
        """Test partitioning by numeric column"""
        output_dir = Path("/tmp/test_partition_numeric")
        
        if output_dir.exists():
            shutil.rmtree(output_dir)
        
        output_dir.mkdir(parents=True)
        
        self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - partition col1 {output_dir}")
        expected_files = ["1.csv", "4.csv", "7.csv"]
        for filename in expected_files:
            filepath = output_dir / filename
            self.assertTrue(filepath.exists())
            
        self.assertEqual(
            Path(output_dir / "1.csv").read_text().strip(),
            "\n".join([
                "datetime,col1,col2,col3,str",
                "2023-01-01 12:00:00,1,2,3,foo",
            ])
        )
        
        shutil.rmtree(output_dir)

if __name__ == "__main__":
    unittest.main() 