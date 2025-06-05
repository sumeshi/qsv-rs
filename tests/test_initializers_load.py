import unittest
import os
from test_base import QsvTestBase

class TestLoad(QsvTestBase):
    
    def test_load_single_file(self):
        """Test loading a single CSV file"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "datetime,col1,col2,col3,str",
                "2023-01-01 12:00:00,1,2,3,foo",
                "2023-01-01 13:00:00,4,5,6,bar",
                "2023-01-01 14:00:00,7,8,9,baz",
            ])
        )

    def test_load_gzip_file(self):
        """Test loading gzip compressed CSV file"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv.gz')} - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "datetime,col1,col2,col3,str",
                "2023-01-01 12:00:00,1,2,3,foo",
                "2023-01-01 13:00:00,4,5,6,bar",
                "2023-01-01 14:00:00,7,8,9,baz",
            ])
        )
    
    def test_load_multiple_files(self):
        """Test loading multiple CSV files"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} {self.get_fixture_path('simple.csv')} - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "datetime,col1,col2,col3,str",
                "2023-01-01 12:00:00,1,2,3,foo",
                "2023-01-01 13:00:00,4,5,6,bar",
                "2023-01-01 14:00:00,7,8,9,baz",
                "2023-01-01 12:00:00,1,2,3,foo",
                "2023-01-01 13:00:00,4,5,6,bar",
                "2023-01-01 14:00:00,7,8,9,baz",
            ])
        )
    
    def test_load_with_custom_separator(self):
        """Test loading with custom separator using -s option"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.tsv')} -s '\t' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "datetime,col1,col2,col3,str",
                "2023-01-01 12:00:00,1,2,3,foo",
                "2023-01-01 13:00:00,4,5,6,bar",
                "2023-01-01 14:00:00,7,8,9,baz",
            ])
        )
    
    def test_load_with_separator_long_option(self):
        """Test loading with custom separator using --separator option"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.tsv')} --separator '\t' - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "datetime,col1,col2,col3,str",
                "2023-01-01 12:00:00,1,2,3,foo",
                "2023-01-01 13:00:00,4,5,6,bar",
                "2023-01-01 14:00:00,7,8,9,baz",
            ])
        )
    
    def test_load_with_low_memory_flag(self):
        """Test loading with low-memory flag"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} --low-memory - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "datetime,col1,col2,col3,str",
                "2023-01-01 12:00:00,1,2,3,foo",
                "2023-01-01 13:00:00,4,5,6,bar",
                "2023-01-01 14:00:00,7,8,9,baz",
            ])
        )
    
    def test_load_with_no_headers_flag(self):
        """Test loading with --no-headers flag"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_noheader.csv')} --no-headers - show")
        self.assertEqual(result.stdout.strip(), '\n'.join([
                "column_1,column_2,column_3,column_4,column_5",
                "2023-01-01 12:00:00,1,2,3,foo",
                "2023-01-01 13:00:00,4,5,6,bar",
                "2023-01-01 14:00:00,7,8,9,baz",
            ])
        )
    
    def test_load_nonexistent_file(self):
        """Test loading a non-existent file should fail gracefully"""
        result = self.run_qsv_command(f"load non_existent_file.csv - show")
        self.assertEqual(result.stderr.strip(), '\n'.join([
                "Error: File not found: non_existent_file.csv",
                "One or more files do not exist",
            ])
        )

if __name__ == "__main__":
    unittest.main()