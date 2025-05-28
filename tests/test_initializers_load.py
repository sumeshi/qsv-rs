import unittest
import os
import tempfile
from test_base import QsvTestBase

class TestLoad(QsvTestBase):
    """
    Test load initializer module
    """
    
    def setUp(self):
        """Set up test fixtures"""
        # Create temporary test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test CSV files
        self.test_csv1 = os.path.join(self.temp_dir, "test1.csv")
        with open(self.test_csv1, 'w') as f:
            f.write("col1,col2,col3\n1,2,3\n4,5,6\n")
            
        self.test_csv2 = os.path.join(self.temp_dir, "test2.csv")
        with open(self.test_csv2, 'w') as f:
            f.write("col1,col2,col3\n7,8,9\n10,11,12\n")
            
        # Create TSV file
        self.test_tsv = os.path.join(self.temp_dir, "test.tsv")
        with open(self.test_tsv, 'w') as f:
            f.write("col1\tcol2\tcol3\n1\t2\t3\n4\t5\t6\n")
    
    def test_load_single_file(self):
        """Test loading a single CSV file"""
        output = self.run_qsv_command("load sample/simple.csv - show")
        
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_load_multiple_files(self):
        """Test loading multiple CSV files"""
        output = self.run_qsv_command(f"load {self.test_csv1} {self.test_csv2} - show")
        
        # Should contain data from both files
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
        self.assert_output_contains(output, "10,11,12")
    
    def test_load_with_custom_separator(self):
        """Test loading with custom separator using -s option"""
        output = self.run_qsv_command(f"load {self.test_tsv} -s '\t' - show")
        
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
    
    def test_load_with_separator_long_option(self):
        """Test loading with custom separator using --separator option"""
        output = self.run_qsv_command(f"load {self.test_tsv} --separator '\t' - show")
        
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
    
    def test_load_with_low_memory_flag(self):
        """Test loading with low-memory flag"""
        output = self.run_qsv_command("load sample/simple.csv --low-memory - show")
        
        # Should still load the data correctly
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
    
    def test_load_with_no_headers_flag(self):
        """Test loading with --no-headers flag"""
        output = self.run_qsv_command("load sample/simple.csv --no-headers - show")
        
        # Should treat first row as data and generate automatic column names
        self.assert_output_contains(output, "column_1,column_2,column_3,column_4,column_5")
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")  # First row becomes data
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
    
    def test_load_gzip_file(self):
        """Test loading gzip compressed CSV file"""
        # Create a gzipped test file
        import gzip
        gzip_file = os.path.join(self.temp_dir, "test.csv.gz")
        with gzip.open(gzip_file, 'wt') as f:
            f.write("col1,col2,col3\n1,2,3\n4,5,6\n")
        
        output = self.run_qsv_command(f"load {gzip_file} - show")
        
        # Should decompress and load the data correctly
        self.assert_output_contains(output, "col1,col2,col3")
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
    
    def test_load_nonexistent_file(self):
        """Test loading a non-existent file should fail gracefully"""
        # This should fail, but we test that it doesn't crash
        output = self.run_qsv_command("load nonexistent.csv - show")
        # The command should fail and return empty output
        self.assertEqual(output, "")

if __name__ == "__main__":
    unittest.main()