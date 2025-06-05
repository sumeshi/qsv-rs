import unittest
import os
import glob
from test_base import QsvTestBase

class TestPartition(QsvTestBase):
    
    def test_partition_by_string_column(self):
        """Test partitioning by string column"""
        output_dir = "/tmp/test_partition"
        
        # Clean up any existing files
        if os.path.exists(output_dir):
            for file in glob.glob(f"{output_dir}/*.csv"):
                os.remove(file)
        else:
            os.makedirs(output_dir)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - partition str {output_dir}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Should create separate files for each unique value
        expected_files = ["foo.csv", "bar.csv", "baz.csv"]
        for filename in expected_files:
            filepath = os.path.join(output_dir, filename)
            self.assertTrue(os.path.exists(filepath), f"File {filename} should be created")
            
        # Check content of foo.csv
        with open(os.path.join(output_dir, "foo.csv"), 'r') as f:
            content = f.read().strip()
        expected_foo = '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ])
        self.assertEqual(content, expected_foo)
        
        # Clean up
        for file in glob.glob(f"{output_dir}/*.csv"):
            os.remove(file)
        os.rmdir(output_dir)
    
    def test_partition_by_numeric_column(self):
        """Test partitioning by numeric column"""
        output_dir = "/tmp/test_partition_numeric"
        
        # Clean up any existing files
        if os.path.exists(output_dir):
            for file in glob.glob(f"{output_dir}/*.csv"):
                os.remove(file)
        else:
            os.makedirs(output_dir)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - partition col1 {output_dir}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Should create files named after numeric values
        expected_files = ["1.csv", "4.csv", "7.csv"]
        for filename in expected_files:
            filepath = os.path.join(output_dir, filename)
            self.assertTrue(os.path.exists(filepath), f"File {filename} should be created")
            
        # Check content of 1.csv
        with open(os.path.join(output_dir, "1.csv"), 'r') as f:
            content = f.read().strip()
        expected_1 = '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
        ])
        self.assertEqual(content, expected_1)
        
        # Clean up
        for file in glob.glob(f"{output_dir}/*.csv"):
            os.remove(file)
        os.rmdir(output_dir)
    
    def test_partition_with_select(self):
        """Test partitioning after column selection"""
        output_dir = "/tmp/test_partition_select"
        
        # Clean up any existing files
        if os.path.exists(output_dir):
            for file in glob.glob(f"{output_dir}/*.csv"):
                os.remove(file)
        else:
            os.makedirs(output_dir)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,str - partition str {output_dir}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Should create files with only selected columns
        expected_files = ["foo.csv", "bar.csv", "baz.csv"]
        for filename in expected_files:
            filepath = os.path.join(output_dir, filename)
            self.assertTrue(os.path.exists(filepath), f"File {filename} should be created")
        
        # Check content of foo.csv (should only have selected columns)
        with open(os.path.join(output_dir, "foo.csv"), 'r') as f:
            content = f.read().strip()
        expected_foo = '\n'.join([
            "col1,str",
            "1,foo",
        ])
        self.assertEqual(content, expected_foo)
            
        # Clean up
        for file in glob.glob(f"{output_dir}/*.csv"):
            os.remove(file)
        os.rmdir(output_dir)
    
    def test_partition_with_filtering(self):
        """Test partitioning after filtering"""
        output_dir = "/tmp/test_partition_filter"
        
        # Clean up any existing files
        if os.path.exists(output_dir):
            for file in glob.glob(f"{output_dir}/*.csv"):
                os.remove(file)
        else:
            os.makedirs(output_dir)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep 'ba' - partition str {output_dir}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Should create files only for filtered data
        expected_files = ["bar.csv", "baz.csv"]
        for filename in expected_files:
            filepath = os.path.join(output_dir, filename)
            self.assertTrue(os.path.exists(filepath), f"File {filename} should be created")
        
        # Should not create foo.csv
        foo_path = os.path.join(output_dir, "foo.csv")
        self.assertFalse(os.path.exists(foo_path), "foo.csv should not be created after filtering")
        
        # Clean up
        for file in glob.glob(f"{output_dir}/*.csv"):
            os.remove(file)
        os.rmdir(output_dir)

if __name__ == "__main__":
    unittest.main() 