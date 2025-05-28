import unittest
import os
import tempfile
import shutil
from test_base import QsvTestBase

class TestPartition(QsvTestBase):
    """
    Test cases for the partition chainable operation
    """
    
    def setUp(self):
        """Set up test environment"""
        super().setUp()
        # Create a temporary directory for partition outputs
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test environment"""
        # Remove temporary directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_partition_by_string_column(self):
        """Test partitioning by string column"""
        output = self.run_qsv_command(f"load sample/simple.csv - partition str {self.temp_dir} - show")
        
        # Should return original data
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "foo")
        self.assert_output_contains(output, "bar")
        self.assert_output_contains(output, "baz")
        
        # Check that partition files were created
        expected_files = ["foo.csv", "bar.csv", "baz.csv"]
        for filename in expected_files:
            file_path = os.path.join(self.temp_dir, filename)
            self.assertTrue(os.path.exists(file_path), f"Partition file {filename} should exist")
            
            # Check file content
            with open(file_path, 'r') as f:
                content = f.read()
                self.assertIn("datetime,col1,col2,col3,str", content)  # Header should be present
                self.assertIn(filename.replace('.csv', ''), content)  # Value should be in the file
    
    def test_partition_by_numeric_column(self):
        """Test partitioning by numeric column"""
        output = self.run_qsv_command(f"load sample/simple.csv - partition col1 {self.temp_dir} - show")
        
        # Should return original data
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        
        # Check that partition files were created for numeric values
        expected_files = ["1.csv", "4.csv", "7.csv"]
        for filename in expected_files:
            file_path = os.path.join(self.temp_dir, filename)
            self.assertTrue(os.path.exists(file_path), f"Partition file {filename} should exist")
            
            # Check file content
            with open(file_path, 'r') as f:
                content = f.read()
                self.assertIn("datetime,col1,col2,col3,str", content)  # Header should be present
                expected_value = filename.replace('.csv', '')
                self.assertIn(f",{expected_value},", content)  # Numeric value should be in the file
    
    def test_partition_preserves_all_columns(self):
        """Test that partition preserves all columns in output files"""
        self.run_qsv_command(f"load sample/simple.csv - partition str {self.temp_dir} - show")
        
        # Check foo.csv content
        foo_file = os.path.join(self.temp_dir, "foo.csv")
        with open(foo_file, 'r') as f:
            content = f.read()
            lines = content.strip().split('\n')
            
            # Should have header + 1 data row
            self.assertEqual(len(lines), 2)
            self.assertEqual(lines[0], "datetime,col1,col2,col3,str")
            self.assertEqual(lines[1], "2023-01-01 12:00:00,1,2,3,foo")
    
    def test_partition_with_filtered_data(self):
        """Test partition after filtering operations"""
        output = self.run_qsv_command(f"load sample/simple.csv - grep 'ba' - partition str {self.temp_dir} - show")
        
        # Should only create files for filtered data (bar and baz)
        expected_files = ["bar.csv", "baz.csv"]
        unexpected_files = ["foo.csv"]
        
        for filename in expected_files:
            file_path = os.path.join(self.temp_dir, filename)
            self.assertTrue(os.path.exists(file_path), f"Partition file {filename} should exist")
        
        for filename in unexpected_files:
            file_path = os.path.join(self.temp_dir, filename)
            self.assertFalse(os.path.exists(file_path), f"Partition file {filename} should not exist")
    
    def test_partition_with_selected_columns(self):
        """Test partition after column selection"""
        self.run_qsv_command(f"load sample/simple.csv - select col1,str - partition str {self.temp_dir} - show")
        
        # Check that partition files only contain selected columns
        foo_file = os.path.join(self.temp_dir, "foo.csv")
        with open(foo_file, 'r') as f:
            content = f.read()
            lines = content.strip().split('\n')
            
            # Should have header + 1 data row with only selected columns
            self.assertEqual(len(lines), 2)
            self.assertEqual(lines[0], "col1,str")
            self.assertEqual(lines[1], "1,foo")
            
            # Should not contain other columns
            self.assertNotIn("datetime", content)
            self.assertNotIn("col2", content)
            self.assertNotIn("col3", content)
    
    def test_partition_error_invalid_column(self):
        """Test partition with invalid column name"""
        output = self.run_qsv_command(f"load sample/simple.csv - partition nonexistent {self.temp_dir} - show")
        
        # Should be empty output due to error
        self.assertEqual(output, "")
    
    def test_partition_creates_directory(self):
        """Test that partition creates output directory if it doesn't exist"""
        new_dir = os.path.join(self.temp_dir, "new_partition_dir")
        self.assertFalse(os.path.exists(new_dir))
        
        self.run_qsv_command(f"load sample/simple.csv - partition str {new_dir} - show")
        
        # Directory should be created
        self.assertTrue(os.path.exists(new_dir))
        
        # Files should be created in the new directory
        expected_files = ["foo.csv", "bar.csv", "baz.csv"]
        for filename in expected_files:
            file_path = os.path.join(new_dir, filename)
            self.assertTrue(os.path.exists(file_path))

if __name__ == '__main__':
    unittest.main() 