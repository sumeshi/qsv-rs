import unittest
import os
from test_base import QsvTestBase

class TestDump(QsvTestBase):
    """
    Test dump finalizer functionality
    """
    
    def test_dump_to_csv_basic(self):
        """Test basic dump to CSV functionality"""
        output_file = "/tmp/test_output.csv"
        
        # Clean up any existing file
        if os.path.exists(output_file):
            os.remove(output_file)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - dump {output_file}")
        
        # Should not produce stdout output (writes to file instead)
        self.assertEqual(result.stdout.strip(), "")
        
        # Check that file was created and has correct content
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            content = f.read().strip()
        
        expected_content = '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ])
        self.assertEqual(content, expected_content)
        
        # Clean up
        os.remove(output_file)
    
    def test_dump_to_tsv(self):
        """Test dump to TSV format"""
        output_file = "/tmp/test_output.tsv"
        
        # Clean up any existing file
        if os.path.exists(output_file):
            os.remove(output_file)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - dump --separator '\t' {output_file}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Check that file was created
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            content = f.read().strip()
        
        # TSV should use tab separators
        expected_content = '\n'.join([
            "datetime\tcol1\tcol2\tcol3\tstr",
            "2023-01-01 12:00:00\t1\t2\t3\tfoo",
            "2023-01-01 13:00:00\t4\t5\t6\tbar",
            "2023-01-01 14:00:00\t7\t8\t9\tbaz",
        ])
        self.assertEqual(content, expected_content)
        
        # Clean up
        os.remove(output_file)
    
    def test_dump_with_select(self):
        """Test dump after column selection"""
        output_file = "/tmp/test_selected.csv"
        
        # Clean up any existing file
        if os.path.exists(output_file):
            os.remove(output_file)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,str - dump {output_file}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Check that file was created with selected columns only
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            content = f.read().strip()
        
        expected_content = '\n'.join([
            "col1,str",
            "1,foo",
            "4,bar",
            "7,baz",
        ])
        self.assertEqual(content, expected_content)
        
        # Clean up
        os.remove(output_file)
    
    def test_dump_with_filtering(self):
        """Test dump after filtering operations"""
        output_file = "/tmp/test_filtered.csv"
        
        # Clean up any existing file
        if os.path.exists(output_file):
            os.remove(output_file)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - grep 'ba' - dump {output_file}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Check that file was created with filtered data
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            content = f.read().strip()
        
        expected_content = '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
        ])
        self.assertEqual(content, expected_content)
        
        # Clean up
        os.remove(output_file)
    
    def test_dump_with_head_tail(self):
        """Test dump after head/tail operations"""
        output_file = "/tmp/test_head_tail.csv"
        
        # Clean up any existing file
        if os.path.exists(output_file):
            os.remove(output_file)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - head 2 - dump {output_file}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Check that file was created with first 2 rows
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            content = f.read().strip()
        
        expected_content = '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 12:00:00,1,2,3,foo",
            "2023-01-01 13:00:00,4,5,6,bar",
        ])
        self.assertEqual(content, expected_content)
        
        # Clean up
        os.remove(output_file)
    
    def test_dump_with_sort(self):
        """Test dump after sorting operations"""
        output_file = "/tmp/test_sorted.csv"
        
        # Clean up any existing file
        if os.path.exists(output_file):
            os.remove(output_file)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - sort str - dump {output_file}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Check that file was created with sorted data
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            content = f.read().strip()
        
        # Should be sorted by str column: bar, baz, foo
        expected_content = '\n'.join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 13:00:00,4,5,6,bar",
            "2023-01-01 14:00:00,7,8,9,baz",
            "2023-01-01 12:00:00,1,2,3,foo",
        ])
        self.assertEqual(content, expected_content)
        
        # Clean up
        os.remove(output_file)
    
    def test_dump_complex_chain(self):
        """Test dump after complex operation chain"""
        output_file = "/tmp/test_complex.csv"
        
        # Clean up any existing file
        if os.path.exists(output_file):
            os.remove(output_file)
        
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - select col1,str - grep 'ba' - sort str - dump {output_file}")
        
        # Should not produce stdout output
        self.assertEqual(result.stdout.strip(), "")
        
        # Check that file was created with complex operations result
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            content = f.read().strip()
        
        # Should have selected columns, filtered for 'ba', sorted by str
        expected_content = '\n'.join([
            "col1,str",
            "4,bar",
            "7,baz",
        ])
        self.assertEqual(content, expected_content)
        
        # Clean up
        os.remove(output_file)

if __name__ == "__main__":
    unittest.main()