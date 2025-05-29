import unittest
import os
import tempfile
from test_base import QsvTestBase

class TestDump(QsvTestBase):
    """
    Test dump finalizer module
    """
    
    def setUp(self):
        """Set up test fixtures"""
        super().__init__()
        self.temp_dir = tempfile.mkdtemp()
        # Add necessary attributes from QsvTestBase
        self.root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.qsv_path = os.path.join(self.root_dir, 'target', 'debug', 'qsv')
        self.sample_file = os.path.join(self.root_dir, 'sample', 'simple.csv')
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up any test files created
        for file in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, file))
        os.rmdir(self.temp_dir)
    
    def test_dump_default_output(self):
        """Test dump with default output file (output.csv)"""
        # Use temp directory for output file
        output_file = os.path.join(self.temp_dir, "output.csv")
        
        # Run command with explicit output path to avoid cluttering project directory
        output = self.run_qsv_command(f"load sample/simple.csv - dump {output_file}")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("datetime,col1,col2,col3,str", content)
            self.assertIn("2023-01-01 12:00:00,1,2,3,foo", content)
            self.assertIn("2023-01-01 13:00:00,4,5,6,bar", content)
            self.assertIn("2023-01-01 14:00:00,7,8,9,baz", content)
    
    def test_dump_no_arguments_default_filename(self):
        """Test dump with no arguments defaults to output.csv"""
        # Change to temp directory to avoid cluttering project directory
        original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        
        try:
            # Run dump without specifying output file - should default to output.csv
            # We need to override the cwd for this specific test
            import subprocess
            import contextlib
            import io
            
            full_command = f"{self.qsv_path} load {self.sample_file} - dump"
            
            # Capture stdout to prevent unwanted output during tests
            with contextlib.redirect_stdout(io.StringIO()):
                result = subprocess.run(full_command, shell=True, capture_output=True, text=True, cwd=self.temp_dir)
            
            if result.returncode != 0:
                self.fail(f"Command failed: {result.stderr}")
            
            # Should create output.csv in current directory (temp directory)
            self.assertTrue(os.path.exists("output.csv"))
            
            # Check file contents
            with open("output.csv", 'r') as f:
                content = f.read()
                self.assertIn("datetime,col1,col2,col3,str", content)
                self.assertIn("2023-01-01 12:00:00,1,2,3,foo", content)
                self.assertIn("2023-01-01 13:00:00,4,5,6,bar", content)
                self.assertIn("2023-01-01 14:00:00,7,8,9,baz", content)
        finally:
            # Always restore original working directory
            os.chdir(original_cwd)
    
    def test_dump_custom_output_positional(self):
        """Test dump with custom output file as positional argument"""
        output_file = os.path.join(self.temp_dir, "custom_output.csv")
        output = self.run_qsv_command(f"load sample/simple.csv - dump {output_file}")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("datetime,col1,col2,col3,str", content)
            self.assertIn("2023-01-01 12:00:00,1,2,3,foo", content)
    
    def test_dump_custom_output_option_short(self):
        """Test dump with custom output file using -o option"""
        output_file = os.path.join(self.temp_dir, "option_output.csv")
        output = self.run_qsv_command(f"load sample/simple.csv - dump -o {output_file}")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("datetime,col1,col2,col3,str", content)
            self.assertIn("2023-01-01 12:00:00,1,2,3,foo", content)
    
    def test_dump_custom_output_option_long(self):
        """Test dump with custom output file using --output option"""
        output_file = os.path.join(self.temp_dir, "long_option_output.csv")
        output = self.run_qsv_command(f"load sample/simple.csv - dump --output {output_file}")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("datetime,col1,col2,col3,str", content)
            self.assertIn("2023-01-01 12:00:00,1,2,3,foo", content)
    
    def test_dump_custom_separator_short(self):
        """Test dump with custom separator using -s option"""
        output_file = os.path.join(self.temp_dir, "tab_separated.tsv")
        output = self.run_qsv_command(f"load sample/simple.csv - dump {output_file} -s '\t'")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents with tab separation
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("datetime\tcol1\tcol2\tcol3\tstr", content)
            self.assertIn("2023-01-01 12:00:00\t1\t2\t3\tfoo", content)
    
    def test_dump_custom_separator_long(self):
        """Test dump with custom separator using --separator option"""
        output_file = os.path.join(self.temp_dir, "pipe_separated.csv")
        output = self.run_qsv_command(f"load sample/simple.csv - dump {output_file} --separator '|'")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents with pipe separation
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("datetime|col1|col2|col3|str", content)
            self.assertIn("2023-01-01 12:00:00|1|2|3|foo", content)
    
    def test_dump_semicolon_separator(self):
        """Test dump with semicolon separator"""
        output_file = os.path.join(self.temp_dir, "semicolon_separated.csv")
        output = self.run_qsv_command(f"load sample/simple.csv - dump {output_file} --separator ';'")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents with semicolon separation
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("datetime;col1;col2;col3;str", content)
            self.assertIn("2023-01-01 12:00:00;1;2;3;foo", content)
    
    def test_dump_after_operations(self):
        """Test dump after various data operations"""
        output_file = os.path.join(self.temp_dir, "after_operations.csv")
        output = self.run_qsv_command(f"load sample/simple.csv - select col1,str - head 2 - dump {output_file}")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents - should only have selected columns and first 2 rows
        with open(output_file, 'r') as f:
            content = f.read()
            lines = content.strip().split('\n')
            
            # Should have header + 2 data rows
            self.assertEqual(len(lines), 3)
            self.assertIn("col1,str", lines[0])
            self.assertIn("1,foo", lines[1])
            self.assertIn("4,bar", lines[2])
            
            # Should not contain other columns
            self.assertNotIn("datetime", content)
            self.assertNotIn("col2", content)
            self.assertNotIn("col3", content)
    
    def test_dump_with_filtering(self):
        """Test dump after filtering operations"""
        output_file = os.path.join(self.temp_dir, "filtered.csv")
        output = self.run_qsv_command(f"load sample/simple.csv - grep 'ba' - dump {output_file}")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents - should only have rows containing "ba"
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("datetime,col1,col2,col3,str", content)
            self.assertIn("bar", content)
            self.assertIn("baz", content)
            self.assertNotIn("foo", content)
    
    def test_dump_combined_options(self):
        """Test dump with both custom output and separator"""
        output_file = os.path.join(self.temp_dir, "combined_options.tsv")
        output = self.run_qsv_command(f"load sample/simple.csv - select datetime,str - dump --output {output_file} --separator '\t'")
        
        # Should create the specified file
        self.assertTrue(os.path.exists(output_file))
        
        # Check file contents
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("datetime\tstr", content)
            self.assertIn("2023-01-01 12:00:00\tfoo", content)
            self.assertIn("2023-01-01 13:00:00\tbar", content)
            self.assertIn("2023-01-01 14:00:00\tbaz", content)

if __name__ == "__main__":
    unittest.main()