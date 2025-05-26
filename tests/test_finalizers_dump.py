import unittest
import os
import re
from test_base import QsvTestBase

class TestDump(QsvTestBase):
    """
    Test dump finalizer module
    """
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        # Create a temporary output file path
        self.temp_output = os.path.join(self.root_dir, "sample", "temp_output.csv")
        # Clean up any existing output file
        if os.path.exists(self.temp_output):
            os.remove(self.temp_output)
    
    def tearDown(self):
        """Tear down test fixtures"""
        # Clean up the output file after tests
        if os.path.exists(self.temp_output):
            os.remove(self.temp_output)
    
    def test_dump_basic(self):
        """Test dumping data to a file"""
        # Run command to dump to output file
        self.run_qsv_command(f"load sample/simple.csv - dump {self.temp_output}")
        
        # Verify the file was created
        self.assertTrue(os.path.exists(self.temp_output))
        
        # Read and verify the content
        with open(self.temp_output, 'r') as f:
            content = f.read()
            
        # Check if the output contains the expected data
        self.assertIn("col1,col2,col3", content)
        self.assertIn("1,2,3", content)
        self.assertIn("4,5,6", content)
        self.assertIn("7,8,9", content)
    
    def test_dump_with_separator(self):
        """Test dumping with a custom separator"""
        # Use tab as separator
        self.run_qsv_command(f"load sample/simple.csv - dump {self.temp_output} -s=\\t")
        
        # Verify the file was created
        self.assertTrue(os.path.exists(self.temp_output))
        
        # Read and verify the content
        with open(self.temp_output, 'r') as f:
            content = f.read()
            
        # Expected header: "datetime"\tcol1\tcol2\tcol3\t"str" (tab separated)
        # Expected data line example: 2023-01-01 12:00:00\t1\t2\t3\tfoo
        
        # The actual output is tab-separated with quotes around some headers.
        expected_header_pattern = r'"datetime"\tcol1\tcol2\tcol3\t"str"'
        self.assertIsNotNone(re.search(expected_header_pattern, content),
                             f"Expected header pattern '{expected_header_pattern}' not found in content.\nContent: {content[:200]}...")
        
        # Check for a data line, ensuring tab separation and correct values
        expected_data_segment_pattern = r"1\t2\t3"
        self.assertIsNotNone(re.search(expected_data_segment_pattern, content),
                             f"Expected data pattern '{expected_data_segment_pattern}' not found in content.\nContent: {content[:200]}...")
        
        self.assertIn("foo", content)
        self.assertIn("2023-01-01 12:00:00", content)

if __name__ == "__main__":
    unittest.main()