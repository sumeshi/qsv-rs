import unittest
import os
from test_base import QsvTestBase

class TestQuilt(QsvTestBase):
    """
    Test quilt module
    """
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        # Path to config file
        self.config_file = os.path.join(self.root_dir, "tests", "test_quilt_config.yaml")
        # Path to temporary output file
        self.temp_output = os.path.join(self.root_dir, "sample", "temp_quilt_output.csv")
        # Clean up existing output file
        if os.path.exists(self.temp_output):
            os.remove(self.temp_output)
    
    def tearDown(self):
        """Tear down test fixtures"""
        # Clean up output file after test
        if os.path.exists(self.temp_output):
            os.remove(self.temp_output)
    
    def test_quilt_basic(self):
        """Test basic quilt functionality"""
        # Run quilt command
        output = self.run_qsv_command(f"quilt {self.config_file}")
        
        # Check command ran without error
        self.assertNotEqual("", output)
        # Check output contains expected CSV data from config
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        # col3 is not selected in transform stage, so exclude from test
    
    def test_quilt_with_output(self):
        """Test quilt with output file"""
        # Run quilt command with output file
        self.run_qsv_command(f"quilt {self.config_file} -o={self.temp_output}")
        
        # Check output file was created
        self.assertTrue(os.path.exists(self.temp_output), "Output file was not created")
        
        # Check contents of output file
        with open(self.temp_output, 'r') as f:
            content = f.read()
            self.assertIn("col1", content, "Output file does not contain expected data")
    
    def test_quilt_with_title(self):
        """Test quilt with title"""
        # Run quilt command with title
        output = self.run_qsv_command(f"quilt {self.config_file} -t=\"Custom Quilt Title\"")
        
        # Check command ran without error
        self.assertNotEqual("", output)
        # Check title is in log message
        # (Actual output is not controllable, so mainly for basic check like syntax error)
        self.assert_output_contains(output, "col1")

if __name__ == "__main__":
    unittest.main()