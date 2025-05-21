import unittest
import subprocess
import os
import sys

class QsvTestBase(unittest.TestCase):
    """
    Base test class for QSV module testing
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Root directory of the project
        self.root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        # Path to the qsv executable
        self.qsv_path = os.path.join(self.root_dir, 'target', 'debug', 'qsv')
        # Default sample file
        self.sample_file = os.path.join(self.root_dir, 'sample', 'simple.csv')
    
    def run_qsv_command(self, command_str):
        """
        Run a QSV command and return its output
        
        Args:
            command_str: Command string to run (e.g. "load sample/simple.csv - select col1 - show")
            
        Returns:
            Output of the command as a string
        """
        full_command = f"{self.qsv_path} {command_str}"
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error executing command: {full_command}")
            print(f"Error: {result.stderr}")
            return ""
        
        return result.stdout.strip()
    
    def assert_output_contains(self, output, expected_content):
        """
        Assert that the output contains the expected content
        
        Args:
            output: Output string to check
            expected_content: Content that should be in the output
        """
        self.assertIn(expected_content, output, 
                      f"Expected output to contain '{expected_content}', but it didn't.\nOutput: {output}")
    
    def assert_output_matches(self, output, expected_lines):
        """
        Assert that the output matches the expected lines
        
        Args:
            output: Output string to check
            expected_lines: List of strings that should be in the output, in order
        """
        output_lines = output.strip().split('\n')
        
        self.assertEqual(len(output_lines), len(expected_lines), 
                         f"Expected {len(expected_lines)} lines, got {len(output_lines)}")
        
        for i, (actual, expected) in enumerate(zip(output_lines, expected_lines)):
            self.assertIn(expected, actual, 
                         f"Line {i+1} doesn't contain expected content.\nExpected: {expected}\nActual: {actual}")