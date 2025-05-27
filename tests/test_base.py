import unittest
import subprocess
import os
import sys
import io
import contextlib

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
        # Replace relative sample path with absolute path
        if "sample/simple.csv" in command_str:
            command_str = command_str.replace("sample/simple.csv", self.sample_file)
        
        full_command = f"{self.qsv_path} {command_str}"
        
        # Capture stdout to prevent unwanted output during tests
        with contextlib.redirect_stdout(io.StringIO()):
            result = subprocess.run(full_command, shell=True, capture_output=True, text=True, cwd=self.root_dir)
        
        if result.returncode != 0:
            # Only print errors if needed for debugging, but suppress during normal test runs
            if os.environ.get('QSV_TEST_DEBUG'):
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
    
    def assert_output_not_contains(self, output, unexpected_content):
        """
        Assert that the output does NOT contain the unexpected content
        
        Args:
            output: Output string to check
            unexpected_content: Content that should NOT be in the output
        """
        self.assertNotIn(unexpected_content, output,
                         f"Expected output NOT to contain '{unexpected_content}', but it did.\nOutput: {output}")
    
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