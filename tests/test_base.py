import unittest
import subprocess
import os
import sys
import io
import contextlib
from pathlib import Path

class QsvTestBase(unittest.TestCase):
    """
    Base test class for QSV module testing
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.root_dir = Path(__file__).parent.parent.resolve()
        self.fixtures_dir = self.root_dir / "tests" / "fixtures"
        self.qsv_path = self.root_dir / "target" / "debug" / "qsv"
    
    def get_fixture_path(self, filename):
        """
        Get the absolute path to a fixture file
        
        Args:
            filename: Name of the fixture file
            
        Returns:
            Absolute path to the fixture file as string
        """
        return str(self.fixtures_dir / filename)
    
    def run_qsv_command(self, command_str):
        """
        Run a QSV command and return its output
        
        Args:
            command_str: Command string to run
            
        Returns:
            Output of the command as a string
        """
        full_command = f"{self.qsv_path} {command_str}"
        
        with contextlib.redirect_stdout(io.StringIO()):
            result = subprocess.run(full_command, shell=True, capture_output=True, text=True, cwd=self.root_dir)
        
        return result
