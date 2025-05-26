import unittest
import re
from test_base import QsvTestBase

class TestChangetz(QsvTestBase):
    """
    Test changetz chainable module
    """
    
    def test_changetz_basic(self):
        """Test converting timezone of a datetime column"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime UTC EST - show")
        self.assertNotEqual("", output)
        self.assert_output_contains(output, "datetime")
        self.assertTrue("07:00:00" in output or "08:00:00" in output or "09:00:00" in output)
    
    def test_changetz_with_format(self):
        """Test timezone conversion with a specified format"""
        cmd = "load sample/simple.csv - changetz datetime UTC EST '%Y-%m-%d %H:%M:%S' - show"
        output = self.run_qsv_command(cmd)
        self.assertNotEqual("", output)
        self.assert_output_contains(output, "datetime")
        date_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
        match = re.search(date_pattern, output)
        self.assertTrue(match, "specified format not output date")

if __name__ == "__main__":
    unittest.main()