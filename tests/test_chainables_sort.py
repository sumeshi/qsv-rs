import unittest
from test_base import QsvTestBase

class TestSort(QsvTestBase):
    """
    Test sort chainable module
    """
    
    def test_sort_ascending(self):
        """Test sorting rows in ascending order (default)"""
        # Note: Sample data is already sorted by col1, so we'll sort by col3 for a visible change
        output = self.run_qsv_command("load sample/simple.csv - sort col3 - show")
        
        # Check output order based on col3 ascending
        lines = output.strip().split('\n')
        # Header + 3 data rows
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], "datetime,col1,col2,col3,str")
        self.assertEqual(lines[1], "2023-01-01 12:00:00,1,2,3,foo")
        self.assertEqual(lines[2], "2023-01-01 13:00:00,4,5,6,bar")
        self.assertEqual(lines[3], "2023-01-01 14:00:00,7,8,9,baz")
    
    def test_sort_descending(self):
        """Test sorting rows in descending order"""
        output = self.run_qsv_command("load sample/simple.csv - sort col1 -d - show")
        
        # Check output based on actual behavior
        # Header + 3 data rows
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], "datetime,col1,col2,col3,str")
        # Expecting descending order (7,8,9 first)
        self.assertEqual(lines[1], "2023-01-01 14:00:00,7,8,9,baz")
        self.assertEqual(lines[2], "2023-01-01 13:00:00,4,5,6,bar")
        self.assertEqual(lines[3], "2023-01-01 12:00:00,1,2,3,foo")
    
    def test_sort_multiple_columns(self):
        """Test sorting rows by multiple columns"""
        # Note: For this test to be meaningful, we need data with duplicates
        # For now, we'll just verify the command works with multiple columns
        output = self.run_qsv_command("load sample/simple.csv - sort col1,col2 - show")
        
        # Should be the same as sorting by col1 in this simple dataset
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], "datetime,col1,col2,col3,str")
        self.assertEqual(lines[1], "2023-01-01 12:00:00,1,2,3,foo")
        self.assertEqual(lines[2], "2023-01-01 13:00:00,4,5,6,bar")
        self.assertEqual(lines[3], "2023-01-01 14:00:00,7,8,9,baz")

if __name__ == "__main__":
    unittest.main()