import unittest
from test_base import QsvTestBase

class TestSort(QsvTestBase):
    """
    Test sort chainable module
    """
    
    def test_sort_single_column_ascending(self):
        """Test sorting by single column in ascending order (default)"""
        output = self.run_qsv_command("load sample/simple.csv - sort str - show")
        
        # Should sort by str column: bar, baz, foo
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("bar", lines[1])
        self.assertIn("baz", lines[2])
        self.assertIn("foo", lines[3])
    
    def test_sort_single_column_descending(self):
        """Test sorting by single column in descending order"""
        output = self.run_qsv_command("load sample/simple.csv - sort str --desc - show")
        
        # Should sort by str column in descending order: foo, baz, bar
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("foo", lines[1])
        self.assertIn("baz", lines[2])
        self.assertIn("bar", lines[3])
    
    def test_sort_numeric_column_ascending(self):
        """Test sorting by numeric column in ascending order"""
        output = self.run_qsv_command("load sample/simple.csv - sort col1 - show")
        
        # Should sort by col1: 1, 4, 7
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("1,2,3,foo", lines[1])
        self.assertIn("4,5,6,bar", lines[2])
        self.assertIn("7,8,9,baz", lines[3])
    
    def test_sort_numeric_column_descending(self):
        """Test sorting by numeric column in descending order"""
        output = self.run_qsv_command("load sample/simple.csv - sort col1 --desc - show")
        
        # Should sort by col1 in descending order: 7, 4, 1
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("7,8,9,baz", lines[1])
        self.assertIn("4,5,6,bar", lines[2])
        self.assertIn("1,2,3,foo", lines[3])
    
    def test_sort_multiple_columns(self):
        """Test sorting by multiple columns"""
        output = self.run_qsv_command("load sample/simple.csv - sort col2,col1 - show")
        
        # Should sort by col2 first, then col1: (2,1), (5,4), (8,7)
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("1,2,3,foo", lines[1])
        self.assertIn("4,5,6,bar", lines[2])
        self.assertIn("7,8,9,baz", lines[3])
    
    def test_sort_multiple_columns_descending(self):
        """Test sorting by multiple columns in descending order"""
        output = self.run_qsv_command("load sample/simple.csv - sort col2,col1 --desc - show")
        
        # Should sort by col2 desc, then col1 desc: (8,7), (5,4), (2,1)
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("7,8,9,baz", lines[1])
        self.assertIn("4,5,6,bar", lines[2])
        self.assertIn("1,2,3,foo", lines[3])
    
    def test_sort_datetime_column(self):
        """Test sorting by datetime column"""
        output = self.run_qsv_command("load sample/simple.csv - sort datetime - show")
        
        # Should sort by datetime (already in ascending order in sample data)
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("2023-01-01 12:00:00", lines[1])
        self.assertIn("2023-01-01 13:00:00", lines[2])
        self.assertIn("2023-01-01 14:00:00", lines[3])
    
    def test_sort_datetime_column_descending(self):
        """Test sorting by datetime column in descending order"""
        output = self.run_qsv_command("load sample/simple.csv - sort datetime --desc - show")
        
        # Should sort by datetime in descending order
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("2023-01-01 14:00:00", lines[1])
        self.assertIn("2023-01-01 13:00:00", lines[2])
        self.assertIn("2023-01-01 12:00:00", lines[3])
    
    def test_sort_nonexistent_column(self):
        """Test sorting by non-existent column should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - sort nonexistent - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_sort_after_filtering(self):
        """Test sorting after filtering operations"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'ba' - sort str - show")
        
        # Should filter to "bar" and "baz", then sort: bar, baz
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 3)  # header + 2 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("bar", lines[1])
        self.assertIn("baz", lines[2])
    
    def test_sort_with_column_selection(self):
        """Test sorting with column selection"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,str - sort str - show")
        
        # Should select columns then sort by str: bar, baz, foo
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("col1,str", lines[0])
        self.assertIn("4,bar", lines[1])
        self.assertIn("7,baz", lines[2])
        self.assertIn("1,foo", lines[3])
    
    def test_sort_column_range(self):
        """Test sorting with column range notation"""
        output = self.run_qsv_command("load sample/simple.csv - sort col1-col3 - show")
        
        # Should sort by col1, col2, col3 (already in ascending order)
        lines = output.strip().split('\n')
        self.assertEqual(len(lines), 4)  # header + 3 data rows
        self.assertIn("datetime,col1,col2,col3,str", lines[0])
        self.assertIn("1,2,3,foo", lines[1])
        self.assertIn("4,5,6,bar", lines[2])
        self.assertIn("7,8,9,baz", lines[3])

if __name__ == "__main__":
    unittest.main() 