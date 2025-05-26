import unittest
from test_base import QsvTestBase

class TestRenamecol(QsvTestBase):
    """
    Test renamecol chainable module
    """
    
    def test_renamecol_basic(self):
        """Test renaming a column"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol col1 new_col_name - show")
        
        # Check if the column was renamed
        self.assert_output_contains(output, "new_col_name,col2,col3")
        
        # Check if the data was preserved
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
        
        # Make sure old column name is not present
        self.assertNotIn("col1,", output)
    
    def test_renamecol_multiple(self):
        """Test renaming multiple columns in sequence"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol col1 a - renamecol col2 b - renamecol col3 c - show")
        
        # Check if all columns were renamed
        self.assert_output_contains(output, "a,b,c")
        
        # Check if the data was preserved
        self.assert_output_contains(output, "1,2,3")
        self.assert_output_contains(output, "4,5,6")
        self.assert_output_contains(output, "7,8,9")
        
        # Make sure old column names are not present
        self.assertNotIn("col1", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)

if __name__ == "__main__":
    unittest.main()