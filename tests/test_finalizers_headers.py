import unittest
from test_base import QsvTestBase

class TestHeaders(QsvTestBase):
    """
    Test headers finalizer module
    """
    
    def test_headers_default_table_format(self):
        """Test headers with default table format"""
        output = self.run_qsv_command("load sample/simple.csv - headers")
        
        # Should display headers in table format
        self.assert_output_contains(output, "Column Name")
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
        
        # Should include table formatting characters
        self.assertIn("│", output)  # Table border character
        self.assertIn("─", output)  # Table border character
    
    def test_headers_plain_format_short_flag(self):
        """Test headers with plain format using -p flag"""
        output = self.run_qsv_command("load sample/simple.csv - headers -p")
        
        # Should display headers as plain text, one per line
        lines = output.strip().split('\n')
        expected_headers = ["datetime", "col1", "col2", "col3", "str"]
        
        self.assertEqual(len(lines), len(expected_headers))
        for header in expected_headers:
            self.assertIn(header, lines)
        
        # Should NOT include table formatting characters
        self.assertNotIn("│", output)
        self.assertNotIn("─", output)
        self.assertNotIn("Column Name", output)
    
    def test_headers_plain_format_long_flag(self):
        """Test headers with plain format using --plain flag"""
        output = self.run_qsv_command("load sample/simple.csv - headers --plain")
        
        # Should display headers as plain text, one per line
        lines = output.strip().split('\n')
        expected_headers = ["datetime", "col1", "col2", "col3", "str"]
        
        self.assertEqual(len(lines), len(expected_headers))
        for header in expected_headers:
            self.assertIn(header, lines)
        
        # Should NOT include table formatting characters
        self.assertNotIn("│", output)
        self.assertNotIn("─", output)
        self.assertNotIn("Column Name", output)
    
    def test_headers_after_select(self):
        """Test headers after selecting specific columns"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,col3 - headers")
        
        # Should only show selected columns
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col3")
        
        # Should not show non-selected columns
        self.assertNotIn("datetime", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("str", output)
    
    def test_headers_after_select_plain(self):
        """Test headers in plain format after selecting specific columns"""
        output = self.run_qsv_command("load sample/simple.csv - select datetime,str - headers --plain")
        
        # Should only show selected columns in plain format
        lines = output.strip().split('\n')
        expected_headers = ["datetime", "str"]
        
        self.assertEqual(len(lines), len(expected_headers))
        for header in expected_headers:
            self.assertIn(header, lines)
        
        # Should not show non-selected columns
        for line in lines:
            self.assertNotIn("col1", line)
            self.assertNotIn("col2", line)
            self.assertNotIn("col3", line)
    
    def test_headers_with_column_range_selection(self):
        """Test headers after selecting column range"""
        output = self.run_qsv_command("load sample/simple.csv - select col1-col3 - headers")
        
        # Should show columns in the range
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        
        # Should not show columns outside the range
        self.assertNotIn("datetime", output)
        self.assertNotIn("str", output)
    
    def test_headers_after_operations(self):
        """Test headers after various operations that don't change column structure"""
        output = self.run_qsv_command("load sample/simple.csv - head 2 - tail 1 - headers")
        
        # Should still show all original columns
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
    
    def test_headers_after_renamecol(self):
        """Test headers after renaming a column"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol col1 renamed_col - headers")
        
        # Should show the renamed column
        self.assert_output_contains(output, "renamed_col")
        
        # Should not show the original column name
        self.assertNotIn("col1", output)
        
        # Other columns should remain unchanged
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")

if __name__ == "__main__":
    unittest.main()