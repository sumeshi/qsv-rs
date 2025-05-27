import unittest
from test_base import QsvTestBase

class TestStats(QsvTestBase):
    """
    Test stats finalizer module
    """
    
    def test_stats_basic(self):
        """Test basic stats functionality"""
        output = self.run_qsv_command("load sample/simple.csv - stats")
        
        # Should show statistics for all columns
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "count")
        self.assert_output_contains(output, "null_count")
        
        # Should include all column names
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
    
    def test_stats_numeric_columns(self):
        """Test stats for numeric columns"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,col2,col3 - stats")
        
        # Should show numeric statistics
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        
        # Should include statistical measures for numeric data
        self.assert_output_contains(output, "count")
        # Note: Specific statistical values depend on Polars implementation
    
    def test_stats_string_column(self):
        """Test stats for string column"""
        output = self.run_qsv_command("load sample/simple.csv - select str - stats")
        
        # Should show statistics for string column
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "str")
        self.assert_output_contains(output, "count")
    
    def test_stats_after_filtering(self):
        """Test stats after filtering operations"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'ba' - stats")
        
        # Should show statistics for filtered data (2 rows)
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "count")
        
        # Should include all original columns
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
    
    def test_stats_after_column_selection(self):
        """Test stats after selecting specific columns"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,str - stats")
        
        # Should show statistics only for selected columns
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "str")
        
        # Should not include non-selected columns
        self.assertNotIn("datetime", output)
        self.assertNotIn("col2", output)
        self.assertNotIn("col3", output)
    
    def test_stats_after_head_operation(self):
        """Test stats after head operation"""
        output = self.run_qsv_command("load sample/simple.csv - head 2 - stats")
        
        # Should show statistics for first 2 rows only
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "count")
        
        # Should include all columns
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
    
    def test_stats_empty_result(self):
        """Test stats with no matching rows"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'xyz' - stats")
        
        # Should still show column information even with no data
        self.assert_output_contains(output, "Statistic")
        
        # Should include all columns
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
    
    def test_stats_after_sort(self):
        """Test stats after sorting operations"""
        output = self.run_qsv_command("load sample/simple.csv - sort str - stats")
        
        # Should show statistics for sorted data (same as original)
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "count")
        
        # Should include all columns
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
    
    def test_stats_after_uniq(self):
        """Test stats after uniq operation"""
        output = self.run_qsv_command("load sample/simple.csv - uniq str - stats")
        
        # Should show statistics for unique data
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "count")
        
        # Should include all columns
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")
        self.assert_output_contains(output, "str")
    
    def test_stats_after_renamecol(self):
        """Test stats after column renaming"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol str text - stats")
        
        # Should show statistics with renamed column
        self.assert_output_contains(output, "Statistic")
        self.assert_output_contains(output, "text")
        
        # Should not include old column name in header (but 'str' may appear as datatype)
        lines = output.split('\n')
        header_line = lines[1] if len(lines) > 1 else ""
        self.assertNotIn("│ str", header_line)
        
        # Should include other columns
        self.assert_output_contains(output, "datetime")
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        self.assert_output_contains(output, "col3")

if __name__ == "__main__":
    unittest.main() 