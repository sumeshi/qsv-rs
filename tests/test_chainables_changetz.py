import unittest
from test_base import QsvTestBase

class TestChangetz(QsvTestBase):
    """
    Test changetz chainable module
    """
    
    def test_changetz_basic_utc_to_jst(self):
        """Test basic timezone conversion from UTC to JST"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo - show")
        
        # Should convert UTC times to JST (UTC+9)
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # UTC 12:00 -> JST 21:00
        self.assert_output_contains(output, "2023-01-01 21:00:00")
        # UTC 13:00 -> JST 22:00  
        self.assert_output_contains(output, "2023-01-01 22:00:00")
        # UTC 14:00 -> JST 23:00
        self.assert_output_contains(output, "2023-01-01 23:00:00")
    
    def test_changetz_utc_to_america_new_york(self):
        """Test timezone conversion from UTC to America/New_York"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz UTC --to_tz America/New_York - show")
        
        # Should convert UTC times to EST (UTC-5 in January)
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # UTC 12:00 -> EST 07:00
        self.assert_output_contains(output, "2023-01-01 07:00:00")
        # UTC 13:00 -> EST 08:00
        self.assert_output_contains(output, "2023-01-01 08:00:00")
        # UTC 14:00 -> EST 09:00
        self.assert_output_contains(output, "2023-01-01 09:00:00")
    
    def test_changetz_local_to_utc(self):
        """Test timezone conversion from local to UTC"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz local --to_tz UTC - show")
        
        # Should convert local times to UTC (exact conversion depends on system timezone)
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Should have some datetime values (exact values depend on system timezone)
        self.assert_output_contains(output, "2023-01-01")
    
    def test_changetz_with_custom_format(self):
        """Test changetz with custom datetime format"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo --format '%Y-%m-%d %H:%M:%S' - show")
        
        # Should still work with explicit format specification
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 21:00:00")
    
    def test_changetz_with_auto_format(self):
        """Test changetz with auto format detection"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo --format auto - show")
        
        # Should work with auto format detection
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 21:00:00")
    
    def test_changetz_with_ambiguous_earliest(self):
        """Test changetz with ambiguous time strategy set to earliest"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo --ambiguous earliest - show")
        
        # Should work with earliest ambiguous time strategy
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 21:00:00")
    
    def test_changetz_with_ambiguous_latest(self):
        """Test changetz with ambiguous time strategy set to latest"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo --ambiguous latest - show")
        
        # Should work with latest ambiguous time strategy
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 21:00:00")
    
    def test_changetz_all_options_combined(self):
        """Test changetz with all options specified"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo --format '%Y-%m-%d %H:%M:%S' --ambiguous earliest - show")
        
        # Should work with all options specified
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 21:00:00")
        self.assert_output_contains(output, "2023-01-01 22:00:00")
        self.assert_output_contains(output, "2023-01-01 23:00:00")
    
    def test_changetz_nonexistent_column(self):
        """Test changetz on non-existent column should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - changetz nonexistent --from_tz UTC --to_tz Asia/Tokyo - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_changetz_missing_from_tz(self):
        """Test changetz without --from_tz should fail"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --to_tz Asia/Tokyo - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_changetz_missing_to_tz(self):
        """Test changetz without --to_tz should fail"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz UTC - show")
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_changetz_invalid_timezone(self):
        """Test changetz with invalid timezone should fail gracefully"""
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime --from_tz InvalidTZ --to_tz UTC - show")
        
        # Should fail and return empty output or original data
        # The exact behavior depends on implementation
        if output:
            # If it returns data, it should be the original unchanged data
            self.assert_output_contains(output, "2023-01-01 12:00:00")
        else:
            # If it fails completely
            self.assertEqual(output, "")

if __name__ == "__main__":
    unittest.main()