import unittest
from test_base import QsvTestBase

class TestChangetz(QsvTestBase):
    
    def test_changetz_basic(self):
        """Test basic timezone change functionality"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - changetz datetime --from-tz UTC --to-tz UTC - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,str",
            "2023-01-01T12:00:00.000000+00:00,1,2,3,foo",
            "2023-01-01T13:00:00.000000+00:00,4,5,6,bar",
            "2023-01-01T14:00:00.000000+00:00,7,8,9,baz",
        ]))
    
    def test_changetz_to_losangeles(self):
        """Test timezone change to Los Angeles"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - changetz datetime --from-tz UTC --to-tz America/Los_Angeles - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,str",
            "2023-01-01T04:00:00.000000-08:00,1,2,3,foo",
            "2023-01-01T05:00:00.000000-08:00,4,5,6,bar",
            "2023-01-01T06:00:00.000000-08:00,7,8,9,baz",
        ]))
    
    def test_changetz_to_tokyo(self):
        """Test timezone change to Tokyo"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - changetz datetime --from-tz UTC --to-tz Asia/Tokyo - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,str",
            "2023-01-01T21:00:00.000000+09:00,1,2,3,foo",
            "2023-01-01T22:00:00.000000+09:00,4,5,6,bar",
            "2023-01-01T23:00:00.000000+09:00,7,8,9,baz",
        ]))
    
    def test_changetz_with_input_format(self):
        """Test changetz with input format"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - changetz datetime --from-tz UTC --to-tz Asia/Tokyo --input-format '%Y-%m-%d %H:%M:%S' - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,str",
            "2023-01-01T21:00:00.000000+09:00,1,2,3,foo",
            "2023-01-01T22:00:00.000000+09:00,4,5,6,bar",
            "2023-01-01T23:00:00.000000+09:00,7,8,9,baz",
        ]))
    
    def test_changetz_with_output_format(self):
        """Test changetz with output format"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - changetz datetime --from-tz UTC --to-tz Asia/Tokyo --output-format '%Y-%m-%d %H:%M:%S' - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,str",
            "2023-01-01 21:00:00,1,2,3,foo",
            "2023-01-01 22:00:00,4,5,6,bar",
            "2023-01-01 23:00:00,7,8,9,baz",
        ]))
    
    
    def test_changetz_dst_comprehensive_earliest(self):
        """Test comprehensive DST scenarios with earliest strategy"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('dst_comprehensive.csv')} - changetz datetime --from-tz America/Los_Angeles --to-tz UTC --input-format '%Y-%m-%d %H:%M:%S' --ambiguous earliest - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,timezone,description",
            "2023-11-05T07:30:00.000000+00:00,Los Angeles,Before DST transition",     # PDT (UTC-7): 00:30 → 07:30
            "2023-11-05T08:30:00.000000+00:00,Los Angeles,Ambiguous time (1st occurrence)",  # PDT (UTC-7): 01:30 → 08:30
            "2023-11-05T08:45:00.000000+00:00,Los Angeles,Ambiguous time (still in overlap)", # PDT (UTC-7): 01:45 → 08:45
            "2023-11-05T10:30:00.000000+00:00,Los Angeles,After DST transition",     # PST (UTC-8): 02:30 → 10:30
            "2023-03-12T09:30:00.000000+00:00,Los Angeles,Non-existent time (spring forward)", # PST (UTC-8): 01:30 → 09:30 (interpreted as PST)
            "2023-03-12T10:30:00.000000+00:00,Los Angeles,After spring forward",     # PDT (UTC-7): 03:30 → 10:30
        ]))

    def test_changetz_dst_comprehensive_latest(self):
        """Test comprehensive DST scenarios with latest strategy"""  
        result = self.run_qsv_command(f"load {self.get_fixture_path('dst_comprehensive.csv')} - changetz datetime --from-tz America/Los_Angeles --to-tz UTC --input-format '%Y-%m-%d %H:%M:%S' --ambiguous latest - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,timezone,description",
            "2023-11-05T07:30:00.000000+00:00,Los Angeles,Before DST transition",     # PDT (UTC-7): 00:30 → 07:30
            "2023-11-05T09:30:00.000000+00:00,Los Angeles,Ambiguous time (1st occurrence)",  # PST (UTC-8): 01:30 → 09:30 (latest interpretation)
            "2023-11-05T09:45:00.000000+00:00,Los Angeles,Ambiguous time (still in overlap)", # PST (UTC-8): 01:45 → 09:45 (latest interpretation)  
            "2023-11-05T10:30:00.000000+00:00,Los Angeles,After DST transition",     # PST (UTC-8): 02:30 → 10:30
            "2023-03-12T09:30:00.000000+00:00,Los Angeles,Non-existent time (spring forward)", # PST (UTC-8): 01:30 → 09:30 (interpreted as PST)
            "2023-03-12T10:30:00.000000+00:00,Los Angeles,After spring forward",     # PDT (UTC-7): 03:30 → 10:30
        ]))

    def test_changetz_ambiguous_autumn_transition(self):
        """Test specific autumn DST transition (fall back scenario)"""
        # Test the original autumn transition test with correct fixture
        result_earliest = self.run_qsv_command(f"load {self.get_fixture_path('dst_ambiguous.csv')} - changetz datetime --from-tz America/Los_Angeles --to-tz UTC --input-format '%Y-%m-%d %H:%M:%S' --ambiguous earliest - show")
        self.assertEqual(result_earliest.stdout.strip(), "\n".join([
            "datetime,location,event",
            "2023-11-05T08:00:00.000000+00:00,Los Angeles,Before DST end",      # PDT (UTC-7): 01:00 → 08:00
            "2023-11-05T08:30:00.000000+00:00,Los Angeles,Ambiguous time",     # PDT (UTC-7): 01:30 → 08:30  
            "2023-11-05T08:45:00.000000+00:00,Los Angeles,Still ambiguous",    # PDT (UTC-7): 01:45 → 08:45
            "2023-11-05T10:30:00.000000+00:00,Los Angeles,After DST end",      # PST (UTC-8): 02:30 → 10:30
        ]))
        
        # Test latest strategy  
        result_latest = self.run_qsv_command(f"load {self.get_fixture_path('dst_ambiguous.csv')} - changetz datetime --from-tz America/Los_Angeles --to-tz UTC --input-format '%Y-%m-%d %H:%M:%S' --ambiguous latest - show")
        self.assertEqual(result_latest.stdout.strip(), "\n".join([
            "datetime,location,event", 
            "2023-11-05T09:00:00.000000+00:00,Los Angeles,Before DST end",      # PST (UTC-8): 01:00 → 09:00 (latest interpretation)
            "2023-11-05T09:30:00.000000+00:00,Los Angeles,Ambiguous time",     # PST (UTC-8): 01:30 → 09:30
            "2023-11-05T09:45:00.000000+00:00,Los Angeles,Still ambiguous",    # PST (UTC-8): 01:45 → 09:45
            "2023-11-05T10:30:00.000000+00:00,Los Angeles,After DST end",      # PST (UTC-8): 02:30 → 10:30
        ]))

    def test_changetz_invalid_source_timezone(self):
        """Test changetz with invalid source timezone exits with error"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - changetz datetime --from-tz Invalid/Timezone --to-tz UTC - show")
        # Expecting error exit code and error message
        self.assertNotEqual(result.returncode, 0)  # Should exit with error code
        self.assertIn("Error: Invalid source timezone", result.stderr)  # Should contain error message

    def test_changetz_invalid_target_timezone(self):
        """Test changetz with invalid timezone exits with error"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - changetz datetime --from-tz UTC --to-tz Invalid/Timezone - show")
        # Expecting error exit code and error message
        self.assertNotEqual(result.returncode, 0)  # Should exit with error code
        self.assertIn("Error: Invalid target timezone", result.stderr)  # Should contain error message

if __name__ == "__main__":
    unittest.main()