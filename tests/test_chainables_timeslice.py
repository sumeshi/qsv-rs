#!/usr/bin/env python3

import subprocess
import tempfile
import os
from test_base import QsvTestBase

class TestTimeslice(QsvTestBase):
    def setUp(self):
        super().setUp()
        # Create test data with timestamps spanning multiple days
        self.timeslice_data = """timestamp,event,value
2023-01-01 09:30:00,early_event,10
2023-01-01 10:00:00,login,1
2023-01-01 10:15:00,page_view,5
2023-01-01 10:30:00,purchase,100
2023-01-01 11:00:00,login,1
2023-01-01 11:15:00,page_view,3
2023-01-01 11:30:00,page_view,7
2023-01-01 12:00:00,logout,1
2023-01-01 12:15:00,login,1
2023-01-01 12:30:00,page_view,2
2023-01-01 13:00:00,late_event,20
2023-01-02 10:00:00,next_day,50"""
        
        # Create temporary file
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_file.write(self.timeslice_data)
        self.temp_file.close()
        self.timeslice_file = self.temp_file.name

    def tearDown(self):
        # Clean up temporary file
        if os.path.exists(self.timeslice_file):
            os.unlink(self.timeslice_file)

    def test_timeslice_start_only(self):
        """Test timeslice with only start time specified"""
        result = self.run_qsv_command(f"load {self.timeslice_file} - timeslice timestamp --start '2023-01-01 11:00:00' - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timestamp,event,value")
            
            # Should include events from 11:00:00 onwards
            timestamps = [line.split(',')[0] for line in lines[1:]]
            for ts in timestamps:
                self.assertGreaterEqual(ts, "2023-01-01 11:00:00")

    def test_timeslice_end_only(self):
        """Test timeslice with only end time specified"""
        result = self.run_qsv_command(f"load {self.timeslice_file} - timeslice timestamp --end '2023-01-01 11:30:00' - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timestamp,event,value")
            
            # Should include events up to 11:30:00
            timestamps = [line.split(',')[0] for line in lines[1:]]
            for ts in timestamps:
                self.assertLessEqual(ts, "2023-01-01 11:30:00")

    def test_timeslice_start_and_end(self):
        """Test timeslice with both start and end times"""
        result = self.run_qsv_command(f"load {self.timeslice_file} - timeslice timestamp --start '2023-01-01 10:00:00' --end '2023-01-01 12:00:00' - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timestamp,event,value")
            
            # Should include events between 10:00:00 and 12:00:00 (inclusive)
            timestamps = [line.split(',')[0] for line in lines[1:]]
            for ts in timestamps:
                self.assertGreaterEqual(ts, "2023-01-01 10:00:00")
                self.assertLessEqual(ts, "2023-01-01 12:00:00")

    def test_timeslice_exact_boundaries(self):
        """Test timeslice with exact timestamp boundaries"""
        result = self.run_qsv_command(f"load {self.timeslice_file} - timeslice timestamp --start '2023-01-01 10:15:00' --end '2023-01-01 11:15:00' - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timestamp,event,value")
            
            # Should include boundary events
            content = result.strip()
            self.assertIn("2023-01-01 10:15:00,page_view,5", content)
            self.assertIn("2023-01-01 11:15:00,page_view,3", content)

    def test_timeslice_no_matches(self):
        """Test timeslice with time range that has no matches"""
        result = self.run_qsv_command(f"load {self.timeslice_file} - timeslice timestamp --start '2023-01-03 00:00:00' --end '2023-01-03 23:59:59' - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timestamp,event,value")
            # Should only have header, no data rows
            self.assertEqual(len(lines), 1)

if __name__ == '__main__':
    import unittest
    unittest.main() 