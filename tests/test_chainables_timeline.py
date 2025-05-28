#!/usr/bin/env python3

import subprocess
import tempfile
import os
from test_base import QsvTestBase

class TestTimeline(QsvTestBase):
    def setUp(self):
        super().setUp()
        # Create test data with timestamps and values
        self.timeline_data = """timestamp,event,value,cpu_usage
2023-01-01 10:00:00,login,1,25.5
2023-01-01 10:15:00,page_view,5,30.2
2023-01-01 10:30:00,purchase,100,45.8
2023-01-01 10:45:00,logout,1,20.1
2023-01-01 11:00:00,login,1,28.7
2023-01-01 11:15:00,page_view,3,35.4
2023-01-01 11:30:00,page_view,7,42.1
2023-01-01 11:45:00,purchase,250,38.9
2023-01-01 12:00:00,logout,1,22.3
2023-01-01 12:15:00,login,1,26.8
2023-01-01 12:30:00,page_view,2,31.5
2023-01-01 12:45:00,purchase,75,29.7"""
        
        # Create temporary file
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_file.write(self.timeline_data)
        self.temp_file.close()
        self.timeline_file = self.temp_file.name

    def tearDown(self):
        # Clean up temporary file
        if os.path.exists(self.timeline_file):
            os.unlink(self.timeline_file)

    def test_timeline_basic_count(self):
        """Test basic timeline aggregation with count only"""
        result = self.run_qsv_command(f"load {self.timeline_file} - timeline timestamp --interval 1h - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timeline_bucket,count")
            self.assertIn("2023-01-01 10:00:00,4", result)
            self.assertIn("2023-01-01 11:00:00,4", result)
            self.assertIn("2023-01-01 12:00:00,4", result)

    def test_timeline_sum_aggregation(self):
        """Test timeline with sum aggregation"""
        result = self.run_qsv_command(f"load {self.timeline_file} - timeline timestamp --interval 1h --sum value - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timeline_bucket,count,sum_value")
            self.assertIn("2023-01-01 10:00:00,4,107.0", result)
            self.assertIn("2023-01-01 11:00:00,4,261.0", result)

    def test_timeline_avg_aggregation(self):
        """Test timeline with average aggregation"""
        result = self.run_qsv_command(f"load {self.timeline_file} - timeline timestamp --interval 1h --avg cpu_usage - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timeline_bucket,count,avg_cpu_usage")
            # Check that we have the expected number of rows (header + 3 data rows)
            self.assertEqual(len(lines), 4)

    def test_timeline_30min_intervals(self):
        """Test timeline with 30-minute intervals"""
        result = self.run_qsv_command(f"load {self.timeline_file} - timeline timestamp --interval 30m - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timeline_bucket,count")
            # Should have more buckets with 30-minute intervals
            self.assertGreater(len(lines), 4)  # More than 1h intervals

    def test_timeline_after_filtering(self):
        """Test timeline after other operations"""
        result = self.run_qsv_command(f"load {self.timeline_file} - contains event purchase - timeline timestamp --interval 1h --sum value - show")
        
        lines = result.strip().split('\n')
        if len(lines) > 0:
            self.assertEqual(lines[0], "timeline_bucket,count,sum_value")
            # Should only have purchase events
            self.assertGreater(len(lines), 1)

if __name__ == '__main__':
    import unittest
    unittest.main() 