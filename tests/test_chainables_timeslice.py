#!/usr/bin/env python3

import subprocess
import tempfile
import os
import unittest
from test_base import QsvTestBase

class TestTimeslice(QsvTestBase):

    def test_timeslice_start_only(self):
        """Test timeslice with only start time specified"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_timeline.csv')} - timeslice datetime --start '2023-01-01 12:00:00' - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,str",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
            "2023-01-01 12:00:00,Mike",
        ]))

    def test_timeslice_end_only(self):
        """Test timeslice with only end time specified"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_timeline.csv')} - timeslice datetime --end '2023-01-01 03:00:00' - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,str",
            "2023-01-01 00:00:00,Alpha",
            "2023-01-01 01:00:00,Bravo",
            "2023-01-01 02:00:00,Charlie",
            "2023-01-01 01:00:00,Bravo",
            "2023-01-01 02:00:00,Charlie",
            "2023-01-01 03:00:00,Delta",
            "2023-01-01 02:00:00,Charlie",
            "2023-01-01 03:00:00,Delta",
            "2023-01-01 03:00:00,Delta",
            "2023-01-01 03:00:00,Delta",
        ]))

    def test_timeslice_start_and_end(self):
        """Test timeslice with both start and end times"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_timeline.csv')} - timeslice datetime --start '2023-01-01 03:00:00' --end '2023-01-01 06:00:00' - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,str",
            "2023-01-01 03:00:00,Delta",
            "2023-01-01 03:00:00,Delta",
            "2023-01-01 04:00:00,Echo",
            "2023-01-01 03:00:00,Delta",
            "2023-01-01 03:00:00,Delta",
            "2023-01-01 04:00:00,Echo",
            "2023-01-01 05:00:00,Foxtrot",
            "2023-01-01 04:00:00,Echo",
            "2023-01-01 04:00:00,Echo",
            "2023-01-01 04:00:00,Echo",
            "2023-01-01 05:00:00,Foxtrot",
            "2023-01-01 06:00:00,Golf",
            "2023-01-01 05:00:00,Foxtrot",
            "2023-01-01 05:00:00,Foxtrot",
            "2023-01-01 05:00:00,Foxtrot",
            "2023-01-01 05:00:00,Foxtrot",
            "2023-01-01 06:00:00,Golf",
            "2023-01-01 06:00:00,Golf",
            "2023-01-01 06:00:00,Golf",
            "2023-01-01 06:00:00,Golf",
            "2023-01-01 06:00:00,Golf",
            "2023-01-01 06:00:00,Golf",
        ]))

if __name__ == "__main__":
    unittest.main() 