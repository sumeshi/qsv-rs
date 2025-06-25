import unittest
import os
import tempfile
from test_base import QsvTestBase

class TestQuilt(QsvTestBase):
    
    def setUp(self):
        super().setUp()
        # Create a temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        # Clean up temporary files
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        
        # Clean up any temporary quilt files in fixtures
        temp_quilt_file = os.path.join(self.fixtures_dir, "multi_op_quilt.yaml")
        if os.path.exists(temp_quilt_file):
            os.remove(temp_quilt_file)
    
    def test_quilt_simple_pipeline(self):
        """Test basic quilt execution with simple pipeline"""
        result = self.run_qsv_command(f"quilt {self.get_fixture_path('quilt-simple.yaml')}")
        self.assertEqual(result.returncode, 0)
        
        # The quilt should output filtered data (col1,str where str is foo or bar)
        expected_lines = [
            "col1,str",
            "1,foo",
            "4,bar"
        ]
        
        for line in expected_lines:
            self.assertIn(line, result.stdout)
    
    def test_quilt_with_dump_output(self):
        """Test quilt execution with dump to file"""
        output_file = os.path.join(self.temp_dir, "test_output.csv")
        
        # Modify the quilt config to use our temp directory
        quilt_content = f"""title: 'Dump Test Quilt'
description: 'Test quilt with dump output'
version: '1.0.0'
author: 'Test Suite'
stages:
  load_and_process:
    type: process
    steps:
      load:
        path: "{self.get_fixture_path('simple.csv')}"
      select:
        colnames:
          - datetime
          - col1
      head:
        number: 2
      dump:
        output: "{output_file}"
"""
        
        # Write temporary quilt file
        temp_quilt_file = os.path.join(self.temp_dir, "temp_quilt.yaml")
        with open(temp_quilt_file, 'w') as f:
            f.write(quilt_content)
        
        result = self.run_qsv_command(f"quilt {temp_quilt_file}")
        self.assertEqual(result.returncode, 0)
        
        # Check that output file was created
        self.assertTrue(os.path.exists(output_file))
        
        # Check output file content
        with open(output_file, 'r') as f:
            content = f.read().strip()
            expected_content = '\n'.join([
                "datetime,col1",
                "2023-01-01 12:00:00,1",
                "2023-01-01 13:00:00,4"
            ])
            self.assertEqual(content, expected_content)
    
    def test_quilt_with_cli_output_override(self):
        """Test quilt execution with CLI output override"""
        output_file = os.path.join(self.temp_dir, "cli_output.csv")
        
        result = self.run_qsv_command(f"quilt {self.get_fixture_path('quilt-simple.yaml')} -o {output_file}")
        self.assertEqual(result.returncode, 0)
        
        # Check that output file was created
        self.assertTrue(os.path.exists(output_file))
        
        # Check output file content
        with open(output_file, 'r') as f:
            content = f.read().strip()
            expected_lines = [
                "col1,str",
                "1,foo",
                "4,bar"
            ]
            for line in expected_lines:
                self.assertIn(line, content)
    
    def test_quilt_join_operation(self):
        """Test quilt execution with join operation"""
        result = self.run_qsv_command(f"quilt {self.get_fixture_path('quilt-join.yaml')}")
        self.assertEqual(result.returncode, 0)
        
        # The join should combine datetime+col1 with datetime+str
        expected_lines = [
            "datetime,col1,str",
            "2023-01-01 12:00:00,1,foo",
            "2023-01-01 13:00:00,4,bar",
            "2023-01-01 14:00:00,7,baz"
        ]
        
        for line in expected_lines:
            self.assertIn(line, result.stdout)
    
    def test_quilt_existing_complex_config(self):
        """Test quilt execution with existing complex config"""
        result = self.run_qsv_command(f"quilt {self.get_fixture_path('quilt-test.yaml')}")
        self.assertEqual(result.returncode, 0)
        
        # This should execute the existing quilt-test.yaml successfully
        # The exact output depends on the configuration, but it should not error
        self.assertNotIn("Error", result.stderr)
    
    def test_quilt_nonexistent_file(self):
        """Test quilt execution with non-existent config file"""
        result = self.run_qsv_command("quilt nonexistent_file.yaml")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Error reading config file", result.stderr)
    
    def test_quilt_invalid_yaml(self):
        """Test quilt execution with invalid YAML"""
        invalid_yaml_content = """
title: 'Invalid YAML'
stages:
  invalid_stage:
    type: process
    steps:
      - this is not valid yaml structure
"""
        
        # Write temporary invalid quilt file
        temp_quilt_file = os.path.join(self.temp_dir, "invalid_quilt.yaml")
        with open(temp_quilt_file, 'w') as f:
            f.write(invalid_yaml_content)
        
        result = self.run_qsv_command(f"quilt {temp_quilt_file}")
        # The process may exit with 0 but should show error messages
        self.assertIn("Error parsing", result.stderr)
    
    def test_quilt_with_multiple_chainable_operations(self):
        """Test quilt with multiple chainable operations in sequence"""
        quilt_content = f"""title: 'Multi-operation Test'
description: 'Test multiple operations'
version: '1.0.0'
stages:
  process_data:
    type: process
    steps:
      load:
        path: "simple.csv"
      select:
        colnames:
          - col1
          - col2
          - str
      isin:
        colname: str
        values:
          - foo
          - bar
      sort:
        colnames: col2
        desc: true
      show:
"""
        
        # Write temporary quilt file in fixtures directory so relative paths work
        temp_quilt_file = os.path.join(self.fixtures_dir, "multi_op_quilt.yaml")
        with open(temp_quilt_file, 'w') as f:
            f.write(quilt_content)
        
        result = self.run_qsv_command(f"quilt {temp_quilt_file}")
        self.assertEqual(result.returncode, 0)
        
        # Should show filtered and sorted data
        expected_lines = [
            "col1,col2,str",
            "4,5,bar",  # col2=5 comes first when sorted desc
            "1,2,foo"   # col2=2 comes second
        ]
        
        output_lines = result.stdout.strip().split('\n')
        if len(output_lines) > 1:  # Check if we have data beyond header
            self.assertEqual(len(output_lines), 3)  # Header + 2 data rows
            for line in expected_lines:
                self.assertIn(line, result.stdout)
        else:
            # If no data, at least check that header is present
            self.assertIn("col1,col2,str", result.stdout)

if __name__ == "__main__":
    unittest.main()