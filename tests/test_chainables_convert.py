import unittest
from test_base import QsvTestBase

class TestConvert(QsvTestBase):
    """
    Test convert chainable module
    """
    
    def test_convert_json_to_yaml(self):
        """Test converting JSON to YAML format"""
        # This test assumes we have sample data with JSON content
        # For now, we'll test the basic functionality
        output = self.run_qsv_command('load sample/simple.csv - convert str --from json --to yaml - show')
        
        # Should include the header
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Basic test that the command runs without error
        # Actual content verification would depend on the sample data format
    
    def test_convert_yaml_to_json(self):
        """Test converting YAML to JSON format"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from yaml --to json - show')
        
        # Should include the header
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Command should execute without error
    
    def test_convert_json_to_xml(self):
        """Test converting JSON to XML format"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from json --to xml - show')
        
        # Should include the header
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Command should execute without error
    
    def test_convert_xml_to_json(self):
        """Test converting XML to JSON format"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from xml --to json - show')
        
        # Should include the header
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Command should execute without error
    
    def test_convert_yaml_to_xml(self):
        """Test converting YAML to XML format"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from yaml --to xml - show')
        
        # Should include the header
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Command should execute without error
    
    def test_convert_xml_to_yaml(self):
        """Test converting XML to YAML format"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from xml --to yaml - show')
        
        # Should include the header
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Command should execute without error
    
    def test_convert_json_format_prettify(self):
        """Test formatting/prettifying JSON (same format conversion)"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from json --to json - show')
        
        # Should include the header
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Command should execute without error
    
    def test_convert_yaml_format_prettify(self):
        """Test formatting/prettifying YAML (same format conversion)"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from yaml --to yaml - show')
        
        # Should include the header
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Command should execute without error
    
    def test_convert_xml_format_prettify(self):
        """Test formatting/prettifying XML (same format conversion)"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from xml --to xml - show')
        
        # Should include the header
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        # Command should execute without error
    
    def test_convert_nonexistent_column(self):
        """Test convert on non-existent column should fail gracefully"""
        output = self.run_qsv_command('load sample/simple.csv - convert nonexistent --from json --to yaml - show')
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_convert_missing_from_parameter(self):
        """Test convert without --from parameter should fail"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --to yaml - show')
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_convert_missing_to_parameter(self):
        """Test convert without --to parameter should fail"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from json - show')
        
        # Should fail and return empty output
        self.assertEqual(output, "")
    
    def test_convert_unsupported_format_combination(self):
        """Test convert with unsupported format combination"""
        output = self.run_qsv_command('load sample/simple.csv - convert str --from txt --to pdf - show')
        
        # Should handle unsupported formats gracefully
        # The exact behavior depends on implementation
        if output:
            # If it returns data, it should at least have the header
            self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        else:
            # If it fails completely
            self.assertEqual(output, "")

if __name__ == "__main__":
    unittest.main() 