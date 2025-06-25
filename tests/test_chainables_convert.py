import unittest
import tempfile
import os
from test_base import QsvTestBase

class TestConvert(QsvTestBase):
    
    def test_convert_json_to_json(self):
        """Test converting JSON to JSON format"""
        self.maxDiff = 9999999999999

        command = f"load {self.get_fixture_path('comprehensive.csv')} - head 3 - convert json --from json --to json - select datetime,col1,col2,col3,str,json - show"
        result = self.run_qsv_command(command)
        self.assertEqual(result.stdout.strip(), "\n".join([
            'datetime,col1,col2,col3,str,json',
            '2023-01-01 12:00:00,1,2,3,foo,"{',
            '  ""age"": 25,',
            '  ""city"": ""Tokyo"",',
            '  ""name"": ""Alice""',
            '}"',
            '2023-01-01 13:00:00,4,5,6,bar,"{',
            '  ""in_stock"": true,',
            '  ""price"": 89999,',
            '  ""product"": ""laptop""',
            '}"',
            '2023-01-01 14:00:00,7,8,9,baz,"{',
            '  ""active"": false,',
            '  ""score"": 95,',
            '  ""user"": ""Bob""',
            '}"',
        ]))
    
    def test_convert_json_to_yaml(self):
        """Test converting JSON to YAML format"""
        command = f"load {self.get_fixture_path('comprehensive.csv')} - head 3 - convert json --from json --to yaml - select datetime,col1,col2,col3,str,json - show"
        result = self.run_qsv_command(command)
        self.assertEqual(result.stdout.strip(), "\n".join([
            'datetime,col1,col2,col3,str,json',
            '2023-01-01 12:00:00,1,2,3,foo,"age: 25',
            'city: Tokyo',
            'name: Alice"',
            '2023-01-01 13:00:00,4,5,6,bar,"in_stock: true',
            'price: 89999',
            'product: laptop"',
            '2023-01-01 14:00:00,7,8,9,baz,"active: false',
            'score: 95',
            'user: Bob"',
        ]))
    
    def test_convert_json_to_xml(self):
        """Test converting JSON to XML format"""
        command = f"load {self.get_fixture_path('comprehensive.csv')} - head 3 - convert json --from json --to xml - select datetime,col1,col2,col3,str,json - show"
        result = self.run_qsv_command(command)
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,str,json",
            "2023-01-01 12:00:00,1,2,3,foo,<age>25</age><city>Tokyo</city><name>Alice</name>",
            "2023-01-01 13:00:00,4,5,6,bar,<in_stock>true</in_stock><price>89999</price><product>laptop</product>",
            "2023-01-01 14:00:00,7,8,9,baz,<active>false</active><score>95</score><user>Bob</user>",
        ]))

if __name__ == "__main__":
    unittest.main() 