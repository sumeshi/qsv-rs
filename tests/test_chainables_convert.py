import unittest
import tempfile
import os
from test_base import QsvTestBase

class TestConvert(QsvTestBase):

    def test_convert_json_to_json(self):
        """Test converting JSON to JSON format"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_withjson.csv')} - convert json --from json --to json - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,str,json",
            "2023-01-01 12:00:00,1,2,3,foo,\"{",
            "  \"\"age\"\": 25,",
            "  \"\"city\"\": \"\"Tokyo\"\",",
            "  \"\"name\"\": \"\"Alice\"\"",
            "}\"",
            "2023-01-01 13:00:00,4,5,6,bar,\"{",
            "  \"\"in_stock\"\": true,",
            "  \"\"price\"\": 89999,",
            "  \"\"product\"\": \"\"laptop\"\"",
            "}\"",
            "2023-01-01 14:00:00,7,8,9,baz,\"{",
            "  \"\"attendees\"\": [",
            "    \"\"John\"\",",
            "    \"\"Jane\"\",",
            "    \"\"Bob\"\"",
            "  ],",
            "  \"\"event\"\": \"\"conference\"\",",
            "  \"\"location\"\": \"\"Osaka\"\"",
            "}\"",
        ]))
    
    def test_convert_json_to_yaml(self):
        """Test converting JSON to YAML format"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_withjson.csv')} - convert json --from json --to yaml - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,str,json",
            "2023-01-01 12:00:00,1,2,3,foo,\"age: 25",
            "city: Tokyo",
            "name: Alice\"",
            "2023-01-01 13:00:00,4,5,6,bar,\"in_stock: true",
            "price: 89999",
            "product: laptop\"",
            "2023-01-01 14:00:00,7,8,9,baz,\"attendees:",
            "- John",
            "- Jane",
            "- Bob",
            "event: conference",
            "location: Osaka\"",
        ]))
    
    def test_convert_json_to_xml(self):
        """Test converting JSON to XML format"""
        result = self.run_qsv_command(f"load {self.get_fixture_path('simple_withjson.csv')} - convert json --from json --to xml - show")
        self.assertEqual(result.stdout.strip(), "\n".join([
            "datetime,col1,col2,col3,str,json",
            "2023-01-01 12:00:00,1,2,3,foo,<age>25</age><city>Tokyo</city><name>Alice</name>",
            "2023-01-01 13:00:00,4,5,6,bar,<in_stock>true</in_stock><price>89999</price><product>laptop</product>",
            "2023-01-01 14:00:00,7,8,9,baz,<attendees><item0>John</item0><item1>Jane</item1><item2>Bob</item2></attendees><event>conference</event><location>Osaka</location>",
        ]))

if __name__ == "__main__":
    unittest.main() 