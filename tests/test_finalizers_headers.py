import unittest
from test_base import QsvTestBase

class TestHeaders(QsvTestBase):
    
    def test_headers_basic(self):
        """Test basic headers functionality"""
        self.assertEqual(
            self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - headers --plain").stdout.strip(),
            "\n".join([
                "datetime",
                "col1",
                "col2",
                "col3",
                "str",
            ])
        )

if __name__ == "__main__":
    unittest.main()