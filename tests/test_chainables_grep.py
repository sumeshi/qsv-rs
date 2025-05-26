import unittest
from test_base import QsvTestBase

class TestGrep(QsvTestBase):
    """
    Test grep chainable module
    """
    
    def test_grep_foo(self):
        output = self.run_qsv_command("load sample/simple.csv - grep foo - show")
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 12:00:00,1,2,3,foo")
        self.assert_output_not_contains(output, "4,5,6,bar")
        self.assert_output_not_contains(output, "7,8,9,baz")

    def test_grep_bar(self):
        output = self.run_qsv_command("load sample/simple.csv - grep bar - show")
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 13:00:00,4,5,6,bar")
        self.assert_output_not_contains(output, "1,2,3,foo")
        self.assert_output_not_contains(output, "7,8,9,baz")

    def test_grep_baz_ignorecase(self):
        output = self.run_qsv_command("load sample/simple.csv - grep BAZ -i - show")
        self.assert_output_contains(output, "datetime,col1,col2,col3,str")
        self.assert_output_contains(output, "2023-01-01 14:00:00,7,8,9,baz")
        self.assert_output_not_contains(output, "1,2,3,foo")
        self.assert_output_not_contains(output, "4,5,6,bar")

if __name__ == "__main__":
    unittest.main()