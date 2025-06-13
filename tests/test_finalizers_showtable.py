import unittest
from test_base import QsvTestBase

class TestShowtable(QsvTestBase):
    
    def test_showtable_basic(self):
        """Test basic showtable functionality"""
        self.assertTrue(
            self.run_qsv_command(f"load {self.get_fixture_path('simple.csv')} - showtable").stdout.strip(),
            "\n".join([
                "shape: (3, 5),"
                "┌─────────────────────┬──────┬──────┬──────┬─────┐,"
                "│ datetime            ┆ col1 ┆ col2 ┆ col3 ┆ str │,"
                "╞═════════════════════╪══════╪══════╪══════╪═════╡,"
                "│ 2023-01-01 12:00:00 ┆ 1    ┆ 2    ┆ 3    ┆ foo │,"
                "├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤,"
                "│ 2023-01-01 13:00:00 ┆ 4    ┆ 5    ┆ 6    ┆ bar │,"
                "├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤,"
                "│ 2023-01-01 14:00:00 ┆ 7    ┆ 8    ┆ 9    ┆ baz │,"
                "└─────────────────────┴──────┴──────┴──────┴─────┘,"
            ])
        )
    
if __name__ == "__main__":
    unittest.main() 