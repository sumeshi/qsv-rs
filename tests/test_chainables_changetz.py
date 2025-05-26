import unittest
import re
from test_base import QsvTestBase

class TestChangetz(QsvTestBase):
    """
    Test changetz chainable module
    """
    
    def test_changetz_basic(self):
        """Test converting timezone of a datetime column"""
        # Use Security_test.csv which has a simpler date/time column
        output = self.run_qsv_command("load sample/simple.csv - changetz datetime UTC EST - show")
        
        # 出力に"datetime"列が含まれていることを確認
        self.assertNotEqual("", output)
        self.assert_output_contains(output, "datetime")
        
        # 変換後の時刻がEST相当になっているか（例: 7:00:00, 8:00:00, 9:00:00 など）
        self.assertTrue("07:00:00" in output or "08:00:00" in output or "09:00:00" in output)
    
    def test_changetz_with_format(self):
        """Test timezone conversion with a specified format"""
        # 特定のフォーマットを指定して変換
        cmd = "load sample/simple.csv - changetz datetime UTC EST '%Y-%m-%d %H:%M:%S' - show"
        output = self.run_qsv_command(cmd)
        
        # 出力に"datetime"列が含まれていることを確認
        self.assertNotEqual("", output)
        self.assert_output_contains(output, "datetime")
        
        # 指定したフォーマットで出力されているか確認（数字とコロンと空白のパターン）
        # 日付パターン(YYYY-MM-DD HH:MM:SS)に一致するものが含まれているか確認
        date_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
        match = re.search(date_pattern, output)
        self.assertTrue(match, "指定したフォーマットで日付が出力されていません")

if __name__ == "__main__":
    unittest.main()