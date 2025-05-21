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
        output = self.run_qsv_command("load sample/Security_test.csv - changetz Time UTC EST - show")
        
        # 出力に"Time"列が含まれていることを確認
        self.assertNotEqual("", output)
        self.assert_output_contains(output, "Time")
        
        # 変換前と変換後でタイムゾーンが変わっていることを確認（5時間の差があるはず）
        # タイムゾーン変換が正しく行われていれば、時間の値が変わっているはず
        self.assertTrue("EST" in output or "T" in output, "タイムゾーン情報が出力に含まれていません")
    
    def test_changetz_with_format(self):
        """Test timezone conversion with a specified format"""
        # 特定のフォーマットを指定して変換
        cmd = "load sample/Security_test.csv - changetz Time UTC EST '%Y-%m-%d %H:%M:%S' - show"
        output = self.run_qsv_command(cmd)
        
        # 出力に"Time"列が含まれていることを確認
        self.assertNotEqual("", output)
        self.assert_output_contains(output, "Time")
        
        # 指定したフォーマットで出力されているか確認（数字とコロンと空白のパターン）
        # 日付パターン(YYYY-MM-DD HH:MM:SS)に一致するものが含まれているか確認
        date_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
        match = re.search(date_pattern, output)
        self.assertTrue(match, "指定したフォーマットで日付が出力されていません")

if __name__ == "__main__":
    unittest.main()