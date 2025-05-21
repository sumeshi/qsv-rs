import unittest
import os
from test_base import QsvTestBase

class TestQuiltVisualize(QsvTestBase):
    """
    Test quilt-visualize module
    """
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        # 設定ファイルのパス
        self.config_file = os.path.join(self.root_dir, "tests", "test_quilt_config.yaml")
        # 一時出力ファイルのパス（HTMLファイル）
        self.temp_output = os.path.join(self.root_dir, "sample", "temp_quilt_viz.html")
        # 既存の出力ファイルをクリーンアップ
        if os.path.exists(self.temp_output):
            os.remove(self.temp_output)
    
    def tearDown(self):
        """Tear down test fixtures"""
        # テスト後に出力ファイルをクリーンアップ
        if os.path.exists(self.temp_output):
            os.remove(self.temp_output)
    
    def test_quilt_visualize_basic(self):
        """Test basic quilt-visualize functionality"""
        # quilt-visualizeコマンドを実行
        output = self.run_qsv_command(f"quilt-visualize {self.config_file}")
        
        # コマンドがエラーなく実行されたか確認
        self.assertNotEqual("", output)
        # HTMLが生成されているか確認
        self.assert_output_contains(output, "<!DOCTYPE html>")
        self.assert_output_contains(output, "<html")
        # 設定ファイルの情報が含まれているか確認
        self.assert_output_contains(output, "Test Quilt Configuration")
    
    def test_quilt_visualize_with_output(self):
        """Test quilt-visualize with output file"""
        # 出力ファイルを指定してquilt-visualizeコマンドを実行
        self.run_qsv_command(f"quilt-visualize {self.config_file} -o={self.temp_output}")
        
        # 出力ファイルが生成されたか確認
        self.assertTrue(os.path.exists(self.temp_output), "出力HTMLファイルが生成されていません")
        
        # 出力ファイルの内容を確認
        with open(self.temp_output, 'r') as f:
            content = f.read()
            self.assertIn("<!DOCTYPE html>", content, "出力ファイルがHTML形式ではありません")
            self.assertIn("Test Quilt Configuration", content, "タイトル情報が含まれていません")
    
    def test_quilt_visualize_with_title(self):
        """Test quilt-visualize with title"""
        # タイトルを指定してquilt-visualizeコマンドを実行
        output = self.run_qsv_command(f"quilt-visualize {self.config_file} -t=\"Custom Visualization Title\"")
        
        # コマンドがエラーなく実行されたか確認
        self.assertNotEqual("", output)
        # カスタムタイトルが含まれているか確認
        self.assert_output_contains(output, "Custom Visualization Title")
        # オリジナルのタイトルが上書きされているか確認
        self.assertNotIn("Test Quilt Configuration", output)

if __name__ == "__main__":
    unittest.main()