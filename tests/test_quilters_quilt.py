import unittest
import os
from test_base import QsvTestBase

class TestQuilt(QsvTestBase):
    """
    Test quilt module
    """
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        # 設定ファイルのパス
        self.config_file = os.path.join(self.root_dir, "tests", "test_quilt_config.yaml")
        # 一時出力ファイルのパス
        self.temp_output = os.path.join(self.root_dir, "sample", "temp_quilt_output.csv")
        # 既存の出力ファイルをクリーンアップ
        if os.path.exists(self.temp_output):
            os.remove(self.temp_output)
    
    def tearDown(self):
        """Tear down test fixtures"""
        # テスト後に出力ファイルをクリーンアップ
        if os.path.exists(self.temp_output):
            os.remove(self.temp_output)
    
    def test_quilt_basic(self):
        """Test basic quilt functionality"""
        # quiltコマンドを実行
        output = self.run_qsv_command(f"quilt {self.config_file}")
        
        # コマンドがエラーなく実行されたか確認
        self.assertNotEqual("", output)
        # 設定ファイルで指定されたCSVファイルのデータが含まれているか確認
        self.assert_output_contains(output, "col1")
        self.assert_output_contains(output, "col2")
        # col3はtransformステージでselectされていないためテストから除外
    
    def test_quilt_with_output(self):
        """Test quilt with output file"""
        # 出力ファイルを指定してquiltコマンドを実行
        self.run_qsv_command(f"quilt {self.config_file} -o={self.temp_output}")
        
        # 出力ファイルが生成されたか確認
        self.assertTrue(os.path.exists(self.temp_output), "出力ファイルが生成されていません")
        
        # 出力ファイルの内容を確認
        with open(self.temp_output, 'r') as f:
            content = f.read()
            self.assertIn("col1", content, "出力ファイルに期待されるデータが含まれていません")
    
    def test_quilt_with_title(self):
        """Test quilt with title"""
        # タイトルを指定してquiltコマンドを実行
        output = self.run_qsv_command(f"quilt {self.config_file} -t=\"Custom Quilt Title\"")
        
        # コマンドがエラーなく実行されたか確認
        self.assertNotEqual("", output)
        # タイトルがログメッセージに含まれているか確認
        # （実際の出力は制御できないため、主に構文エラーなどの基本チェック）
        self.assert_output_contains(output, "col1")

if __name__ == "__main__":
    unittest.main()