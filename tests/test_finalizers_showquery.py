import unittest
from test_base import QsvTestBase

class TestShowquery(QsvTestBase):
    """
    Test showquery finalizer module
    """
    
    def test_showquery_basic(self):
        """Test basic showquery functionality"""
        output = self.run_qsv_command("load sample/simple.csv - showquery")
        
        # Should show Polars query plan information
        # The exact format depends on Polars implementation, but should contain query-related terms
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Common terms that might appear in a Polars query plan
        query_terms = ["PLAN", "SELECT", "SCAN", "CSV", "PROJECT", "FILTER"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")
    
    def test_showquery_after_select(self):
        """Test showquery after column selection"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,str - showquery")
        
        # Should show query plan including column selection
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Should contain information about the operations performed
        query_terms = ["PLAN", "SELECT", "PROJECT", "SCAN"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")
    
    def test_showquery_after_filtering(self):
        """Test showquery after filtering operations"""
        output = self.run_qsv_command("load sample/simple.csv - grep 'foo' - showquery")
        
        # Should show query plan including filtering
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Should contain information about the operations performed
        query_terms = ["PLAN", "FILTER", "SCAN", "SELECT"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")
    
    def test_showquery_after_head(self):
        """Test showquery after head operation"""
        output = self.run_qsv_command("load sample/simple.csv - head 2 - showquery")
        
        # Should show query plan including limit operation
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Should contain information about the operations performed
        query_terms = ["PLAN", "LIMIT", "SCAN", "SELECT"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")
    
    def test_showquery_after_sort(self):
        """Test showquery after sorting operations"""
        output = self.run_qsv_command("load sample/simple.csv - sort str - showquery")
        
        # Should show query plan including sort operation
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Should contain information about the operations performed
        query_terms = ["PLAN", "SORT", "SCAN", "SELECT"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")
    
    def test_showquery_complex_chain(self):
        """Test showquery after complex operation chain"""
        output = self.run_qsv_command("load sample/simple.csv - select col1,str - grep 'ba' - head 1 - showquery")
        
        # Should show query plan for complex chain
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Should contain information about the operations performed
        query_terms = ["PLAN", "SCAN", "SELECT", "PROJECT", "FILTER", "LIMIT"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")
    
    def test_showquery_after_contains(self):
        """Test showquery after contains operation"""
        output = self.run_qsv_command("load sample/simple.csv - contains str foo - showquery")
        
        # Should show query plan including contains filter
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Should contain information about the operations performed
        query_terms = ["PLAN", "FILTER", "SCAN", "SELECT"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")
    
    def test_showquery_after_isin(self):
        """Test showquery after isin operation"""
        output = self.run_qsv_command("load sample/simple.csv - isin str foo,bar - showquery")
        
        # Should show query plan including isin filter
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Should contain information about the operations performed
        query_terms = ["PLAN", "FILTER", "SCAN", "SELECT"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")
    
    def test_showquery_after_uniq(self):
        """Test showquery after uniq operation"""
        output = self.run_qsv_command("load sample/simple.csv - uniq str - showquery")
        
        # Should show query plan including unique operation
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Should contain information about the operations performed
        query_terms = ["PLAN", "UNIQUE", "SCAN", "SELECT", "DISTINCT"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")
    
    def test_showquery_after_renamecol(self):
        """Test showquery after column renaming"""
        output = self.run_qsv_command("load sample/simple.csv - renamecol str text - showquery")
        
        # Should show query plan including column renaming
        self.assertTrue(len(output) > 0, "Showquery should produce output")
        
        # Should contain information about the operations performed
        query_terms = ["PLAN", "SCAN", "SELECT", "PROJECT", "RENAME"]
        has_query_term = any(term.lower() in output.lower() for term in query_terms)
        self.assertTrue(has_query_term, f"Output should contain query plan information. Got: {output}")

if __name__ == "__main__":
    unittest.main() 