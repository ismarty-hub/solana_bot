
import unittest
from unittest.mock import MagicMock, patch
import os
import sys

# Add the project directory to sys.path
sys.path.append(os.getcwd())

class TestMLFiltering(unittest.TestCase):

    def test_calculate_timeframe_stats_filtering(self):
        """Test that calculate_timeframe_stats only considers tokens with ML_PASSED=True"""
        from analytics_tracker import calculate_timeframe_stats
        
        tokens = [
            {"mint": "T1", "ML_PASSED": True, "status": "win", "ath_roi": 100, "final_roi": 50},
            {"mint": "T2", "ML_PASSED": False, "status": "win", "ath_roi": 500, "final_roi": 250},
            {"mint": "T3", "ML_PASSED": True, "status": "loss", "ath_roi": 10, "final_roi": -90},
            {"mint": "T4", "status": "win", "ath_roi": 1000} # Missing ML_PASSED (legacy)
        ]
        
        stats = calculate_timeframe_stats(tokens)
        
        # Should only count T1 and T3
        self.assertEqual(stats["total_tokens"], 2)
        self.assertEqual(stats["wins"], 1) # Only T1
        self.assertEqual(stats["losses"], 1) # Only T3
        self.assertEqual(stats["max_roi"], 100) # From T1
        self.assertEqual(stats["average_ath_wins"], 100.0) # From T1

    @patch('analytics_tracker.load_json')
    @patch('analytics_tracker.save_json')
    @patch('analytics_tracker.upload_file_to_supabase')
    @patch('analytics_tracker.download_file_from_supabase')
    def test_update_daily_file_entry_filtering(self, mock_download, mock_upload, mock_save, mock_load):
        """Test that update_daily_file_entry records all tokens but filters daily_summary"""
        from analytics_tracker import update_daily_file_entry
        import asyncio
        
        # Mock daily file content
        mock_load.return_value = {
            "date": "2023-10-01",
            "signal_type": "discovery",
            "tokens": [],
            "daily_summary": {}
        }
        mock_save.return_value = "mock_path"
        mock_download.return_value = False
        mock_upload.return_value = True
        
        token_data = {"mint": "NEW", "ML_PASSED": False, "status": "win", "ath_roi": 1000}
        
        async def run_test():
            await update_daily_file_entry("2023-10-01", "discovery", token_data)
            
            # Check the data passed to save_json
            args, _ = mock_save.call_args
            saved_data = args[0]
            
            # All tokens should be in the 'tokens' list
            self.assertEqual(len(saved_data["tokens"]), 1)
            self.assertEqual(saved_data["tokens"][0]["mint"], "NEW")
            
            # But daily_summary should be zero because ML_PASSED is False
            self.assertEqual(saved_data["daily_summary"]["total_tokens"], 0)
            self.assertEqual(saved_data["daily_summary"]["wins"], 0)

        asyncio.run(run_test())

    @patch('alerts.monitoring.load_latest_tokens_from_overlap')
    @patch('alerts.monitoring.send_alert_to_subscribers')
    @patch('alerts.monitoring.safe_save')
    @patch('alerts.monitoring.safe_load')
    @patch('alerts.monitoring.download_latest_overlap')
    def test_discovery_alert_filtering(self, mock_dl, mock_load_state, mock_save, mock_send, mock_tokens):
        """Test that discovery alerts are skipped for ML_PASSED=False"""
        from alerts.monitoring import background_loop
        import asyncio
        
        # Setup mocks
        mock_tokens.return_value = {
            "FAIL": {"grade": "HIGH", "ml_passed": False, "token_metadata": {"mint": "FAIL"}},
            "PASS": {"grade": "HIGH", "ml_passed": True, "token_metadata": {"mint": "PASS"}}
        }
        mock_load_state.return_value = {} # Fresh state
        
        # We need to break the infinite loop after 1 cycle
        async def run_test():
            try:
                # We'll pulse the loop once and then raise an exception to exit
                with patch('asyncio.sleep', side_effect=Exception("StopLoop")):
                    await background_loop(MagicMock(), MagicMock())
            except Exception as e:
                if str(e) != "StopLoop":
                    raise e
            
            # Verify send_alert_to_subscribers was called ONLY for PASS
            self.assertEqual(mock_send.call_count, 1)
            args, _ = mock_send.call_args
            self.assertEqual(args[1]["token_metadata"]["mint"], "PASS")

        asyncio.run(run_test())

if __name__ == '__main__':
    unittest.main()
