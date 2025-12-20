
import unittest
from unittest.mock import MagicMock, patch
import os
import sys
from datetime import datetime, timezone

# Add the project directory to sys.path
sys.path.append(os.getcwd())

class TestAlertFiltering(unittest.TestCase):

    @patch('alerts.monitoring.load_latest_tokens_from_overlap')
    @patch('alerts.monitoring.send_alert_to_subscribers')
    @patch('alerts.monitoring.safe_save')
    @patch('alerts.monitoring.safe_load')
    @patch('alerts.monitoring.download_latest_overlap')
    def test_discovery_alert_filtering_logic(self, mock_dl, mock_load_state, mock_save, mock_send, mock_tokens):
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
                # print(f"Caught expected exception: {e}")
                pass
            
            # Verify send_alert_to_subscribers was called ONLY for PASS
            # Wait, mock_send might be a coroutine mock
            self.assertTrue(mock_send.called)
            
            # Check arguments of all calls
            mints_alerted = [call.args[1]["token_metadata"]["mint"] for call in mock_send.call_args_list]
            print(f"Mints alerted: {mints_alerted}")
            
            assert "PASS" in mints_alerted
            assert "FAIL" not in mints_alerted

        asyncio.run(run_test())

    @patch('alerts.alpha_monitoring.load_latest_alpha_tokens')
    @patch('alerts.alpha_monitoring.send_alpha_alert')
    @patch('alerts.alpha_monitoring.safe_save')
    @patch('alerts.alpha_monitoring.safe_load')
    def test_alpha_alert_filtering_logic(self, mock_load_state, mock_save, mock_send, mock_tokens):
        """Test that alpha alerts are skipped for ML_PASSED=False"""
        from alerts.alpha_monitoring import alpha_monitoring_loop
        import asyncio
        
        # Setup mocks
        mock_tokens.return_value = {
            "FAIL": [{"result": {"grade": "HIGH"}, "ML_PASSED": False}],
            "PASS": [{"result": {"grade": "HIGH"}, "ML_PASSED": True}]
        }
        mock_load_state.return_value = {} # Fresh state
        
        async def run_test():
            try:
                with patch('asyncio.sleep', side_effect=Exception("StopLoop")):
                    # We need to mock download_alpha_overlap_results too if it's called
                    with patch('alerts.alpha_monitoring.download_alpha_overlap_results', return_value=True):
                        await alpha_monitoring_loop(MagicMock(), MagicMock())
            except Exception as e:
                pass
            
            # Verify send_alpha_alert was called ONLY for PASS
            mints_alerted = [call.args[2] for call in mock_send.call_args_list]
            print(f"Alpha Mints alerted: {mints_alerted}")
            
            assert "PASS" in mints_alerted
            assert "FAIL" not in mints_alerted

        asyncio.run(run_test())

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestAlertFiltering)
    unittest.TextTestRunner(verbosity=2).run(suite)
