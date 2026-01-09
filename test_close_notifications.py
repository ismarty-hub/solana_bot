import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from pathlib import Path

# Mocking modules that might be hard to import or have side effects
import sys
sys.modules['shared.file_io'] = MagicMock()
sys.modules['supabase_utils'] = MagicMock()
sys.modules['config'] = MagicMock()
sys.modules['config'].PORTFOLIOS_FILE = Path("test_portfolios.json")
sys.modules['config'].DATA_DIR = Path(".")
sys.modules['config'].SIGNAL_FRESHNESS_WINDOW = 3600

# Mock telegram
mock_telegram = MagicMock()
mock_telegram_ext = MagicMock()
sys.modules['telegram'] = mock_telegram
sys.modules['telegram.ext'] = mock_telegram_ext

from trade_manager import PortfolioManager

class TestCloseNotifications(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.portfolio_file = Path("test_portfolios.json")
        self.pm = PortfolioManager(self.portfolio_file)
        self.pm.save = MagicMock() # Don't actually save to disk
        
        self.chat_id = "123456789"
        self.pm.portfolios = {
            self.chat_id: {
                "capital_usd": 1000.0,
                "positions": {
                    "test_mint_discovery": {
                        "mint": "test_mint",
                        "symbol": "TEST",
                        "entry_price": 1.0,
                        "token_amount": 100,
                        "investment_usd": 100.0,
                        "signal_type": "discovery",
                        "entry_time": datetime.now(timezone.utc).isoformat()
                    }
                },
                "stats": {
                    "total_trades": 0,
                    "total_pnl": 0.0,
                    "wins": 0,
                    "losses": 0,
                    "best_trade": 0.0,
                    "worst_trade": 0.0
                },
                "trade_history": []
            }
        }
        
    async def test_exit_position_notifies_when_enabled(self):
        # Setup mocks
        app = MagicMock()
        app.bot.send_message = AsyncMock()
        
        user_manager = MagicMock()
        user_manager.get_user_prefs.return_value = {"trade_notifications_enabled": True}
        
        # Execute
        await self.pm.exit_position(
            self.chat_id, 
            "test_mint_discovery", 
            "Test Reason", 
            app, 
            user_manager, 
            exit_roi=10.0
        )
        
        # Verify
        app.bot.send_message.assert_called_once()
        args, kwargs = app.bot.send_message.call_args
        self.assertIn("PAPER TRADE CLOSED", kwargs['text'])
        self.assertIn("TEST", kwargs['text'])
        self.assertIn("+10.00%", kwargs['text'])

    async def test_exit_position_silences_when_disabled(self):
        # Setup mocks
        app = MagicMock()
        app.bot.send_message = AsyncMock()
        
        user_manager = MagicMock()
        user_manager.get_user_prefs.return_value = {"trade_notifications_enabled": False}
        
        # Execute
        await self.pm.exit_position(
            self.chat_id, 
            "test_mint_discovery", 
            "Test Reason", 
            app, 
            user_manager, 
            exit_roi=10.0
        )
        
        # Verify
        app.bot.send_message.assert_not_called()

if __name__ == '__main__':
    unittest.main()
