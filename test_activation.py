import sys
import os
from pathlib import Path
import json
from unittest.mock import MagicMock

# 1. Mock environment variables before importing anything
os.environ['BOT_TOKEN'] = 'mock_token'
os.environ['ADMIN_USER_ID'] = '123,456'
os.environ['DATA_DIR'] = './data'

# 2. Mock modules that might fail due to missing dependencies
sys.modules['joblib'] = MagicMock()
sys.modules['dotenv'] = MagicMock()
sys.modules['supabase'] = MagicMock()

# 3. Mock file mapping for Supabase
# We need to make sureUserManager's functions call our mocks
sys.modules['supabase_utils'] = MagicMock()

# Add root to path
sys.path.append(os.getcwd())

# Mock shared.file_io to avoid joblib usage during tests
import shared.file_io
def mock_safe_load(path, default):
    path = Path(path)
    if not path.exists(): return default
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return default

def mock_safe_save(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)
    return True

shared.file_io.safe_load = mock_safe_load
shared.file_io.safe_save = mock_safe_save

# Now import UserManager
from alerts.user_manager import UserManager

# Use test files
TEST_PREFS = Path("data/test_user_prefs.json") 
TEST_STATS = Path("data/test_user_stats.json")
TEST_CODES = Path("data/test_activation_codes.json")

def test_activation_flow():
    # Cleanup previous tests if any
    for f in [TEST_PREFS, TEST_STATS, TEST_CODES]:
        if f.exists(): os.remove(f)
    
    with open(TEST_CODES, 'w') as f:
        json.dump({}, f)

    um = UserManager(TEST_PREFS, TEST_STATS)
    chat_id = "123456789"
    
    # Patch the constants used inside UserManager methods
    import alerts.user_manager
    alerts.user_manager.ACTIVATION_CODES_FILE = TEST_CODES
    alerts.user_manager.USE_SUPABASE = False # Disable real supabase calls for bit test
    
    print("--- Starting Activation Test ---")
    
    # 1. Check initial status
    is_sub = um.is_subscribed(chat_id)
    print(f"Initial subscription (should be False): {is_sub}")
    
    # 2. Generate code
    days = 30
    code = um.generate_activation_code(days)
    print(f"Generated code: {code} for {days} days")
    
    # 3. Verify code exists in file
    with open(TEST_CODES, 'r') as f:
        codes = json.load(f)
    if code in codes:
        print("✅ Code successfully saved to file.")
    else:
        print("❌ Code NOT found in file.")
        return

    # 4. Redeem code
    redeemed_days = um.redeem_activation_code(chat_id, code)
    if redeemed_days == days:
        print(f"✅ Successfully redeemed {redeemed_days} days.")
    else:
        print(f"❌ Redemption failed. Expected {days}, got {redeemed_days}")
        return

    # 5. Check subscription status
    is_sub_final = um.is_subscribed(chat_id)
    print(f"Final subscription (should be True): {is_sub_final}")
    
    # Reload um to ensure persistence
    um2 = UserManager(TEST_PREFS, TEST_STATS)
    is_sub_reloaded = um2.is_subscribed(chat_id)
    print(f"Reloaded subscription check: {is_sub_reloaded}")

    # 6. Verify code is removed
    with open(TEST_CODES, 'r') as f:
        codes = json.load(f)
    if code not in codes:
        print("✅ Code successfully removed after use.")
    else:
        print("❌ Code STILL EXISTS in file after use.")
    
    print("--- Test Complete ---")

if __name__ == "__main__":
    try:
        test_activation_flow()
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Test failed with error: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        for f in [TEST_PREFS, TEST_STATS, TEST_CODES]:
            if f.exists(): os.remove(f)
