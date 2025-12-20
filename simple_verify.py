
import os
import sys

# Add the project directory to sys.path
sys.path.append(os.getcwd())

from analytics_tracker import calculate_timeframe_stats

def test_analytics_filtering():
    print("Testing Analytics Filtering...")
    tokens = [
        {"mint": "T1", "ML_PASSED": True, "status": "win", "ath_roi": 100, "final_roi": 50},
        {"mint": "T2", "ML_PASSED": False, "status": "win", "ath_roi": 500, "final_roi": 250},
        {"mint": "T3", "ML_PASSED": True, "status": "loss", "ath_roi": 10, "final_roi": -90}
    ]
    
    stats = calculate_timeframe_stats(tokens)
    print(f"Stats: {stats}")
    
    # Should only count T1 and T3
    assert stats["total_tokens"] == 2
    assert stats["wins"] == 1
    assert stats["losses"] == 1
    assert stats["max_roi"] == 100
    print("✅ Analytics filtering passed!")

if __name__ == "__main__":
    try:
        test_analytics_filtering()
    except Exception as e:
        print(f"❌ Test Failed: {e}")
        import traceback
        traceback.print_exc()
