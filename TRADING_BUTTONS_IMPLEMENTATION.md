# Interactive Trading Controls Implementation

## Overview
Added comprehensive interactive trading controls to the `/pnl` and `/portfolio` commands with:
- ✅ Pagination (Next/Back buttons) - 5 positions per page
- ✅ Individual Sell buttons for each position
- ✅ Sell All button
- ✅ Confirmation step before execution (prevents accidental clicks)
- ✅ Page counter (e.g., "Page 1/6")

## Features

### 1. Pagination System
- **Positions per page**: 5 tokens (configurable in `PAGE_SIZE`)
- **Navigation buttons**: 
  - "⏪ Back" - Goes to previous page (if not on page 1)
  - "Next ⏩" - Goes to next page (if not on last page)
- **Page counter**: Shows "Page X/Y" on every message

### 2. Individual Sell Buttons
- One "🔴 Sell {Symbol}" button per position
- Clicking triggers a confirmation dialog
- Shows: "Are you sure you want to sell {SYMBOL}?"
- Buttons: "✅ Confirm Sell" and "❌ Cancel"

### 3. Sell All Button
- "💥 Sell All" button on every page
- Asks for confirmation showing number of positions to close
- Executes closing of all active positions on confirmation
- Shows success message with count of closed positions

### 4. Confirmation System
- **Two-step process**:
  1. Click "Sell {Symbol}" or "Sell All"
  2. Confirm or cancel on the next screen
- **Prevents accidental trades**
- All messages include proper emoji indicators (✅, ❌, 🔴, 💥)

## Files Modified

### 1. `alerts/trading_buttons.py` (NEW)
New module handling all trading button interactions:
- `send_pnl_page()` - Sends paginated PnL message
- `send_portfolio_page()` - Sends paginated portfolio message
- `handle_pnl_page_callback()` - Handles PnL pagination
- `handle_portfolio_page_callback()` - Handles portfolio pagination
- `handle_sell_confirm_callback()` - Shows confirmation for single position
- `handle_sell_execute_callback()` - Executes single position sale
- `handle_sell_all_confirm_callback()` - Shows confirmation for all positions
- `handle_sell_all_execute_callback()` - Executes closing all positions
- `handle_sell_cancel_callback()` - Cancels the sell operation

### 2. `alerts/commands.py`
Updated existing functions:
- `button_handler()` - Extended to route trading button callbacks
- `pnl_cmd()` - Simplified, now uses `send_pnl_page()` from trading_buttons
- `portfolio_cmd()` - Simplified, now uses `send_portfolio_page()` from trading_buttons

### 3. `bot.py`
Updated:
- `button_wrapper()` - Now passes `portfolio_manager` to button_handler

## Callback Data Structure

The following callback patterns are used:

```
pnl_page:{page_number}              # PnL pagination
portfolio_page:{page_number}        # Portfolio pagination
sell_confirm:{position_key}         # Confirm single position sell
sell_execute:{position_key}         # Execute single position sell
sell_all_confirm                    # Confirm sell all
sell_all_execute                    # Execute sell all
sell_cancel                         # Cancel any sell operation
```

## Position Key Format
```
{mint}_{signal_type}
Example: "2RV6uc3bLSEQkvd81tQSLwHipVsEDen4bgviFsPVpump_discovery"
```

## Usage Examples

### Example 1: User has 12 positions
```
/pnl

📊 Unrealized P/L Report

💰 Portfolio Value:
• Available Capital: $1,234.56
• Invested (Cost Basis): $2,000.00
• Total Value: $3,234.56

🟢 Unrealized P/L:
• USD: $+142.87
• Percentage: +7.14%

📈 Open Positions (12) - Page 1/3:
1. 🟢 TOKEN1
   Price: $0.000123
   P/L: $+50.00 (+5.00%)

2. 🔴 TOKEN2
   Price: $0.000456
   P/L: $-25.00 (-2.50%)

[⏪ Back] [Next ⏩] [💥 Sell All]
[🔴 Sell TOKEN1]
[🔴 Sell TOKEN2]
...
```

### Example 2: Click "🔴 Sell TOKEN1"
```
⚠️ Confirm Sell

Are you sure you want to sell TOKEN1?

This action cannot be undone.

[✅ Confirm Sell] [❌ Cancel]
```

### Example 3: Click "✅ Confirm Sell"
```
✅ Position Closed

TOKEN1 has been sold.
Final ROI: +5.47%
```

### Example 4: Click "💥 Sell All"
```
⚠️ Close All Positions?

You are about to close 12 position(s).

This action cannot be undone.

[✅ Confirm Sell All] [❌ Cancel]
```

### Example 5: Click "✅ Confirm Sell All"
```
✅ All Positions Closed

Successfully closed 12 position(s).
```

## Technical Details

### ROI Calculation on Sell
When closing a position, the system:
1. Checks `active_tracking.json` for current ROI from analytics
2. Falls back to calculating from current price if needed
3. Passes ROI to `portfolio_manager.exit_position()`

### Error Handling
- Invalid page numbers → Shows error message
- Position not found → Shows error message
- Failed API calls → Shows error with exception message
- Graceful fallbacks for missing data

### Performance
- **Live prices**: Fetched fresh on each `/pnl` call
- **Pagination**: Reduces message size (5 items max per page)
- **Async**: All operations are async and non-blocking
- **Caching**: Uses existing portfolio cache system

## Configuration

To change positions per page, modify in `alerts/trading_buttons.py`:
```python
PAGE_SIZE = 5  # Change this number
```

## Security Considerations

1. **User verification**: Chat ID matched from callback query
2. **Two-step confirmation**: Prevents accidental trades
3. **Portfolio validation**: Checks position exists before closing
4. **Error messages**: Safe error handling without sensitive info exposure

## Future Enhancements

- [ ] Add position details preview on hover
- [ ] Partial position close (sell 50%, 75%, etc.)
- [ ] Limit orders on close
- [ ] Position grouping by signal type
- [ ] Quick stats per position (entry time, time in trade, etc.)
