# 🎯 Button-Based Menu Navigation System

## Overview

The bot now features a comprehensive hierarchical menu system that provides an intuitive button-based interface for all features while maintaining backward compatibility with text commands.

**Key Features:**
- ✅ Logical grouping of related features into categories
- ✅ Hierarchical navigation with back buttons
- ✅ One-click access to all bot features
- ✅ Status indicators showing active modes
- ✅ Custom input support for amounts, take profit, predictions
- ✅ Full backward compatibility with all `/commands`

## Menu Structure

```
📱 MAIN MENU
├─ 🔔 ALERTS (with status indicator)
│  ├─ 🎯 Set Alert Grades (CRITICAL/HIGH/MEDIUM/LOW)
│  ├─ 📋 View Current Settings
│  └─ 🌟 Alpha Alerts (Subscribe/Unsubscribe)
│
├─ 📈 PAPER TRADING (with status indicator)
│  ├─ ▶️ Enable Trading (if disabled)
│  │  └─ Select Initial Capital: $100/$500/$1000/$5000/Custom
│  ├─ 💼 View Portfolio (if enabled)
│  ├─ 📊 View P&L (if enabled)
│  ├─ 📜 Trade History (if enabled)
│  ├─ 📈 Performance Stats (if enabled)
│  ├─ 👀 Watchlist (if enabled)
│  └─ 💰 Reset Capital (if enabled)
│
├─ 🤖 ML PREDICTIONS
│  ├─ 🎯 Single Token (prompts for token input)
│  └─ 📊 Batch Prediction (prompts for comma-separated tokens)
│
├─ ⚙️ SETTINGS
│  ├─ 🔄 Mode Selection (Alerts Only / Trading Only / Both)
│  ├─ 🎯 Take Profit Settings
│  │  ├─ Discovery Signals TP
│  │  ├─ Alpha Signals TP
│  │  └─ View Current TP
│  └─ 👤 View My Settings
│
└─ ℹ️ HELP & INFO
   ├─ 📖 Getting Started
   ├─ 🔔 About Alerts
   ├─ 📈 About Trading
   └─ 🤖 About ML
```

## Callback Data Patterns

### Menu Navigation
```
menu_main              - Show main menu
menu_alerts            - Show alerts submenu
menu_trading           - Show trading submenu
menu_ml                - Show ML predictions menu
menu_settings          - Show settings submenu
menu_help              - Show help menu
```

### Mode Selection
```
mode_alerts_set        - Set mode to alerts only
mode_papertrade_set    - Set mode to paper trading only
mode_both_set          - Set mode to both alerts and trading
settings_mode          - Show mode selection menu
```

### Alerts Configuration
```
setalerts_menu         - Show alert grades menu
grade_critical         - Toggle CRITICAL grade
grade_high             - Toggle HIGH grade
grade_medium           - Toggle MEDIUM grade
grade_low              - Toggle LOW grade
grades_done            - Finalize grade selection
myalerts_direct        - Show current alert settings
```

### Alpha Alerts
```
alpha_menu             - Show alpha alerts submenu
alpha_subscribe_menu   - Subscribe to alpha alerts
alpha_unsubscribe_menu - Unsubscribe from alpha alerts
```

### Paper Trading
```
menu_trading           - Show trading menu
enable_trading         - Show enable trading submenu
init_capital:{amount}  - Initialize with preset amount
custom_capital         - Prompt for custom amount (awaits text input)
portfolio_direct       - Show portfolio
pnl_direct             - Show P&L
history_direct         - Show trade history
performance_direct     - Show performance stats
watchlist_direct       - Show watchlist
resetcapital_menu      - Show reset capital menu
reset_capital:{amount} - Reset to preset amount
reset_capital_custom   - Prompt for custom reset amount (awaits text input)
```

### ML Predictions
```
predict_single         - Single token prediction (awaits text input)
predict_batch_menu     - Batch prediction (awaits text input)
```

### Take Profit Settings
```
settings_tp            - Show TP settings menu
tp_discovery_menu      - Set discovery TP (awaits text input)
tp_alpha_menu          - Set alpha TP (awaits text input)
tp_view                - View current TP settings
mysettings_direct      - View all settings
```

### Help Topics
```
help_getting_started   - Getting started guide
help_alerts            - Alert system explanation
help_trading           - Paper trading explanation
help_ml                - ML prediction explanation
```

## User Experience Flow

### First-Time User
1. User sends `/start`
2. Bot shows main menu with 5 sections
3. User clicks "⚙️ Settings"
4. User selects mode (Alerts/Trading/Both)
5. User returns to main menu
6. User explores relevant sections

### Alert Configuration Flow
1. User clicks "🔔 Alerts"
2. Shows current settings and grade status
3. User clicks "🎯 Set Alert Grades"
4. Can toggle each grade on/off
5. Clicks "🔄 Done Selecting" to confirm
6. Returns to alerts submenu

### Trading Setup Flow
1. User clicks "📈 Paper Trading"
2. Shows trading is disabled
3. User clicks "▶️ Enable Trading"
4. Selects preset amount or custom
5. Bot initializes portfolio with capital
6. Returns to trading menu (now shows portfolio options)

### Mode Switching Flow
1. User clicks "⚙️ Settings"
2. Clicks "🔄 Mode Selection"
3. Selects new mode (auto-saves)
4. Returns to settings menu
5. Main menu now shows updated status indicators

## Implementation Details

### New Files

**alerts/menu_navigation.py** (450 lines)
- Contains all menu display functions
- Uses emoji-rich formatting for clarity
- Dynamically shows/hides options based on status
- Organized by menu sections

**alerts/menu_handler.py** (380 lines)
- Central router for all menu callbacks
- Integrates with existing command functions
- Handles context.user_data for stateful inputs
- Manages custom input flows

**alerts/message_handler.py** (150 lines)
- Processes text input for custom values
- Handles: custom capital, TP settings, predictions
- Sets appropriate context flags for routing
- Validates input before processing

### Modified Files

**alerts/commands.py**
- Updated `start_cmd()` to show main menu
- Updated `button_handler()` to route menu callbacks
- All command functions remain unchanged
- Full backward compatibility maintained

**bot.py**
- Registered text message handler
- Imports new menu handler
- Maintains all existing handlers

### Context User Data Flags
```python
context.user_data['awaiting_capital']      # Waiting for custom capital input
context.user_data['resetting_capital']     # Flag to differentiate reset vs init
context.user_data['awaiting_tp_discovery'] # Waiting for discovery TP input
context.user_data['awaiting_tp_alpha']     # Waiting for alpha TP input
context.user_data['awaiting_predict']      # Waiting for single token predict
context.user_data['awaiting_predict_batch'] # Waiting for batch predict
```

## Status Indicators

### Main Menu
```
🔔 Alerts ✅          (if alerts mode enabled)
🔔 Alerts ⭕          (if alerts mode disabled)

📈 Paper Trading ✅   (if papertrade mode enabled)
📈 Paper Trading ⭕   (if papertrade mode disabled)
```

### Alert Grades Menu
```
🔴 CRITICAL           (can be toggled)
🟠 HIGH               (can be toggled)
🟡 MEDIUM             (can be toggled)
🟢 LOW                (can be toggled)
```

### Trading Menu
```
Status: ✅ Enabled    (if trading is active)
Status: ❌ Disabled   (if trading is not active)

Capital: $1,234.56    (shows current capital)
Open Positions: 5     (shows active trades)
```

## Command Integration

All original commands still work:
```
/start              - Show menu (was old mode selection)
/help               - Help text (unchanged)
/myalerts           - View settings (accessible via menu)
/setalerts          - Set grades (accessible via menu)
/portfolio          - View portfolio (accessible via menu)
/pnl                - View P&L (accessible via menu)
/history            - View history (accessible via menu)
/performance        - Performance stats (accessible via menu)
/papertrade         - Enable trading (accessible via menu)
/resetcapital       - Reset capital (accessible via menu)
/predict            - Single prediction (accessible via menu)
/predict_batch      - Batch prediction (accessible via menu)
/set_tp             - Set take profit (accessible via menu)
/alpha_subscribe    - Alpha alerts on (accessible via menu)
/alpha_unsubscribe  - Alpha alerts off (accessible via menu)
...and all others
```

## Accessibility

### Advantages of Button Interface
- ✅ **Discovery**: New users can explore features without knowing commands
- ✅ **Efficiency**: One click vs. typing commands with arguments
- ✅ **Feedback**: Status indicators show active modes at a glance
- ✅ **Safety**: Confirmations prevent accidental actions
- ✅ **Validation**: Input is validated before processing
- ✅ **Guidance**: Help menu explains features

### Why Commands Still Matter
- ✅ **Power Users**: Experienced users can still type fast
- ✅ **Scripting**: Commands work in group chats, bots, scripts
- ✅ **Accessibility**: Screen readers work better with text
- ✅ **Reliability**: Commands don't depend on button callbacks
- ✅ **History**: Users are familiar with command syntax
- ✅ **Batch Operations**: Can paste multiple commands

## Testing Checklist

- [ ] Main menu displays with correct status indicators
- [ ] Alerts submenu shows grade selection
- [ ] Trading submenu shows options based on enabled/disabled status
- [ ] Back buttons return to parent menu
- [ ] Mode selection updates immediately
- [ ] Custom capital input works and initializes portfolio
- [ ] Reset capital works correctly
- [ ] TP settings accept text input (median/mean/number)
- [ ] ML prediction prompts accept tokens
- [ ] All original `/commands` still work
- [ ] Button callbacks don't conflict with commands
- [ ] Help topics display correctly
- [ ] Grade selection toggles work (UI updates)
- [ ] Alpha alerts toggle works
- [ ] Custom inputs validate and show errors

## Error Handling

Each input type validates before processing:
- **Capital amounts**: Must be positive number
- **TP values**: Must be "median", "mean", or positive number
- **Token input**: Passed to command for validation
- **Grade selection**: Buttons ensure only valid grades

All errors show user-friendly messages and allow retry.

## Future Enhancements

- 📊 Add charts/sparklines for portfolio quick view
- 🔔 Menu item badges showing unread alerts count
- 📌 Save recent tokens for quick prediction
- ⚡ Quick actions (e.g., "Close All" from main menu)
- 🎨 Custom theme selection in settings
- 🔐 Multi-level confirmation for risky operations
- 📱 Mobile-optimized menu spacing
