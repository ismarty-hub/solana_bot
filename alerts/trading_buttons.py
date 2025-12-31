# ... (imports)
import hashlib

# ... (logger)

def get_short_key(full_key: str) -> str:
    """Generate a short stable hash for a position key."""
    return hashlib.sha256(full_key.encode()).hexdigest()[:12]

def find_key_by_hash(portfolio: dict, short_key: str) -> str:
    """Find the full key from the portfolio that matches the short hash."""
    if not portfolio or "positions" not in portfolio:
        return None
    
    for key in portfolio["positions"].keys():
        if get_short_key(key) == short_key:
            return key
    return None

# ... (send_pnl_page)
# ... inside `for pos in page_positions`:
    # ...
    # Sell buttons for each position
    for pos in page_positions:
        key = f"{pos.get('mint', '')}_{pos.get('signal_type', '')}"
        short_key = get_short_key(key)
        symbol = pos.get('symbol', 'N/A')
        # SC = Sell Confirm
        keyboard.append([InlineKeyboardButton(f"🔴 Sell {symbol}", callback_data=f"sc:{short_key}")])

# ... (send_portfolio_page)
# ... inside `for key in keys_page`:
    for key in keys_page:
        pos = active_positions[key]
        short_key = get_short_key(key)
        symbol = pos.get('symbol', 'N/A')
        keyboard.append([InlineKeyboardButton(f"🔴 Sell {symbol}", callback_data=f"sc:{short_key}")])

# ... 
async def handle_sell_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       user_manager, portfolio_manager):
    """Handle sell confirmation for a single position."""
    query = update.callback_query
    chat_id = str(query.from_user.id)
    
    try:
        short_key = query.data.split(":")[1]
    except IndexError:
        await query.answer("❌ Invalid position key")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    full_key = find_key_by_hash(portfolio, short_key)
    
    if not full_key:
        await query.answer("❌ Position not found or closed.")
        return
    
    position = portfolio["positions"].get(full_key)
    symbol = position.get("symbol", "N/A")
    
    # Ask for confirmation
    confirm_keyboard = [
        [
            # SX = Sell Execute
            InlineKeyboardButton("✅ Confirm Sell", callback_data=f"sx:{short_key}"),
            InlineKeyboardButton("❌ Cancel", callback_data="sell_cancel")
        ]
    ]
    
    await query.edit_message_text(
        f"⚠️ <b>Confirm Sell</b>\n\n"
        f"Are you sure you want to sell <b>{symbol}</b>?\n\n"
        f"This action cannot be undone.",
        reply_markup=InlineKeyboardMarkup(confirm_keyboard),
        parse_mode="HTML"
    )
    await query.answer()


async def handle_sell_execute_callback(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       user_manager, portfolio_manager):
    """Execute the actual sell."""
    query = update.callback_query
    chat_id = str(query.from_user.id)
    
    try:
        short_key = query.data.split(":")[1]
    except IndexError:
        await query.answer("❌ Invalid position key")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    full_key = find_key_by_hash(portfolio, short_key)
    
    if not full_key:
        await query.answer("❌ Position not found or closed.")
        return
    
    position = portfolio["positions"].get(full_key)
    
    try:
        # Get current ROI
        active_tracking = await portfolio_manager.download_active_tracking()
        analytics_key = f"{position.get('mint', '')}_{position.get('signal_type', '')}"
        data = active_tracking.get(analytics_key, {})
        
        current_roi = float(data.get("current_roi", 0))
        if current_roi == 0:
            # Fallback
            curr_price = await portfolio_manager.fetch_current_price_fallback(position.get("mint", ""))
            if curr_price > 0:
                current_roi = ((curr_price - position.get("entry_price", 0)) / position.get("entry_price", 1)) * 100
        
        # Execute sell
        await portfolio_manager.exit_position(
            chat_id, full_key,
            "Button Close 🔴",
            context.application,
            exit_roi=current_roi
        )
        
        symbol = position.get("symbol", "N/A")
        await query.edit_message_text(
            f"✅ <b>Position Closed</b>\n\n"
            f"{symbol} has been sold.\n"
            f"Final ROI: {current_roi:+.2f}%",
            parse_mode="HTML"
        )
        await query.answer("✅ Position closed successfully!")
        
    except Exception as e:
        logger.exception(f"Error closing position for {chat_id}: {e}")
        await query.answer(f"❌ Error: {str(e)}")


async def send_pnl_page(message, chat_id: str, portfolio: dict, pnl_data: dict, page: int = 0):
    """
    Send a paginated PnL message with sell buttons.
    
    Args:
        message: Telegram message object
        chat_id: User's chat ID
        portfolio: Portfolio data
        pnl_data: PnL calculation results
        page: Page number (0-indexed)
    """
    positions_detail = pnl_data.get("positions_detail", [])
    total_unrealized_usd = pnl_data.get("total_unrealized_usd", 0)
    total_unrealized_pct = pnl_data.get("total_unrealized_pct", 0)
    total_cost_basis = pnl_data.get("total_cost_basis", 0)
    position_count = pnl_data.get("position_count", 0)
    
    capital = portfolio.get('capital_usd', 0)
    total_value = capital + total_cost_basis + total_unrealized_usd
    
    # Pagination logic
    total_pages = max(1, (len(positions_detail) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_positions = positions_detail[start_idx:end_idx]
    
    pnl_emoji = "🟢" if total_unrealized_usd > 0 else "🔴" if total_unrealized_usd < 0 else "⚪"
    
    # Build message
    msg = (
        f"📊 <b>Unrealized P/L Report</b>\n\n"
        f"<b>💰 Portfolio Value:</b>\n"
        f"• Available Capital: <b>${capital:,.2f}</b>\n"
        f"• Invested (Cost Basis): <b>${total_cost_basis:,.2f}</b>\n"
        f"• Total Value: <b>${total_value:,.2f}</b>\n\n"
        f"<b>{pnl_emoji} Unrealized P/L:</b>\n"
        f"• USD: <b>${total_unrealized_usd:+,.2f}</b>\n"
        f"• Percentage: <b>{total_unrealized_pct:+.2f}%</b>\n\n"
        f"<b>📈 Open Positions ({position_count}) - Page {page + 1}/{total_pages}:</b>\n"
    )
    
    for i, pos in enumerate(page_positions, start=start_idx + 1):
        pos_emoji = "🟢" if pos.get("unrealized_pnl_usd", 0) > 0 else "🔴"
        
        # Get TP and SL from portfolio positions
        mint = pos.get('mint')
        signal_type = pos.get('signal_type')
        position_key = f"{mint}_{signal_type}"
        portfolio_pos = portfolio.get('positions', {}).get(position_key, {})
        
        tp = portfolio_pos.get('tp_used')
        sl = portfolio_pos.get('sl_used')
        tp_display = f"+{float(tp):.0f}%" if tp else "N/A"
        sl_display = f"{float(sl):.0f}%" if sl else "N/A"
        
        msg += (
            f"{i}. {pos_emoji} <b>{pos.get('symbol', 'N/A')}</b>\n"
            f"   Price: ${pos.get('current_price', 0):.8f}\n"
            f"   P/L: ${pos.get('unrealized_pnl_usd', 0):+,.2f} ({pos.get('unrealized_pnl_pct', 0):+.2f}%)\n"
            f"   TP: {tp_display} | SL: {sl_display}\n"
        )
    
    # Build keyboard
    keyboard = []
    
    # Navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⏪ Back", callback_data=f"pnl_page:{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Next ⏩", callback_data=f"pnl_page:{page + 1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Sell All button (always on a new row)
    keyboard.append([InlineKeyboardButton("💥 Sell All", callback_data="sell_all_confirm")])
    
    # Sell buttons for each position
    for pos in page_positions:
        key = f"{pos.get('mint', '')}_{pos.get('signal_type', '')}"
        short_key = get_short_key(key)
        symbol = pos.get('symbol', 'N/A')
        keyboard.append([InlineKeyboardButton(f"🔴 Sell {symbol}", callback_data=f"sc:{short_key}")])
    
    markup = InlineKeyboardMarkup(keyboard)
    msg += "\n<i>💡 Use /portfolio to see full position details</i>"
    
    await message.reply_html(msg, reply_markup=markup)


async def send_portfolio_page(message, chat_id: str, portfolio: dict, page: int = 0):
    """
    Send a paginated Portfolio message with sell buttons.
    
    Args:
        message: Telegram message object
        chat_id: User's chat ID
        portfolio: Portfolio data
        page: Page number (0-indexed)
    """
    capital = portfolio.get('capital_usd', 0)
    positions = portfolio.get('positions', {})
    
    # Filter active positions
    active_positions = {k: v for k, v in positions.items() if v.get('status') == 'active'}
    total_positions = len(active_positions)
    total_pages = max(1, (total_positions + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    keys_page = list(active_positions.keys())[start_idx:end_idx]
    
    # Build message
    msg = (
        f"💼 <b>Paper Trading Portfolio</b>\n\n"
        f"<b>💰 Capital Summary:</b>\n"
        f"• Available: <b>${capital:,.2f}</b>\n"
    )
    
    if not keys_page:
        msg += "\n<i>No open positions</i>"
    else:
        msg += f"\n<b>📈 Open Positions ({total_positions}) - Page {page + 1}/{total_pages}:</b>\n"
        for i, key in enumerate(keys_page, start=start_idx + 1):
            pos = active_positions[key]
            symbol = pos.get('symbol', 'N/A')
            entry = pos.get('entry_price', 0)
            invested = pos.get('investment_usd', 0)
            
            # Get TP and SL values
            tp = pos.get('tp_used')
            sl = pos.get('sl_used')
            tp_display = f"+{float(tp):.0f}%" if tp else "N/A"
            sl_display = f"{float(sl):.0f}%" if sl else "N/A"
            
            msg += (
                f"{i}. <b>{symbol}</b>\n"
                f"   Entry: ${entry:.6f} | Invested: ${invested:,.2f}\n"
                f"   TP: {tp_display} | SL: {sl_display}\n"
            )
    
    # Build keyboard
    keyboard = []
    
    # Navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⏪ Back", callback_data=f"portfolio_page:{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Next ⏩", callback_data=f"portfolio_page:{page + 1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Sell All button
    keyboard.append([InlineKeyboardButton("💥 Sell All", callback_data="sell_all_confirm")])
    
    # Sell buttons per position
    for key in keys_page:
        pos = active_positions[key]
        short_key = get_short_key(key)
        symbol = pos.get('symbol', 'N/A')
        keyboard.append([InlineKeyboardButton(f"🔴 Sell {symbol}", callback_data=f"sc:{short_key}")])
    
    markup = InlineKeyboardMarkup(keyboard)
    msg += "\n<i>💡 Use /pnl for live unrealized P/L</i>"
    
    await message.reply_html(msg, reply_markup=markup)


async def handle_pnl_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   user_manager, portfolio_manager):
    """Handle pagination for /pnl command."""
    query = update.callback_query
    chat_id = str(query.from_user.id)
    
    # Extract page number
    try:
        page = int(query.data.split(":")[1])
    except (IndexError, ValueError):
        await query.answer("❌ Invalid page number")
        return
    
    prefs = user_manager.get_user_prefs(chat_id)
    if "papertrade" not in prefs.get("modes", []):
        await query.answer("❌ Paper trading is not enabled.")
        return
    
    try:
        live_prices = await portfolio_manager.update_positions_with_live_prices(chat_id)
        pnl_data = portfolio_manager.calculate_unrealized_pnl(chat_id, live_prices)
    except Exception as e:
        logger.exception(f"Error calculating PnL for {chat_id}: {e}")
        await query.answer("❌ Error fetching live prices.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    await send_pnl_page(query.message, chat_id, portfolio, pnl_data, page=page)
    await query.answer()


async def handle_portfolio_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                        user_manager, portfolio_manager):
    """Handle pagination for /portfolio command."""
    query = update.callback_query
    chat_id = str(query.from_user.id)
    
    # Extract page number
    try:
        page = int(query.data.split(":")[1])
    except (IndexError, ValueError):
        await query.answer("❌ Invalid page number")
        return
    
    prefs = user_manager.get_user_prefs(chat_id)
    if "papertrade" not in prefs.get("modes", []):
        await query.answer("❌ Paper trading is not enabled.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    await send_portfolio_page(query.message, chat_id, portfolio, page=page)
    await query.answer()


async def handle_sell_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       user_manager, portfolio_manager):
    """Handle sell confirmation for a single position."""
    query = update.callback_query
    chat_id = str(query.from_user.id)
    
    try:
        short_key = query.data.split(":")[1]
    except IndexError:
        await query.answer("❌ Invalid position key")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    full_key = find_key_by_hash(portfolio, short_key)
    
    if not full_key:
        await query.answer("❌ Position not found or closed.")
        return
    
    position = portfolio["positions"].get(full_key)
    # No need to check 'if not position' again as find_key_by_hash validates keys exist in portfolio
    
    symbol = position.get("symbol", "N/A")
    
    # Ask for confirmation
    confirm_keyboard = [
        [
            InlineKeyboardButton("✅ Confirm Sell", callback_data=f"sx:{short_key}"),
            InlineKeyboardButton("❌ Cancel", callback_data="sell_cancel")
        ]
    ]
    
    await query.edit_message_text(
        f"⚠️ <b>Confirm Sell</b>\n\n"
        f"Are you sure you want to sell <b>{symbol}</b>?\n\n"
        f"This action cannot be undone.",
        reply_markup=InlineKeyboardMarkup(confirm_keyboard),
        parse_mode="HTML"
    )
    await query.answer()


async def handle_sell_execute_callback(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       user_manager, portfolio_manager):
    """Execute the actual sell."""
    query = update.callback_query
    chat_id = str(query.from_user.id)
    
    try:
        short_key = query.data.split(":")[1]
    except IndexError:
        await query.answer("❌ Invalid position key")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    full_key = find_key_by_hash(portfolio, short_key)

    if not full_key:
        await query.answer("❌ Position not found or closed.")
        return
    
    position = portfolio["positions"].get(full_key)
    
    try:
        # Get current ROI
        active_tracking = await portfolio_manager.download_active_tracking()
        analytics_key = f"{position.get('mint', '')}_{position.get('signal_type', '')}"
        data = active_tracking.get(analytics_key, {})
        
        current_roi = float(data.get("current_roi", 0))
        if current_roi == 0:
            # Fallback
            curr_price = await portfolio_manager.fetch_current_price_fallback(position.get("mint", ""))
            if curr_price > 0:
                current_roi = ((curr_price - position.get("entry_price", 0)) / position.get("entry_price", 1)) * 100
        
        # Execute sell
        await portfolio_manager.exit_position(
            chat_id, full_key,
            "Button Close 🔴",
            context.application,
            exit_roi=current_roi
        )
        
        symbol = position.get("symbol", "N/A")
        await query.edit_message_text(
            f"✅ <b>Position Closed</b>\n\n"
            f"{symbol} has been sold.\n"
            f"Final ROI: {current_roi:+.2f}%",
            parse_mode="HTML"
        )
        await query.answer("✅ Position closed successfully!")
        
    except Exception as e:
        logger.exception(f"Error closing position for {chat_id}: {e}")
        await query.answer(f"❌ Error: {str(e)}")


async def handle_sell_all_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                           user_manager, portfolio_manager):
    """Ask for confirmation to sell all positions."""
    query = update.callback_query
    chat_id = str(query.from_user.id)
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    if not portfolio or not portfolio.get("positions"):
        await query.answer("❌ No open positions.")
        return
    
    positions = portfolio["positions"]
    count = len([p for p in positions.values() if p.get("status") == "active"])
    
    if count == 0:
        await query.answer("❌ No active positions to close.")
        return
    
    confirm_keyboard = [
        [
            InlineKeyboardButton("✅ Confirm Sell All", callback_data="sell_all_execute"),
            InlineKeyboardButton("❌ Cancel", callback_data="sell_cancel")
        ]
    ]
    
    msg = f"⚠️ <b>Close All Positions?</b>\n\n"
    msg += f"You are about to close <b>{count}</b> position(s).\n\n"
    msg += "This action cannot be undone."
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(confirm_keyboard),
        parse_mode="HTML"
    )
    await query.answer()


async def handle_sell_all_execute_callback(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                           user_manager, portfolio_manager):
    """Execute closing all positions."""
    query = update.callback_query
    chat_id = str(query.from_user.id)
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    if not portfolio or not portfolio.get("positions"):
        await query.answer("❌ No open positions.")
        return
    
    try:
        positions = portfolio["positions"]
        active_keys = [k for k, v in positions.items() if v.get("status") == "active"]
        
        if not active_keys:
            await query.answer("❌ No active positions to close.")
            return
        
        # Get active tracking for ROI calculation
        active_tracking = await portfolio_manager.download_active_tracking()
        
        closed_count = 0
        for position_key in active_keys:
            position = positions[position_key]
            
            # Get current ROI
            analytics_key = f"{position.get('mint', '')}_{position.get('signal_type', '')}"
            data = active_tracking.get(analytics_key, {})
            
            current_roi = float(data.get("current_roi", 0))
            if current_roi == 0:
                # Fallback
                curr_price = await portfolio_manager.fetch_current_price_fallback(position.get("mint", ""))
                if curr_price > 0:
                    current_roi = ((curr_price - position.get("entry_price", 0)) / position.get("entry_price", 1)) * 100
            
            try:
                await portfolio_manager.exit_position(
                    chat_id, position_key,
                    "Button Close All 💥",
                    context.application,
                    exit_roi=current_roi
                )
                closed_count += 1
            except Exception as e:
                logger.warning(f"Failed to close position {position_key}: {e}")
        
        await query.edit_message_text(
            f"✅ <b>All Positions Closed</b>\n\n"
            f"Successfully closed <b>{closed_count}</b> position(s).",
            parse_mode="HTML"
        )
        await query.answer("✅ All positions closed!")
        
    except Exception as e:
        logger.exception(f"Error closing all positions for {chat_id}: {e}")
        await query.answer(f"❌ Error: {str(e)}")


async def handle_sell_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle sell cancellation."""
    query = update.callback_query
    
    await query.edit_message_text(
        "❌ <b>Sell Cancelled</b>\n\n"
        "No positions were closed.",
        parse_mode="HTML"
    )
    await query.answer("Cancelled")
