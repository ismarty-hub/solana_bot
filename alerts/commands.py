#!/usr/bin/env python3
"""
alerts/commands.py - User-facing bot commands
"""

import requests
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import ALL_GRADES
from shared.file_io import safe_load
from shared.utils import truncate_address
from alerts.formatters import format_alert_html


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Handle /start command."""
    chat_id = str(update.effective_chat.id)
    
    logging.info(f"🚀 User {chat_id} started bot")
    
    # Debug subscription status
    prefs = safe_load(user_manager.prefs_file, {})
    user_data = prefs.get(chat_id, {})
    
    logging.info(f"🔍 Debug user {chat_id}:")
    logging.info(f"  - Found in prefs: {chat_id in prefs}")
    logging.info(f"  - subscribed: {user_data.get('subscribed', False)}")
    logging.info(f"  - active: {user_data.get('active', False)}")
    logging.info(f"  - expires_at: {user_data.get('expires_at')}")
    
    is_sub = user_manager.is_subscribed(chat_id)
    is_expired = user_manager.is_subscription_expired(chat_id)
    
    logging.info(f"  - is_subscribed(): {is_sub}")
    logging.info(f"  - is_subscription_expired(): {is_expired}")
    
    if not is_sub:
        await update.message.reply_html(
            f"👋 Welcome!\n\n"
            f"❌ You are not subscribed to alerts.\n"
            f"Please contact the admin to activate your subscription.\n\n"
            f"🔍 <b>Debug info:</b>\n"
            f"• User found: {chat_id in prefs}\n"
            f"• Subscribed: {user_data.get('subscribed', False)}\n"
            f"• Active: {user_data.get('active', False)}\n"
            f"• Expired: {is_expired}"
        )
        return

    user_prefs = user_manager.get_user_prefs(chat_id)

    if not user_prefs.get("created_at"):
        user_manager.update_user_prefs(chat_id, {
            "grades": ALL_GRADES.copy(),
            "active": True,
            "created_at": user_manager.now_iso()
        })
        user_prefs = user_manager.get_user_prefs(chat_id)
    else:
        user_manager.activate_user(chat_id)

    keyboard = [
        [
            InlineKeyboardButton("🔴 CRITICAL Only", callback_data="preset_critical"),
            InlineKeyboardButton("🔥 CRITICAL + HIGH", callback_data="preset_critical_high")
        ],
        [
            InlineKeyboardButton("📊 All Grades", callback_data="preset_all"),
            InlineKeyboardButton("⚙️ Custom Setup", callback_data="custom_setup")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    welcome_msg = (
        f"👋 <b>Welcome to Token Grade Alerts!</b>\n\n"
        f"🎯 Current subscription: <b>{', '.join(user_prefs.get('grades', ALL_GRADES))}</b>\n\n"
        f"This bot monitors new Solana tokens and alerts you when they show "
        f"overlap with yesterday's winning tokens based on holder analysis.\n\n"
        f"<b>Grade Meanings:</b>\n"
        f"🔴 <b>CRITICAL</b> - Very high overlap (50%+ or strong concentration)\n"
        f"🟠 <b>HIGH</b> - Significant overlap (30%+ overlap)\n"
        f"🟡 <b>MEDIUM</b> - Notable overlap (15%+ overlap)\n"
        f"🟢 <b>LOW</b> - Some overlap (5%+ overlap)\n\n"
        f"Choose your alert preferences:"
    )

    await update.message.reply_html(welcome_msg, reply_markup=reply_markup)


async def setalerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Handle /setalerts command."""
    chat_id = str(update.effective_chat.id)
    
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You are not subscribed. Please contact the admin.")
        return
    
    args = context.args or []
    valid = set(ALL_GRADES)
    chosen = [a.upper() for a in args if a.upper() in valid]

    if not chosen:
        keyboard = [
            [InlineKeyboardButton("🔴 CRITICAL", callback_data="preset_critical"),
             InlineKeyboardButton("🔥 CRITICAL + HIGH", callback_data="preset_critical_high")],
            [InlineKeyboardButton("📊 All Grades", callback_data="preset_all")]
        ]
        await update.message.reply_html(
            "⚠️ Usage: /setalerts GRADE1 GRADE2 ...\nAvailable: CRITICAL, HIGH, MEDIUM, LOW",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    success = user_manager.update_user_prefs(chat_id, {"grades": chosen, "active": True})
    
    if success:
        await update.message.reply_html(f"✅ Alert preferences updated! You will receive: <b>{', '.join(chosen)}</b>")
    else:
        await update.message.reply_text("❌ Failed to save preferences. Please try again.")


async def myalerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Handle /myalerts command."""
    chat_id = str(update.effective_chat.id)
    
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You are not subscribed. Please contact the admin.")
        return
    
    prefs = user_manager.get_user_prefs(chat_id)
    stats = user_manager.get_user_stats(chat_id)
    
    if not prefs.get("active", False):
        await update.message.reply_text("You are not currently subscribed. Use /start to subscribe.")
        return

    total_alerts = stats.get("alerts_received", 0)
    last_alert = stats.get("last_alert_at")
    last_alert_str = "Never" if not last_alert else f"<i>{last_alert[:10]}</i>"

    breakdown_lines = []
    for grade in ALL_GRADES:
        count = stats.get("grade_breakdown", {}).get(grade, 0)
        if count > 0:
            breakdown_lines.append(f"  • {grade}: {count}")
    
    breakdown_text = "\n".join(breakdown_lines) if breakdown_lines else "  • No alerts received yet"

    msg = (
        f"📊 <b>Your Alert Settings</b>\n\n"
        f"🎯 <b>Subscribed to:</b> {', '.join(prefs.get('grades', ALL_GRADES))}\n"
        f"📈 <b>Total alerts received:</b> {total_alerts}\n"
        f"🕐 <b>Last alert:</b> {last_alert_str}\n\n"
        f"<b>Breakdown by grade:</b>\n{breakdown_text}\n\n"
        f"Use /setalerts to change your preferences."
    )
    
    await update.message.reply_html(msg)


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Handle /stop command."""
    chat_id = str(update.effective_chat.id)
    success = user_manager.deactivate_user(chat_id)
    
    if success:
        await update.message.reply_html("😔 You have been unsubscribed. Use /start to reactivate.")
    else:
        await update.message.reply_text("❌ Failed to unsubscribe. Please try again.")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    help_text = (
        "🤖 <b>Token Grade Alerts - Help</b>\n\n"
        "Commands:\n"
        "• /start - Subscribe and set preferences\n"
        "• /setalerts GRADE1 GRADE2 - Set alert grades\n"
        "• /myalerts - View your settings and stats\n"
        "• /stop - Unsubscribe (keeps your data)\n"
        "• /help - Show this help message\n\n"
        "Grades: CRITICAL, HIGH, MEDIUM, LOW\n"
    )
    await update.message.reply_html(help_text)


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager, is_admin: bool = False):
    """Handle /stats command."""
    chat_id = str(update.effective_chat.id)
    user_stats = user_manager.get_user_stats(chat_id)
    
    msg = (
        f"📊 <b>Your Statistics</b>\n\n"
        f"📬 Total alerts received: <b>{user_stats.get('alerts_received', 0)}</b>\n"
        f"📅 Member since: <i>{user_stats.get('joined_at', 'Unknown')[:10] if user_stats.get('joined_at') else 'Unknown'}</i>\n"
    )
    
    if is_admin:
        platform_stats = user_manager.get_all_stats()
        msg += (
            f"\n🏢 <b>Platform Statistics (Admin)</b>\n"
            f"• Total users: <b>{platform_stats['total_users']}</b>\n"
            f"• Active users: <b>{platform_stats['active_users']}</b>\n"
            f"• Total alerts sent: <b>{platform_stats['total_alerts_sent']}</b>\n"
        )
    
    await update.message.reply_html(msg)


async def testalert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a test alert for a known token."""
    chat_id = str(update.effective_chat.id)
    args = context.args or []
    alert_type = "CHANGE"
    grade = "CRITICAL"
    previous_grade = "HIGH" if alert_type == "CHANGE" else None

    # Parse arguments
    if args:
        if args[0].upper() in ["NEW", "CHANGE"]:
            alert_type = args[0].upper()
        if len(args) > 1 and args[1].upper() in ALL_GRADES:
            grade = args[1].upper()
        if alert_type == "CHANGE" and len(args) > 2 and args[2].upper() in ALL_GRADES:
            previous_grade = args[2].upper()

    token_id = "G8cGYUUdnwvQ8W1iMy37TMD2xpMnYS4NCh1YKQJepump"
    
    try:
        resp = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{token_id}", timeout=10)
        data = resp.json()
        pairs = data.get("pairs", []) or []
        mc = None
        fdv = None
        base = {}
        
        if pairs:
            mc = pairs[0].get("marketCap")
            fdv = pairs[0].get("fdv")
            base = pairs[0].get("baseToken", {}) or {}

        token_data = {
            "token": token_id,
            "grade": grade,
            "token_metadata": {
                "name": base.get("name", "TestToken"),
                "symbol": base.get("symbol", "TT"),
            },
            "overlap_percentage": 85.3,
            "concentration": 42.1
        }

        initial_mc = mc * 0.6 if mc else None
        initial_fdv = fdv * 0.6 if fdv else None
        first_alert = (datetime.utcnow() - timedelta(days=2)).isoformat() + "Z"

        message = format_alert_html(
            token_data,
            alert_type,
            previous_grade=previous_grade if alert_type == "CHANGE" else None,
            initial_mc=initial_mc,
            initial_fdv=initial_fdv,
            first_alert_at=first_alert
        )

        mint_val = token_data.get("token_metadata", {}).get("mint") or token_data.get("token") or ""
        truncated_val = truncate_address(mint_val)
        kb = None
        
        if mint_val:
            kb = InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(f"📋 Copy {truncated_val}", callback_data=f"copy:{mint_val}"),
                    InlineKeyboardButton("🔗 DexScreener", url=f"https://dexscreener.com/solana/{mint_val}")
                ]]
            )

        await update.message.reply_html(f"🔔 Test Alert ({alert_type})\n\n{message}", reply_markup=kb)
    
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to fetch token data: {e}")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Handle inline keyboard button callbacks."""
    query = update.callback_query
    if not query:
        return

    data = query.data or ""

    # Handle copy callbacks
    if data.startswith("copy:"):
        try:
            _, address = data.split(":", 1)
            await query.answer(text=address, show_alert=True)
        except Exception as e:
            try:
                await query.message.reply_text(data.split(":", 1)[1])
            except Exception:
                pass
        return

    # For other interactions, enforce subscription
    chat_id = str(query.from_user.id)
    if not user_manager.is_subscribed(chat_id):
        try:
            await query.answer("⛔ You are not subscribed. Please contact the admin.", show_alert=True)
        except Exception:
            pass
        return

    # Acknowledge callback
    try:
        await query.answer()
    except Exception:
        pass

    if data == "preset_critical":
        user_manager.update_user_prefs(chat_id, {"grades": ["CRITICAL"]})
        try:
            await query.edit_message_text("✅ Preferences updated: CRITICAL only.", parse_mode="HTML")
        except Exception:
            pass
    
    elif data == "preset_critical_high":
        user_manager.update_user_prefs(chat_id, {"grades": ["CRITICAL", "HIGH"]})
        try:
            await query.edit_message_text("✅ Preferences updated: CRITICAL + HIGH.", parse_mode="HTML")
        except Exception:
            pass
    
    elif data == "preset_all":
        user_manager.update_user_prefs(chat_id, {"grades": ALL_GRADES.copy()})
        try:
            await query.edit_message_text("✅ Preferences updated: ALL grades.", parse_mode="HTML")
        except Exception:
            pass
    
    elif data == "custom_setup":
        try:
            await query.edit_message_text(
                "⚙️ Custom Setup\n\nUse /setalerts GRADE1 GRADE2 ...\nAvailable: CRITICAL, HIGH, MEDIUM, LOW",
                parse_mode="HTML"
            )
        except Exception:
            pass