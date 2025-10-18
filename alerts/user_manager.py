#!/usr/bin/env python3
"""
alerts/user_manager.py - User management and preferences
"""

import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List

from shared.file_io import safe_load, safe_save
from config import ALL_GRADES, ADMIN_USER_ID, USE_SUPABASE


class UserManager:
    """Manages user preferences, stats, and subscriptions."""

    def __init__(self, prefs_file: Path, stats_file: Path):
        self.prefs_file = prefs_file
        self.stats_file = stats_file

    @staticmethod
    def now_iso():
        """Return current UTC time in ISO format."""
        return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    def get_user_prefs(self, chat_id: str) -> Dict[str, Any]:
        """Get user preferences, creating default entry if not found."""
        prefs = safe_load(self.prefs_file, {})
        user = prefs.get(chat_id)

        if user:
            # Ensure 'modes' key exists for backward compatibility, defaulting to both
            if "modes" not in user or not user["modes"]:
                user["modes"] = ["alerts", "papertrade"] if user.get("active") else ["alerts"]
                self.update_user_prefs(chat_id, {"modes": user["modes"]})
            return user

        # Create default user entry
        prefs[chat_id] = {
            "grades": [],
            "created_at": self.now_iso(),
            "updated_at": self.now_iso(),
            "active": False,
            "subscribed": None,
            "total_alerts_received": 0,
            "last_alert_at": None,
            "expires_at": None,
            "modes": ["alerts"]  # Default mode for new users
        }
        safe_save(self.prefs_file, prefs)
        return prefs[chat_id]

    def update_user_prefs(self, chat_id: str, updates: Dict[str, Any]) -> bool:
        """Update user preferences."""
        try:
            prefs = safe_load(self.prefs_file, {})

            if chat_id not in prefs:
                prefs[chat_id] = {
                    "grades": ALL_GRADES.copy(),
                    "created_at": self.now_iso(),
                    "active": True,
                    "total_alerts_received": 0,
                    "modes": ["alerts"]
                }

            prefs[chat_id].update(updates)
            prefs[chat_id]["updated_at"] = self.now_iso()
            safe_save(self.prefs_file, prefs)
            return True

        except Exception as e:
            logging.exception(f"Failed to update user prefs for {chat_id}: {e}")
            return False

    def set_modes(self, chat_id: str, modes: List[str]) -> bool:
        """
        Sets a user's active modes (e.g., ['alerts', 'papertrade']).
        Ensures at least one mode is active, defaulting to 'alerts'.
        """
        valid_modes = {"alerts", "papertrade"}
        # Ensure modes are valid and there are no duplicates
        cleaned_modes = sorted(list(set(m for m in modes if m in valid_modes)))
        
        # If the list is empty after cleaning (e.g., invalid input), default to alerts
        if not cleaned_modes:
            cleaned_modes = ["alerts"]
            
        return self.update_user_prefs(chat_id, {"modes": cleaned_modes})

    def enable_papertrade_mode(self, chat_id: str) -> bool:
        """Adds 'papertrade' to a user's modes without removing others."""
        user_prefs = self.get_user_prefs(chat_id)
        modes = set(user_prefs.get("modes", ["alerts"]))
        modes.add("papertrade")
        return self.set_modes(chat_id, list(modes))

    def disable_papertrade_mode(self, chat_id: str) -> bool:
        """Removes 'papertrade' from a user's modes, ensuring 'alerts' remains if it's the only one."""
        user_prefs = self.get_user_prefs(chat_id)
        modes = set(user_prefs.get("modes", ["alerts"]))
        modes.discard("papertrade")
        return self.set_modes(chat_id, list(modes))

    def get_trading_users(self) -> List[str]:
        """Get a list of chat_ids for users with papertrade mode enabled."""
        prefs = safe_load(self.prefs_file, {})
        trading_users = []
        for chat_id, user in prefs.items():
            if user.get("active") and "papertrade" in user.get("modes", []):
                trading_users.append(chat_id)
        return trading_users
        
    def get_alerting_users(self) -> Dict[str, Any]:
        """Get a dictionary of users with alerts mode enabled."""
        prefs = safe_load(self.prefs_file, {})
        alerting_users = {}
        for chat_id, user in prefs.items():
            if user.get("active") and "alerts" in user.get("modes", []):
                alerting_users[chat_id] = user
        return alerting_users

    def deactivate_user(self, chat_id: str) -> bool:
        """Deactivate a user and clear their modes."""
        return self.update_user_prefs(chat_id, {
            "active": False,
            "deactivated_at": self.now_iso(),
            "modes": [] # Clear modes on stop
        })

    def activate_user(self, chat_id: str) -> bool:
        """Activate a user."""
        return self.update_user_prefs(chat_id, {
            "active": True,
            "reactivated_at": self.now_iso()
        })

    def get_active_users(self) -> Dict[str, Dict[str, Any]]:
        """Get all active users."""
        prefs = safe_load(self.prefs_file, {})
        return {k: v for k, v in prefs.items() if v.get("active", True)}

    def get_user_stats(self, chat_id: str) -> Dict[str, Any]:
        """Get user statistics."""
        stats = safe_load(self.stats_file, {})
        return stats.get(chat_id, {
            "alerts_received": 0,
            "last_alert_at": None,
            "joined_at": None,
            "grade_breakdown": {g: 0 for g in ALL_GRADES}
        })

    def update_user_stats(self, chat_id: str, grade: str = None):
        """Update user statistics after sending an alert."""
        try:
            stats = safe_load(self.stats_file, {})

            if chat_id not in stats:
                stats[chat_id] = {
                    "alerts_received": 0,
                    "last_alert_at": None,
                    "joined_at": self.now_iso(),
                    "grade_breakdown": {g: 0 for g in ALL_GRADES}
                }

            stats[chat_id]["alerts_received"] += 1
            stats[chat_id]["last_alert_at"] = self.now_iso()

            if grade and grade in stats[chat_id]["grade_breakdown"]:
                stats[chat_id]["grade_breakdown"][grade] += 1

            safe_save(self.stats_file, stats)

        except Exception as e:
            logging.exception(f"Failed to update stats for {chat_id}: {e}")

    def get_all_stats(self) -> Dict[str, Any]:
        """Get platform-wide statistics."""
        prefs = safe_load(self.prefs_file, {})
        stats = safe_load(self.stats_file, {})

        total_users = len(prefs)
        active_users = len([u for u in prefs.values() if u.get("active", True)])
        total_alerts = sum(s.get("alerts_received", 0) for s in stats.values())

        grade_totals = {g: 0 for g in ALL_GRADES}
        for user_stats in stats.values():
            for grade, count in user_stats.get("grade_breakdown", {}).items():
                if grade in grade_totals:
                    grade_totals[grade] += count

        return {
            "total_users": total_users,
            "active_users": active_users,
            "total_alerts_sent": total_alerts,
            "grade_breakdown": grade_totals,
            "generated_at": self.now_iso()
        }

    def mark_notified(self, chat_id: str):
        """Mark that an expired user has been notified this month."""
        prefs = safe_load(self.prefs_file, {})
        if chat_id in prefs:
            prefs[chat_id]["last_notified"] = self.now_iso()
            safe_save(self.prefs_file, prefs)

            if USE_SUPABASE:
                # ✅ --- FIX: Call the new, correct upload function ---
                from alerts.monitoring import upload_all_bot_data_to_supabase
                upload_all_bot_data_to_supabase()
                # ✅ --- END FIX ---

    def add_user_with_expiry(self, chat_id: str, days_valid: int) -> str:
        """Add or update a user with subscription expiry."""
        try:
            chat_id = str(chat_id)
            prefs = safe_load(self.prefs_file, {})
            now = self.now_iso()
            expiry_date = (datetime.utcnow() + timedelta(days=days_valid)).replace(
                microsecond=0
            ).isoformat() + "Z"

            if chat_id not in prefs:
                prefs[chat_id] = {
                    "grades": ALL_GRADES.copy(),
                    "created_at": now,
                    "total_alerts_received": 0,
                    "modes": ["alerts", "papertrade"] # Default to both for new subscribers
                }

            # Update user data
            prefs[chat_id].update({
                "updated_at": now,
                "expires_at": expiry_date,
                "active": True,
                "subscribed": True,
                "modes": ["alerts", "papertrade"]
            })

            safe_save(self.prefs_file, prefs)
            logging.info(f"✅ Saved user {chat_id} with subscribed=True, expires_at={expiry_date}")

            verify_prefs = safe_load(self.prefs_file, {})
            verify_user = verify_prefs.get(chat_id, {})
            logging.info(f"🔍 Verification - User {chat_id}: subscribed={verify_user.get('subscribed')}, active={verify_user.get('active')}")

            if USE_SUPABASE:
                # ✅ --- FIX: Call the new, correct upload function ---
                from alerts.monitoring import upload_all_bot_data_to_supabase
                upload_all_bot_data_to_supabase()
                # ✅ --- END FIX ---

            return expiry_date

        except Exception as e:
            logging.exception(f"❌ Error in add_user_with_expiry for {chat_id}: {e}")
            raise

    def is_subscription_expired(self, chat_id: str) -> bool:
        """Check if a user's subscription has expired (admin never expires)."""
        if ADMIN_USER_ID and str(chat_id) == ADMIN_USER_ID:
            return False

        prefs = safe_load(self.prefs_file, {})
        user = prefs.get(chat_id)

        if not user:
            return True

        expires_at = user.get("expires_at")
        if not expires_at:
            return False

        try:
            expiry_date = datetime.fromisoformat(expires_at.rstrip("Z"))
            return datetime.utcnow() > expiry_date
        except Exception as e:
            logging.warning(f"⚠️ Could not parse expiry for {chat_id}: {e}")
            return False

    def is_subscribed(self, chat_id: str) -> bool:
        """Check if a user has a valid subscription."""
        if ADMIN_USER_ID and str(chat_id) == ADMIN_USER_ID:
            return True

        prefs = safe_load(self.prefs_file, {})
        user = prefs.get(str(chat_id))

        if not user:
            logging.debug(f"User {chat_id} not found in preferences")
            return False

        if user.get("subscribed") is None:
            logging.debug(f"User {chat_id} subscription status not set")
            return False

        if not user.get("subscribed", False):
            logging.debug(f"User {chat_id} not subscribed")
            return False

        if self.is_subscription_expired(str(chat_id)):
            logging.debug(f"User {chat_id} subscription expired")
            return False

        logging.debug(f"User {chat_id} subscription valid")
        return True