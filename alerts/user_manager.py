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

logger = logging.getLogger(__name__)


class UserManager:
    """Manages user preferences, stats, and subscriptions."""

    def __init__(self, prefs_file: Path, stats_file: Path):
        self.prefs_file = prefs_file
        self.stats_file = stats_file

    @staticmethod
    def now_iso():
        """Return current UTC time in ISO format (Z-suffixed)."""
        return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    def _persist_prefs(self, prefs: Dict[str, Any]):
        """Helper to save prefs with logging."""
        try:
            safe_save(self.prefs_file, prefs)
        except Exception:
            logger.exception("Failed to persist prefs")

    def _normalize_user_record(self, chat_id: str, user: Dict[str, Any], prefs: Dict[str, Any]) -> None:
        """
        Normalize legacy user records in-place.
        """
        modified = False

        # Ensure modes exists and is a list
        if "modes" not in user or not isinstance(user.get("modes"), list):
            user["modes"] = user.get("modes") or ["alerts"]
            modified = True

        # Legacy fix: if alpha was placed in modes, migrate it to the boolean flag
        if "alpha_alerts" in user.get("modes", []):
            try:
                user["modes"] = [m for m in user.get("modes", []) if m != "alpha_alerts"]
                if not user["modes"]:
                    user["modes"] = ["alerts"]
                user["alpha_alerts"] = True
                modified = True
            except Exception:
                pass

        # Ensure alpha_alerts boolean exists
        if "alpha_alerts" not in user:
            user["alpha_alerts"] = False
            modified = True

        # Ensure 'subscribed' key exists
        if "subscribed" not in user:
            user["subscribed"] = False
            modified = True

        # Ensure 'active' key exists
        if "active" not in user:
            user["active"] = user.get("active", False)
            modified = True
            
        # --- NEW: TP Preferences ---
        if "tp_preference" not in user:
            user["tp_preference"] = "median" # Default to median
            modified = True
            
        if "tp_discovery" not in user:
            user["tp_discovery"] = None
            modified = True
            
        if "tp_alpha" not in user:
            user["tp_alpha"] = None
            modified = True
            
        # Trading capital management defaults
        if "reserve_balance" not in user:
            user["reserve_balance"] = 0.0
            modified = True
            
        if "min_trade_size" not in user:
            user["min_trade_size"] = 10.0
            modified = True

        if modified:
            prefs[chat_id] = user
            self._persist_prefs(prefs)

    def get_user_prefs(self, chat_id: str) -> Dict[str, Any]:
        """Get user preferences, creating a default entry if not found."""
        prefs = safe_load(self.prefs_file, {})
        user = prefs.get(chat_id)

        if user:
            try:
                self._normalize_user_record(chat_id, user, prefs)
            except Exception:
                logger.exception(f"Error normalizing prefs for {chat_id}")
            return prefs.get(chat_id)

        # Create default user entry
        now = self.now_iso()
        prefs[chat_id] = {
            "grades": [],
            "created_at": now,
            "updated_at": now,
            "active": False,
            "subscribed": False,
            "total_alerts_received": 0,
            "last_alert_at": None,
            "expires_at": None,
            "modes": ["alerts"],
            "alpha_alerts": False,
            # New TP defaults
            "tp_preference": "median",
            "tp_discovery": None,
            "tp_alpha": None,
            # Trading capital management
            "reserve_balance": 0.0,
            "min_trade_size": 10.0
        }
        self._persist_prefs(prefs)
        return prefs[chat_id]

    def update_user_prefs(self, chat_id: str, updates: Dict[str, Any]) -> bool:
        """Update user preferences and persist safely."""
        try:
            prefs = safe_load(self.prefs_file, {})

            if chat_id not in prefs:
                # Initialize default if new
                self.get_user_prefs(chat_id)
                prefs = safe_load(self.prefs_file, {})

            # Avoid accidentally placing alpha_alerts inside modes
            if "modes" in updates and isinstance(updates.get("modes"), list):
                cleaned_modes = [m for m in updates["modes"] if m != "alpha_alerts"]
                updates["modes"] = cleaned_modes or ["alerts"]

            prefs[chat_id].update(updates)
            prefs[chat_id]["updated_at"] = self.now_iso()
            self._persist_prefs(prefs)
            return True

        except Exception as e:
            logger.exception(f"Failed to update user prefs for {chat_id}: {e}")
            return False

    def set_modes(self, chat_id: str, modes: List[str]) -> bool:
        """Sets a user's active modes."""
        valid_modes = {"alerts", "papertrade"}
        cleaned_modes = sorted(list(set(m for m in modes if m in valid_modes)))

        if not cleaned_modes:
            cleaned_modes = ["alerts"]

        return self.update_user_prefs(chat_id, {"modes": cleaned_modes})

    def get_alpha_subscribers(self) -> List[str]:
        """Get a list of chat_ids for users who are active, subscribed, and have alpha_alerts=True."""
        prefs = safe_load(self.prefs_file, {})
        alpha_subscribers: List[str] = []

        for chat_id, user in prefs.items():
            try:
                self._normalize_user_record(chat_id, user, prefs)
            except: pass

            if user.get("active", False) and user.get("alpha_alerts", False) and self.is_subscribed(chat_id):
                alpha_subscribers.append(chat_id)

        return alpha_subscribers

    def enable_papertrade_mode(self, chat_id: str) -> bool:
        """Adds 'papertrade' to a user's modes."""
        user_prefs = self.get_user_prefs(chat_id)
        modes = set(user_prefs.get("modes", ["alerts"]))
        modes.add("papertrade")
        return self.set_modes(chat_id, list(modes))

    def disable_papertrade_mode(self, chat_id: str) -> bool:
        """Removes 'papertrade' from a user's modes."""
        user_prefs = self.get_user_prefs(chat_id)
        modes = set(user_prefs.get("modes", ["alerts"]))
        modes.discard("papertrade")
        return self.set_modes(chat_id, list(modes))

    def get_trading_users(self) -> List[str]:
        """Get users with papertrade mode enabled."""
        prefs = safe_load(self.prefs_file, {})
        trading_users = []
        for chat_id, user in prefs.items():
            if user.get("active") and "papertrade" in user.get("modes", []):
                trading_users.append(chat_id)
        return trading_users

    def get_alerting_users(self) -> Dict[str, Any]:
        """Get users with alerts mode enabled."""
        prefs = safe_load(self.prefs_file, {})
        alerting_users = {}
        for chat_id, user in prefs.items():
            if user.get("active") and "alerts" in user.get("modes", []):
                alerting_users[chat_id] = user
        return alerting_users

    def deactivate_user(self, chat_id: str) -> bool:
        """Deactivate a user."""
        return self.update_user_prefs(chat_id, {
            "active": False,
            "deactivated_at": self.now_iso(),
            "modes": []
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
            logger.exception(f"Failed to update stats for {chat_id}: {e}")

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
        """Mark that an expired user has been notified."""
        prefs = safe_load(self.prefs_file, {})
        if chat_id in prefs:
            prefs[chat_id]["last_notified"] = self.now_iso()
            self._persist_prefs(prefs)

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
                    "modes": ["alerts", "papertrade"],
                    "tp_preference": "median"
                }

            prefs[chat_id].update({
                "updated_at": now,
                "expires_at": expiry_date,
                "active": True,
                "subscribed": True
            })

            self._persist_prefs(prefs)
            return expiry_date

        except Exception as e:
            logger.exception(f"❌ Error in add_user_with_expiry for {chat_id}: {e}")
            raise

    def is_subscription_expired(self, chat_id: str) -> bool:
        """Check if a user's subscription has expired."""
        if ADMIN_USER_ID and int(chat_id) in ADMIN_USER_ID:
            return False

        prefs = safe_load(self.prefs_file, {})
        user = prefs.get(str(chat_id))

        if not user:
            return True

        expires_at = user.get("expires_at")
        if not expires_at:
            return False

        try:
            expiry_date = datetime.fromisoformat(expires_at.rstrip("Z"))
            return datetime.utcnow() > expiry_date
        except Exception:
            return False

    def is_subscribed(self, chat_id: str) -> bool:
        """Check if a user has a valid subscription."""
        if ADMIN_USER_ID and int(chat_id) in ADMIN_USER_ID:
            return True

        prefs = safe_load(self.prefs_file, {})
        user = prefs.get(str(chat_id))

        if not user:
            return False

        if not user.get("subscribed"):
            return False

        return not self.is_subscription_expired(str(chat_id))