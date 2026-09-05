#!/usr/bin/env python3
"""
StockGro Referral Monitor Bot - Railway Deployment
All configuration from Environment Variables - NO HARDCODING
"""
import os
import sys
import json
import time
import sqlite3
import logging
import asyncio
import threading
from datetime import datetime
from typing import Optional, Dict, List, Tuple, Set
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from queue import Queue, Empty

# =========================================================
# 🔧 LOAD FROM ENVIRONMENT VARIABLES - NO HARDCODING
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_ID", "").split(",") if x.strip()]
FIREBASE_URL = os.getenv("FIREBASE_URL", "")
DEFAULT_TARGET = int(os.getenv("DEFAULT_TARGET", "40"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "8"))

# Validate required variables
if not BOT_TOKEN:
    print("❌ ERROR: BOT_TOKEN not set in environment variables")
    print("   Please add BOT_TOKEN in Railway Variables")
    sys.exit(1)

if not ADMIN_IDS:
    print("⚠️ WARNING: ADMIN_ID not set - admin functions restricted")
    print("   Please add ADMIN_ID in Railway Variables")

# =========================================================

# ===== Third Party Imports =====
try:
    import requests
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import (
        Application, CommandHandler, CallbackQueryHandler,
        ContextTypes, MessageHandler, filters, ConversationHandler
    )
except ImportError as e:
    print(f"Missing required library: {e}")
    print("Install: pip install python-telegram-bot requests")
    sys.exit(1)

# ===== Logging =====
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ===== Configuration =====
DB_PATH = "referral_monitor.db"
MAX_RECENT_SUCCESS = 20
PORT = int(os.getenv("PORT", "8080"))

# Conversation states
WAITING_FOR_FIREBASE_URL = 1

# ===== Database Layer =====
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    target INTEGER DEFAULT 40,
    firebase_url TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    phone TEXT,
    name TEXT,
    timestamp REAL,
    user_id INTEGER
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS stats (
    user_id INTEGER,
    success_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    last_fail_reason TEXT,
    last_success_time REAL,
    PRIMARY KEY (user_id)
);
"""

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        for stmt in SCHEMA.split(';'):
            if stmt.strip():
                conn.execute(stmt)
        conn.commit()

# ===== Firebase Monitor =====
class FirebaseMonitor:
    def __init__(self, firebase_url: str):
        self.firebase_url = firebase_url.rstrip('/') + '/'
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._event_queue = Queue()
        self._known_ids: Set[str] = set()
        self._lock = threading.Lock()
        self._load_known_ids()
        self._fetch_initial_state()
        logger.info(f"FirebaseMonitor initialized")

    def _load_known_ids(self):
        try:
            with get_db() as conn:
                rows = conn.execute("SELECT event_id FROM processed_events").fetchall()
                with self._lock:
                    self._known_ids = {row['event_id'] for row in rows}
                logger.info(f"Loaded {len(self._known_ids)} known event IDs")
        except Exception as e:
            logger.error(f"Error loading known IDs: {e}")
            with self._lock:
                self._known_ids = set()

    def _fetch_initial_state(self):
        try:
            resp = requests.get(self.firebase_url + 'clients.json', timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    for c_id in data.keys():
                        with self._lock:
                            self._known_ids.add(f"client_{c_id}")
                logger.info(f"Initial state fetched")
        except Exception as e:
            logger.warning(f"Could not fetch initial state: {e}")

    def _extract_referral_events(self, client_data: dict) -> List[Dict]:
        events = []
        try:
            for client_id, data in client_data.items():
                if not isinstance(data, dict):
                    continue
                if data.get('status') == 'registered':
                    event_id = f"reg_{client_id}_{int(time.time())}"
                    display_name = data.get('display_name', 'Unknown User')
                    phone = data.get('phone', '')
                    
                    if phone and len(phone) >= 10:
                        with self._lock:
                            if event_id in self._known_ids:
                                continue
                            duplicate_check = f"phone_{phone}_{int(time.time() / 3600)}"
                            if duplicate_check in self._known_ids:
                                continue
                        
                        events.append({
                            'event_id': event_id,
                            'phone': phone,
                            'name': display_name,
                            'timestamp': time.time(),
                            'client_id': client_id
                        })
        except Exception as e:
            logger.error(f"Error extracting events: {e}")
        return events

    def _check_for_new_events(self):
        try:
            resp = requests.get(self.firebase_url + 'clients.json', timeout=15)
            if resp.status_code != 200:
                logger.warning(f"Firebase API error: {resp.status_code}")
                return

            data = resp.json()
            if not isinstance(data, dict):
                return

            events = self._extract_referral_events(data)
            
            for event_data in events:
                if not self._is_valid_referral_event(event_data):
                    continue
                
                self._record_event(
                    event_data['event_id'], 
                    event_data['phone'], 
                    event_data['name'], 
                    time.time()
                )
                self._event_queue.put({
                    'phone': event_data['phone'],
                    'name': event_data['name'],
                    'timestamp': time.time()
                })
                logger.info(f"New referral: +91{event_data['phone']} ({event_data['name']})")

        except requests.RequestException as e:
            logger.error(f"Firebase request failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

    def _is_valid_referral_event(self, event_data: dict) -> bool:
        required_fields = ['event_id', 'phone', 'name']
        if not all(field in event_data for field in required_fields):
            return False
        
        phone = event_data.get('phone', '')
        name = event_data.get('name', '')
        
        if len(phone) < 10 or not phone.isdigit():
            return False
        
        suspicious_names = ['test', 'bot', 'fake', 'admin', 'dummy', 'temp']
        if any(s in name.lower() for s in suspicious_names):
            logger.warning(f"Suspicious event rejected: {name}")
            return False
        
        with self._lock:
            recent_count = sum(1 for eid in self._known_ids 
                             if eid.startswith(f"phone_{phone}_") 
                             and int(time.time() - 3600) < int(eid.split('_')[-1]) * 3600)
            if recent_count >= 5:
                logger.warning(f"Rate limit exceeded for +91{phone}")
                return False
        
        return True

    def _record_event(self, event_id: str, phone: str, name: str, timestamp: float):
        with get_db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO processed_events (event_id, phone, name, timestamp, user_id) VALUES (?, ?, ?, ?, ?)",
                (event_id, phone, name, timestamp, 0)
            )
            conn.commit()
        
        with self._lock:
            self._known_ids.add(event_id)
            hour_key = f"phone_{phone}_{int(timestamp / 3600)}"
            self._known_ids.add(hour_key)

    def get_global_stats(self) -> Tuple[int, int]:
        try:
            with get_db() as conn:
                success = conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0]
                failed = conn.execute("SELECT COALESCE(SUM(failed_count), 0) FROM stats").fetchone()[0]
                return success, failed
        except Exception:
            return 0, 0

    def get_user_stats(self, user_id: int) -> dict:
        try:
            with get_db() as conn:
                row = conn.execute(
                    "SELECT success_count, failed_count, last_fail_reason FROM stats WHERE user_id = ?",
                    (user_id,)
                ).fetchone()
                if row:
                    return dict(row)
                return {'success_count': 0, 'failed_count': 0, 'last_fail_reason': ''}
        except Exception:
            return {'success_count': 0, 'failed_count': 0, 'last_fail_reason': ''}

    def update_user_stats(self, user_id: int, success: bool = True, fail_reason: str = ""):
        with get_db() as conn:
            if success:
                conn.execute(
                    "INSERT INTO stats (user_id, success_count, failed_count, last_success_time) "
                    "VALUES (?, 1, 0, ?) ON CONFLICT(user_id) DO UPDATE SET "
                    "success_count = success_count + 1, last_success_time = ?",
                    (user_id, time.time(), time.time())
                )
            else:
                conn.execute(
                    "INSERT INTO stats (user_id, success_count, failed_count, last_fail_reason) "
                    "VALUES (?, 0, 1, ?) ON CONFLICT(user_id) DO UPDATE SET "
                    "failed_count = failed_count + 1, last_fail_reason = ?",
                    (user_id, fail_reason, fail_reason)
                )
            conn.commit()

    def get_user_target(self, user_id: int) -> int:
        with get_db() as conn:
            row = conn.execute(
                "SELECT target FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if row:
                return row['target']
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, target) VALUES (?, ?)",
                (user_id, DEFAULT_TARGET)
            )
            conn.commit()
            return DEFAULT_TARGET

    def get_user_firebase_url(self, user_id: int) -> Optional[str]:
        with get_db() as conn:
            row = conn.execute(
                "SELECT firebase_url FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if row and row['firebase_url']:
                return row['firebase_url']
            return None

    def update_user_firebase_url(self, user_id: int, firebase_url: str):
        with get_db() as conn:
            conn.execute(
                "INSERT INTO users (user_id, firebase_url) VALUES (?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET firebase_url = ?",
                (user_id, firebase_url, firebase_url)
            )
            conn.commit()

    def update_user_target(self, user_id: int, target: int):
        with get_db() as conn:
            conn.execute(
                "INSERT INTO users (user_id, target) VALUES (?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET target = ?",
                (user_id, target, target)
            )
            conn.commit()

    def get_recent_successes(self, limit: int = 10) -> List[Dict]:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT phone, name, timestamp FROM processed_events "
                "ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return [dict(row) for row in rows]

    def start_monitoring(self):
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        logger.info("Monitoring started")

    def stop_monitoring(self):
        self.running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Monitoring stopped")

    def _monitor_loop(self):
        while self.running:
            try:
                self._check_for_new_events()
                time.sleep(CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                time.sleep(5)

    def get_pending_events(self) -> List[Dict]:
        events = []
        try:
            while True:
                events.append(self._event_queue.get_nowait())
        except Empty:
            pass
        return events

# ===== Telegram Bot =====
class ReferralBot:
    def __init__(self, token: str, admin_ids: List[int]):
        self.token = token
        self.admin_ids = admin_ids
        self.monitor: Optional[FirebaseMonitor] = None
        self.application: Optional[Application] = None
        
    def is_admin(self, user_id: int) -> bool:
        return user_id in self.admin_ids

    def _get_keyboard(self, user_id: int) -> InlineKeyboardMarkup:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        
        keyboard = [
            [InlineKeyboardButton("📊 Live Progress", callback_data="progress")],
            [InlineKeyboardButton("👥 Successful Referrals", callback_data="success_list")],
            [InlineKeyboardButton("📜 Recent Success", callback_data="recent")],
        ]
        
        if self.monitor:
            if self.monitor.running:
                keyboard.append([InlineKeyboardButton("⏹ Stop Monitor", callback_data="stop")])
            else:
                keyboard.append([InlineKeyboardButton("▶️ Start Monitor", callback_data="start")])
        
        keyboard.append([InlineKeyboardButton("⚙️ Settings", callback_data="settings")])
        
        if self.is_admin(user_id):
            keyboard.append([InlineKeyboardButton("🔧 Admin Panel", callback_data="admin")])
        
        return InlineKeyboardMarkup(keyboard)

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        username = update.effective_user.username or "User"
        
        # Initialize user
        with get_db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, target) VALUES (?, ?)",
                (user_id, DEFAULT_TARGET)
            )
            conn.commit()
        
        # Check if Firebase URL is set
        firebase_url = None
        with get_db() as conn:
            row = conn.execute(
                "SELECT firebase_url FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if row:
                firebase_url = row['firebase_url']
        
        # If FIREBASE_URL is set in environment, use it
        if not firebase_url and FIREBASE_URL:
            firebase_url = FIREBASE_URL
            with get_db() as conn:
                conn.execute(
                    "UPDATE users SET firebase_url = ? WHERE user_id = ?",
                    (firebase_url, user_id)
                )
                conn.commit()
        
        # If no Firebase URL, ask user
        if not firebase_url:
            await update.message.reply_text(
                f"🚀 Welcome {username} to StockGro Referral Monitor!\n\n"
                f"⚠️ **Firebase URL not set!**\n\n"
                f"Please send your Firebase URL to start monitoring.\n\n"
                f"Example: `https://stockgro-xxxx.firebaseio.com/`\n\n"
                f"Type or paste your Firebase URL below:",
                parse_mode='Markdown'
            )
            return WAITING_FOR_FIREBASE_URL
        
        # Initialize monitor with stored URL
        if not self.monitor:
            self.monitor = FirebaseMonitor(firebase_url)
        
        target = self.monitor.get_user_target(user_id)
        
        welcome_msg = (
            f"🚀 Welcome {username} to StockGro Referral Monitor!\n\n"
            f"📌 Your personal referral tracking dashboard.\n"
            f"🎯 Target: {target}\n"
            f"📡 Firebase: Connected ✅\n\n"
            f"Use the buttons below to control monitoring and view progress."
        )
        
        await update.message.reply_text(
            welcome_msg,
            reply_markup=self._get_keyboard(user_id)
        )
        return ConversationHandler.END

    async def handle_firebase_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        firebase_url = update.message.text.strip()
        
        # Validate URL
        if not firebase_url.startswith("https://") or "firebase" not in firebase_url:
            await update.message.reply_text(
                "❌ Invalid Firebase URL!\n\n"
                "URL should be like:\n"
                "`https://stockgro-xxxx.firebaseio.com/`\n\n"
                "Please send again:",
                parse_mode='Markdown'
            )
            return WAITING_FOR_FIREBASE_URL
        
        # Save Firebase URL
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET firebase_url = ? WHERE user_id = ?",
                (firebase_url, user_id)
            )
            conn.commit()
        
        # Initialize monitor
        if not self.monitor:
            self.monitor = FirebaseMonitor(firebase_url)
        else:
            self.monitor.firebase_url = firebase_url.rstrip('/') + '/'
        
        target = self.monitor.get_user_target(user_id)
        
        await update.message.reply_text(
            f"✅ Firebase URL saved successfully!\n\n"
            f"📡 URL: {firebase_url}\n"
            f"🎯 Target: {target}\n\n"
            f"Send /start to view the dashboard.",
            reply_markup=self._get_keyboard(user_id)
        )
        return ConversationHandler.END

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        data = query.data
        
        # Check if Firebase URL is set
        with get_db() as conn:
            row = conn.execute(
                "SELECT firebase_url FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            firebase_url = row['firebase_url'] if row else None
        
        if not firebase_url and FIREBASE_URL:
            firebase_url = FIREBASE_URL
            with get_db() as conn:
                conn.execute(
                    "UPDATE users SET firebase_url = ? WHERE user_id = ?",
                    (firebase_url, user_id)
                )
                conn.commit()
        
        if not firebase_url:
            await query.edit_message_text(
                "⚠️ **Firebase URL not set!**\n\n"
                "Please send /start and enter your Firebase URL.",
                parse_mode='Markdown'
            )
            return
        
        # Initialize monitor if not already
        if not self.monitor:
            self.monitor = FirebaseMonitor(firebase_url)
        
        if data == "progress":
            await self.show_progress(query, user_id)
        elif data == "success_list":
            await self.show_success_list(query, user_id)
        elif data == "recent":
            await self.show_recent(query, user_id)
        elif data == "start":
            await self.start_monitor(query, user_id)
        elif data == "stop":
            await self.stop_monitor(query, user_id)
        elif data == "settings":
            await self.show_settings(query, user_id)
        elif data == "admin":
            if self.is_admin(user_id):
                await self.show_admin_panel(query, user_id)
            else:
                await query.edit_message_text("⛔ Access denied. Admin only.")
        elif data.startswith("set_target_"):
            await self.set_target(query, user_id, data.split("_")[-1])
        elif data == "broadcast":
            if self.is_admin(user_id):
                await query.edit_message_text("📢 Send /broadcast <message> to broadcast to all users")
            else:
                await query.edit_message_text("⛔ Access denied.")
        elif data == "stats":
            if self.is_admin(user_id):
                await self.show_stats(query, user_id)
            else:
                await query.edit_message_text("⛔ Access denied.")
        elif data == "refresh":
            await self.show_progress(query, user_id)
        elif data == "back_to_menu":
            await query.edit_message_text(
                "📋 Main Menu",
                reply_markup=self._get_keyboard(user_id)
            )

    async def show_progress(self, query, user_id: int):
        if not self.monitor:
            await query.edit_message_text("⚠️ Monitor not initialized.")
            return
        
        target = self.monitor.get_user_target(user_id)
        stats = self.monitor.get_user_stats(user_id)
        global_success, _ = self.monitor.get_global_stats()
        success_count = stats.get('success_count', global_success)
        
        progress_text = (
            f"📊 REFERRAL PROGRESS\n"
            f"{'═' * 25}\n"
            f"✅ Successful: {success_count}\n"
            f"🎯 Target: {target}\n"
            f"📈 Progress: {success_count}/{target}\n"
            f"📊 Percentage: {min(100, int((success_count/target)*100)) if target > 0 else 0}%\n"
            f"\n📌 Status: {'🟢 Active' if self.monitor.running else '🔴 Stopped'}"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="refresh")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu")]
        ]
        
        await query.edit_message_text(
            progress_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_success_list(self, query, user_id: int):
        if not self.monitor:
            await query.edit_message_text("⚠️ Monitor not initialized.")
            return
        
        events = self.monitor.get_recent_successes(MAX_RECENT_SUCCESS)
        
        if not events:
            await query.edit_message_text("📭 No successful referrals yet.")
            return
        
        def mask_phone(phone: str) -> str:
            if len(phone) >= 10:
                return phone[:2] + "****" + phone[-4:]
            return phone
        
        text = f"👥 Recent Successful Referrals ({len(events)})\n{'═' * 30}\n"
        
        for idx, event in enumerate(events, 1):
            phone = mask_phone(event.get('phone', 'Unknown'))
            name = event.get('name', 'Unknown')
            timestamp = datetime.fromtimestamp(event.get('timestamp', time.time())).strftime("%H:%M")
            text += f"{idx}. {name} | 📱 +91{phone} | ⏰ {timestamp}\n"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu")]]
        await query.edit_message_text(
            text[:4000],
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_recent(self, query, user_id: int):
        if not self.monitor:
            await query.edit_message_text("⚠️ Monitor not initialized.")
            return
        
        events = self.monitor.get_recent_successes(1)
        
        if not events:
            await query.edit_message_text("📭 No recent successes.")
            return
        
        event = events[0]
        phone = event.get('phone', 'Unknown')
        if len(phone) >= 10:
            phone = phone[:3] + "****" + phone[-4:]
        
        text = (
            f"📜 Most Recent Success\n"
            f"{'═' * 25}\n"
            f"👤 Name: {event.get('name', 'Unknown')}\n"
            f"📱 Phone: +91{phone}\n"
            f"⏰ Time: {datetime.fromtimestamp(event.get('timestamp', time.time())).strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def start_monitor(self, query, user_id: int):
        if not self.is_admin(user_id):
            await query.edit_message_text("⛔ Only admins can start monitoring.")
            return
        
        if not self.monitor:
            await query.edit_message_text("⚠️ Please set Firebase URL first.")
            return
        
        if self.monitor.running:
            await query.edit_message_text("🟢 Monitoring already running.")
            return
        
        self.monitor.start_monitoring()
        text = "✅ Monitoring started!\n\n📊 Real-time referral tracking is now active."
        await query.edit_message_text(text, reply_markup=self._get_keyboard(user_id))

    async def stop_monitor(self, query, user_id: int):
        if not self.is_admin(user_id):
            await query.edit_message_text("⛔ Only admins can stop monitoring.")
            return
        
        if not self.monitor or not self.monitor.running:
            await query.edit_message_text("🟡 Monitoring is already stopped.")
            return
        
        self.monitor.stop_monitoring()
        text = "⏹ Monitoring stopped.\n\n📊 No further events will be tracked."
        await query.edit_message_text(text, reply_markup=self._get_keyboard(user_id))

    async def show_settings(self, query, user_id: int):
        if not self.monitor:
            await query.edit_message_text("⚠️ Monitor not initialized.")
            return
        
        target = self.monitor.get_user_target(user_id)
        stats = self.monitor.get_user_stats(user_id)
        
        text = (
            f"⚙️ Settings\n"
            f"{'═' * 25}\n"
            f"👤 User ID: {user_id}\n"
            f"🎯 Current Target: {target}\n"
            f"✅ Successes: {stats.get('success_count', 0)}\n"
            f"📊 Monitoring: {'🟢 Active' if self.monitor.running else '🔴 Stopped'}\n\n"
            f"Set new target:"
        )
        
        keyboard = [
            [
                InlineKeyboardButton("10", callback_data="set_target_10"),
                InlineKeyboardButton("25", callback_data="set_target_25"),
                InlineKeyboardButton("40", callback_data="set_target_40"),
            ],
            [
                InlineKeyboardButton("50", callback_data="set_target_50"),
                InlineKeyboardButton("100", callback_data="set_target_100"),
                InlineKeyboardButton("Custom", callback_data="set_target_custom"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu")]
        ]
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def set_target(self, query, user_id: int, target_str: str):
        if not self.monitor:
            await query.edit_message_text("⚠️ Monitor not initialized.")
            return
        
        if target_str == "custom":
            await query.edit_message_text(
                "Enter custom target as: /settarget <number>\n"
                "Example: /settarget 75"
            )
            return
        
        try:
            target = int(target_str)
            if target <= 0:
                await query.edit_message_text("❌ Target must be positive.")
                return
            
            self.monitor.update_user_target(user_id, target)
            await query.edit_message_text(
                f"✅ Target updated to {target}!",
                reply_markup=self._get_keyboard(user_id)
            )
        except ValueError:
            await query.edit_message_text("❌ Invalid target number.")

    async def show_admin_panel(self, query, user_id: int):
        if not self.is_admin(user_id):
            await query.edit_message_text("⛔ Access denied.")
            return
        
        if not self.monitor:
            await query.edit_message_text("⚠️ Monitor not initialized.")
            return
        
        global_success, global_failed = self.monitor.get_global_stats()
        
        text = (
            f"🔧 Admin Panel\n"
            f"{'═' * 25}\n"
            f"📊 Global Success: {global_success}\n"
            f"❌ Global Failed: {global_failed}\n"
            f"🟢 Monitor Status: {'Active' if self.monitor.running else 'Stopped'}\n"
            f"📱 Firebase URL: {self.monitor.firebase_url[:50]}...\n"
            f"👥 Total Events: {len(self.monitor._known_ids) if hasattr(self.monitor, '_known_ids') else 0}\n"
        )
        
        keyboard = [
            [InlineKeyboardButton("📊 Full Stats", callback_data="stats")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="refresh")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu")]
        ]
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def show_stats(self, query, user_id: int):
        if not self.is_admin(user_id) or not self.monitor:
            await query.edit_message_text("⛔ Access denied.")
            return
        
        global_success, global_failed = self.monitor.get_global_stats()
        recent = self.monitor.get_recent_successes(5)
        
        text = (
            f"📊 Detailed Statistics\n"
            f"{'═' * 25}\n"
            f"✅ Total Successes: {global_success}\n"
            f"❌ Total Failed: {global_failed}\n"
            f"📈 Success Rate: {int((global_success/(global_success+global_failed))*100) if (global_success+global_failed) > 0 else 0}%\n"
            f"⏳ Monitoring: {'🟢 Active' if self.monitor.running else '🔴 Stopped'}\n\n"
            f"📌 Recent 5 Events:\n"
        )
        
        for idx, event in enumerate(recent, 1):
            phone = event.get('phone', 'Unknown')
            if len(phone) >= 10:
                phone = phone[:3] + "****" + phone[-4:]
            text += f"  {idx}. {event.get('name', 'Unknown')} | +91{phone}\n"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="admin")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        if not self.is_admin(user_id):
            await update.message.reply_text("⛔ Admin only.")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /broadcast <message>")
            return
        
        message = ' '.join(context.args)
        
        with get_db() as conn:
            users = conn.execute("SELECT user_id FROM users").fetchall()
        
        sent_count = 0
        for user in users:
            try:
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text=f"📢 Broadcast from Admin:\n\n{message}"
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Broadcast failed to {user['user_id']}: {e}")
        
        await update.message.reply_text(f"✅ Broadcast sent to {sent_count} users.")

    async def settarget_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        if not context.args:
            await update.message.reply_text("Usage: /settarget <number>")
            return
        
        try:
            target = int(context.args[0])
            if target <= 0:
                await update.message.reply_text("❌ Target must be positive.")
                return
            
            if self.monitor:
                self.monitor.update_user_target(user_id, target)
            else:
                with get_db() as conn:
                    conn.execute(
                        "INSERT INTO users (user_id, target) VALUES (?, ?) "
                        "ON CONFLICT(user_id) DO UPDATE SET target = ?",
                        (user_id, target, target)
                    )
                    conn.commit()
            
            await update.message.reply_text(
                f"✅ Target updated to {target}!\nUse /start to see dashboard.",
                reply_markup=self._get_keyboard(user_id)
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid number. Use /settarget 40")

    async def alert_handler(self, context: ContextTypes.DEFAULT_TYPE):
        if not self.monitor:
            return
        
        events = self.monitor.get_pending_events()
        
        for event in events:
            phone = event.get('phone', 'Unknown')
            name = event.get('name', 'Unknown')
            global_success, _ = self.monitor.get_global_stats()
            
            with get_db() as conn:
                users = conn.execute("SELECT user_id FROM users").fetchall()
            
            for user in users:
                user_id = user['user_id']
                target = self.monitor.get_user_target(user_id)
                
                if len(phone) >= 10:
                    display_phone = phone[:3] + "****" + phone[-4:]
                else:
                    display_phone = phone
                
                alert_text = (
                    f"✅ New StockGro referral success!\n\n"
                    f"📱 Phone: +91{display_phone}\n"
                    f"👤 Name: {name}\n"
                    f"📊 Progress: {global_success}/{target}\n"
                    f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}"
                )
                
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=alert_text,
                        reply_markup=self._get_keyboard(user_id)
                    )
                    if self.monitor:
                        self.monitor.update_user_stats(user_id, success=True)
                except Exception as e:
                    logger.error(f"Alert failed for {user_id}: {e}")
            
            await asyncio.sleep(0.5)

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        logger.error(f"Update {update} caused error {context.error}")
        try:
            if update and hasattr(update, 'effective_message'):
                await context.bot.send_message(
                    chat_id=update.effective_user.id,
                    text="⚠️ An error occurred. Please try again."
                )
        except Exception:
            pass

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        if not update.message.text.startswith('/'):
            await update.message.reply_text(
                "Use /start to see the dashboard.",
                reply_markup=self._get_keyboard(user_id)
            )

    def run(self):
        self.application = Application.builder().token(self.token).build()
        
        # Conversation handler for Firebase URL setup
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", self.start_command)
            ],
            states={
                WAITING_FOR_FIREBASE_URL: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_firebase_url_input)
                ]
            },
            fallbacks=[
                CommandHandler("start", self.start_command),
                CommandHandler("cancel", self.start_command)
            ]
        )
        
        self.application.add_handler(conv_handler)
        self.application.add_handler(CommandHandler("settarget", self.settarget_command))
        self.application.add_handler(CommandHandler("broadcast", self.broadcast_command))
        self.application.add_handler(CallbackQueryHandler(self.button_handler))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        
        job_queue = self.application.job_queue
        if job_queue:
            job_queue.run_repeating(self.alert_handler, interval=3, first=1)
        
        self.application.add_error_handler(self.error_handler)
        logger.info("Bot starting on Railway...")
        
        # Use polling for Railway
        self.application.run_polling()

# ===== Main =====
def main():
    init_db()
    
    print("=" * 50)
    print("  StockGro Referral Monitor Bot")
    print("  Environment Variables Only - No Hardcoding")
    print("=" * 50)
    print(f"✅ Bot starting")
    print(f"📡 BOT_TOKEN: {'✅ Set' if BOT_TOKEN else '❌ MISSING'}")
    print(f"👥 ADMIN_ID: {'✅ Set' if ADMIN_IDS else '❌ MISSING'}")
    print(f"📡 Firebase: {'✅ Set in ENV' if FIREBASE_URL else '⚠️ User will set via bot'}")
    print(f"🎯 Default Target: {DEFAULT_TARGET}")
    print("=" * 50)
    
    bot = ReferralBot(BOT_TOKEN, ADMIN_IDS)
    bot.run()

if __name__ == "__main__":
    main()
