import asyncio
import random
import string
import re
import json
import tempfile
from datetime import datetime, timedelta
from telethon import TelegramClient, functions, types, events
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    UpdateAppToLoginError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    SessionExpiredError,
    PasswordHashInvalidError,
    RPCError,
    ChannelInvalidError,
    UserDeactivatedError,
    UserDeactivatedBanError,
    AuthKeyUnregisteredError
)
from pyrogram import Client as PyroClient, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from pyrogram.errors import (
    UserNotParticipant,
    PeerIdInvalid,
    ChatWriteForbidden,
    FloodWait,
    MessageNotModified,
)
from pyrogram.enums import ParseMode, ChatType
import config
from database import EnhancedDatabaseManager
from utils import validate_phone_number, generate_progress_bar, format_duration
import os
import logging
from cryptography.fernet import Fernet, InvalidToken
import threading
import time
import requests
from flask import Flask, jsonify
import sys
import io
import subprocess

# =======================================================
# REMOVED: AccountManager class - Not used in current broadcast system
# =======================================================

# =======================================================
# 🔍 ACCOUNT HEALTH MONITOR
# =======================================================

class AccountHealthMonitor:
    """Continuously monitors accounts for bans/freezes and auto-removes them"""
    
    def __init__(self):
        self.monitoring = {}
        self.banned_accounts = set()
    
    async def check_account_status(self, client, account_id, user_id):
        """Check if an account is still active"""
        try:
            # Try to get account info
            me = await client.get_me()
            if me:
                return True, "Active"
        except UserDeactivatedBanError:
            return False, "Account is banned/deactivated"
        except AuthKeyUnregisteredError:
            return False, "Account session expired"
        except UserDeactivatedError:
            return False, "Account is deactivated"
        except Exception as e:
            err_str = str(e).lower()
            if any(word in err_str for word in ['ban', 'deactivat', 'deleted', 'invalid']):
                return False, f"Account issue: {str(e)[:50]}"
            return True, "Unknown status"
    
    async def remove_banned_account(self, user_id, account_id, reason):
        """Remove banned account and all its data from database"""
        try:
            logger.warning(f"🚨 Auto-removing banned account {account_id} for user {user_id}: {reason}")
            
            # Delete account from database
            result = db.db.accounts.delete_one({
                'user_id': user_id,
                '_id': account_id
            })
            
            if result.deleted_count > 0:
                logger.info(f"✅ Removed banned account {account_id} from database")
                
                # Send notification to user
                try:
                    await send_dm_log(user_id,
                        f"<blockquote>🚨 <b>ACCOUNT REMOVED AUTOMATICALLY</b></blockquote>\n\n"
                        f"<b>Reason:</b> {reason}\n"
                        f"<b>Account ID:</b> <code>{str(account_id)[:8]}...</code>\n\n"
                        f"<b>Action Taken:</b>\n"
                        f"• Account removed from database\n"
                        f"• Broadcast continues with remaining accounts\n\n"
                        f"<i>⚠️ Please add a new account to continue broadcasting</i>"
                    )
                except Exception as e:
                    logger.error(f"Failed to send notification: {e}")
                
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to remove banned account {account_id}: {e}")
            return False
    
    async def monitor_account(self, client, account_id, user_id):
        """Continuously monitor a single account"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every 60 seconds
                
                # Skip if not in monitoring list
                if user_id not in self.monitoring or not self.monitoring[user_id]:
                    break
                
                # Check account status
                is_active, status = await self.check_account_status(client, account_id, user_id)
                
                if not is_active:
                    # Account is banned/frozen
                    self.banned_accounts.add(account_id)
                    
                    # Remove from database
                    await self.remove_banned_account(user_id, account_id, status)
                    
                    # Stop monitoring this account
                    break
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring account {account_id}: {e}")
                await asyncio.sleep(30)
    
    def start_monitoring(self, user_id):
        """Start monitoring all accounts for a user"""
        self.monitoring[user_id] = True
    
    def stop_monitoring(self, user_id):
        """Stop monitoring accounts for a user"""
        self.monitoring[user_id] = False
    
    def is_account_banned(self, account_id):
        """Check if account is marked as banned"""
        return account_id in self.banned_accounts

# Global account health monitor
account_monitor = AccountHealthMonitor()

# =======================================================
# 🚀 INITIALIZATION & CONFIGURATION
# =======================================================
# REMOVED: Duplicate imports already at top of file

# ✅ Force UTF-8 output for PowerShell and cmd (Windows fix)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="ignore")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="ignore")

# ✅ Force Python to use UTF-8 mode for all IO
os.environ["PYTHONIOENCODING"] = "utf-8"

# =======================================================
# 🧠 LOGGING CONFIGURATION
# =======================================================
# Main log levels
logging.getLogger("__main__").setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("telethon").setLevel(logging.ERROR)

# 🧹 Show INFO level logs from database (to see Clear APIs progress)
db_logger = logging.getLogger("database")
db_logger.setLevel(logging.INFO)

# 🧱 Suppress noisy asyncio socket warnings
def _ignore_socket_warnings(loop, context):
    """Suppress harmless asyncio 'socket.send() raised exception' warnings."""
    msg = context.get("message", "")
    exc = context.get("exception")

    # Ignore low-level harmless network errors
    if isinstance(exc, OSError) or "socket.send" in msg:
        logging.getLogger("asyncio").debug(f"Ignored asyncio socket warning: {msg}")
        return

    # Let real exceptions show normally
    loop.default_exception_handler(context)

# Apply handler globally to current event loop
try:
    asyncio.get_event_loop().set_exception_handler(_ignore_socket_warnings)
except RuntimeError:
    pass

# =======================================================
# 🧩 OTHER GLOBALS & SETUP
# =======================================================
# Auto-reply runtime state (in-memory)

# Global main asyncio loop reference
MAIN_LOOP = None

# ✅ Initialize Flask app
app = Flask(__name__)

# Create logs directory and set up file + console logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/Brutod_bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

print("BRUTOD ADS Bot Stopped Successfully 🚀")

# =======================================================
# 🔐 ENCRYPTION KEY INITIALIZATION
# =======================================================

ENCRYPTION_KEY = getattr(config, 'ENCRYPTION_KEY', None)
KEY_FILE = 'encryption.key'

if not ENCRYPTION_KEY:
    logger.warning("No ENCRYPTION_KEY in config. Loading or generating from file.")
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, 'r', encoding='utf-8') as f:
            ENCRYPTION_KEY = f.read().strip()
    else:
        ENCRYPTION_KEY = Fernet.generate_key().decode()
        with open(KEY_FILE, 'w', encoding='utf-8') as f:
            f.write(ENCRYPTION_KEY)
        logger.info("Generated and saved new encryption key to encryption.key")
else:
    with open(KEY_FILE, 'w', encoding='utf-8') as f:
        f.write(ENCRYPTION_KEY)
    logger.info("Using ENCRYPTION_KEY from config and saved to file.")

cipher_suite = Fernet(ENCRYPTION_KEY.encode())

# =======================================================
# 🗄️ DATABASE INITIALIZATION
# =======================================================
db = EnhancedDatabaseManager()

# =======================================================
# 🗣️ GROUPS MANAGEMENT SYSTEM
# =======================================================

def ensure_db_methods(db):
    """Ensure database has required group management methods"""
    
    if not hasattr(db, 'add_target_group'):
        def add_target_group(self, user_id, group_id, title):
            """Add a target group for broadcasting"""
            self.db.target_groups.update_one(
                {
                    'user_id': user_id,
                    'group_id': group_id
                },
                {
                    '$set': {
                        'user_id': user_id,
                        'group_id': group_id,
                        'title': title,
                        'added_at': datetime.now()
                    }
                },
                upsert=True
            )
        setattr(db.__class__, 'add_target_group', add_target_group)
    
    if not hasattr(db, 'remove_target_group'):
        def remove_target_group(self, user_id, group_id):
            """Remove a target group from broadcasting"""
            self.db.target_groups.delete_one({
                'user_id': user_id,
                'group_id': group_id
            })
        setattr(db.__class__, 'remove_target_group', remove_target_group)
    
    if not hasattr(db, 'get_target_group'):
        def get_target_group(self, user_id, group_id):
            """Get a specific target group"""
            return self.db.target_groups.find_one({
                'user_id': user_id,
                'group_id': group_id
            })
        setattr(db.__class__, 'get_target_group', get_target_group)
    
    # REMOVED: Blacklist management - sending to all groups without restrictions
    if not hasattr(db, 'clear_all_blacklisted_groups'):
        def clear_all_blacklisted_groups(self, user_id):
            """Clear all blacklisted groups for a user"""
            try:
                result = self.db.blacklisted_groups.delete_many({'user_id': user_id})
                return result.deleted_count
            except Exception as e:
                logger.error(f"Failed to clear blacklisted groups for user {user_id}: {e}")
                return 0
        setattr(db.__class__, 'clear_all_blacklisted_groups', clear_all_blacklisted_groups)
    
    if not hasattr(db, 'get_blacklisted_groups_count'):
        def get_blacklisted_groups_count(self, user_id):
            """Get count of blacklisted groups for a user"""
            try:
                return self.db.blacklisted_groups.count_documents({'user_id': user_id})
            except Exception as e:
                logger.error(f"Failed to count blacklisted groups for user {user_id}: {e}")
                return 0
        setattr(db.__class__, 'get_blacklisted_groups_count', get_blacklisted_groups_count)

# =======================================================
# 🚀 ULTRA-FAST GROUPS CACHE SYSTEM
# =======================================================

GROUPS_CACHE = {}
GROUPS_CACHE_TTL = 120  # 2 minutes cache (faster refresh)
QUICK_CACHE = {}  # Instant access cache for immediate responses

# Background task tracking
PRELOAD_TASKS = {}

async def ultra_fast_preload_groups(uid):
    """⚡ ULTRA-FAST group preloading with instant response + background loading"""
    try:
        # 🎯 INSTANT RESPONSE: Check if we have ANY cached data
        quick_cache = QUICK_CACHE.get(uid)
        if quick_cache and time.time() - quick_cache['timestamp'] < 30:  # 30 sec instant cache
            logger.info(f"🚀 INSTANT cache hit for user {uid} - {len(quick_cache['groups'])} groups")
            return quick_cache['groups']

        accounts = db.get_user_accounts(uid)
        if not accounts:
            return []

        # 🚀 PARALLEL ULTRA-FAST GROUP FETCHING
        async def lightning_fast_groups(acc):
            """Lightning-fast group fetching - only essentials"""
            tg_client = None
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                # Get user's API credentials
                credentials = db.get_user_api_credentials(acc['user_id'])
                if not credentials:
                    logger.error(f"No API credentials found for user {acc['user_id']}")
                    return []
                tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                await asyncio.wait_for(tg_client.connect(), timeout=3)  # 3sec timeout
                
                groups = []
                # 🚀 SPEED BOOST: Limit to first 100 dialogs for instant response
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group:
                        groups.append({
                            'id': dialog.id,
                            'title': dialog.title[:30],  # Truncate long titles for speed
                            'selected': True
                        })
                        
                        # Load all groups (no limit)
                            
                return groups
            except Exception as e:
                logger.warning(f"Fast fetch failed for {acc.get('phone_number', 'unknown')}: {e}")
                return []
            finally:
                if tg_client:
                    try:
                        await asyncio.wait_for(tg_client.disconnect(), timeout=1)
                    except Exception:
                        pass

        # 🚀 PARALLEL PROCESSING: All accounts simultaneously
        start_time = time.time()
        tasks = [lightning_fast_groups(acc) for acc in accounts]
        groups_lists = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine results
        all_groups = []
        seen_ids = set()
        for groups in groups_lists:
            if isinstance(groups, list):  # Skip exceptions
                for group in groups:
                    if group['id'] not in seen_ids:
                        seen_ids.add(group['id'])
                        all_groups.append(group)

        load_time = time.time() - start_time
        logger.info(f"⚡ ULTRA-FAST load completed in {load_time:.2f}s - {len(all_groups)} groups")

        # 🎯 DUAL CACHE: Quick cache for instant access + full cache
        QUICK_CACHE[uid] = {
            'groups': all_groups,
            'timestamp': time.time()
        }
        
        GROUPS_CACHE[uid] = {
            'groups': all_groups,
            'timestamp': time.time()
        }

        # 🚀 BACKGROUND TASK: Load remaining groups silently
        if uid not in PRELOAD_TASKS or PRELOAD_TASKS[uid].done():
            PRELOAD_TASKS[uid] = asyncio.create_task(background_full_load(uid, accounts))

        return all_groups

    except Exception as e:
        logger.error(f"Ultra-fast group loading failed for user {uid}: {e}")
        return []

async def background_full_load(uid, accounts):
    """🔄 Background task to load ALL groups without blocking UI"""
    try:
        await asyncio.sleep(2)  # Let UI respond first
        
        async def full_account_groups(acc):
            """Load ALL groups from account in background"""
            tg_client = None
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                # Get user's API credentials
                credentials = db.get_user_api_credentials(acc['user_id'])
                if not credentials:
                    logger.error(f"No API credentials found for user {acc['user_id']}")
                    return []
                tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                await tg_client.connect()
                
                groups = []
                async for dialog in tg_client.iter_dialogs(limit=None):  # ALL groups
                    if dialog.is_group:
                        groups.append({
                            'id': dialog.id,
                            'title': dialog.title,
                            'selected': True
                        })
                return groups
            except Exception as e:
                logger.debug(f"Background load failed for {acc.get('phone_number', 'unknown')}: {e}")
                return []
            finally:
                if tg_client:
                    try:
                        await tg_client.disconnect()
                    except Exception:
                        pass

        # Load all groups in background
        tasks = [full_account_groups(acc) for acc in accounts]
        all_groups_lists = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Merge with existing cache
        full_groups = []
        seen_ids = set()
        
        for groups in all_groups_lists:
            if isinstance(groups, list):
                for group in groups:
                    if group['id'] not in seen_ids:
                        seen_ids.add(group['id'])
                        full_groups.append(group)

        # Update full cache
        GROUPS_CACHE[uid] = {
            'groups': full_groups,
            'timestamp': time.time()
        }
        
        logger.info(f"🔄 Background loaded {len(full_groups)} total groups for user {uid}")

    except Exception as e:
        logger.error(f"Background group loading failed for user {uid}: {e}")

def get_ultra_fast_groups(uid):
    """🚀 Get groups with ultra-fast priority: Quick cache > Full cache > None"""
    # Priority 1: Instant quick cache
    quick = QUICK_CACHE.get(uid)
    if quick and time.time() - quick['timestamp'] < 60:  # 1 min quick cache
        return quick['groups']
    
    # Priority 2: Full cache
    cache = GROUPS_CACHE.get(uid)
    if cache and time.time() - cache['timestamp'] < GROUPS_CACHE_TTL:
        return cache['groups']
    
    # Priority 3: Expired - clean up
    QUICK_CACHE.pop(uid, None)
    GROUPS_CACHE.pop(uid, None)
    return None

def invalidate_groups_cache(uid):
    """🗑️ Force invalidate cache for user"""
    QUICK_CACHE.pop(uid, None)
    GROUPS_CACHE.pop(uid, None)
    if uid in PRELOAD_TASKS:
        PRELOAD_TASKS[uid].cancel()
        PRELOAD_TASKS.pop(uid, None)

# Legacy compatibility
async def preload_user_groups(uid):
    """Legacy compatibility wrapper"""
    return await ultra_fast_preload_groups(uid)


async def auto_select_all_groups(uid, phone):
# =======================================================
# 🔢 MULTI-ACCOUNT SLOT SYSTEM (ACCOUNT 1 / ACCOUNT 2)
# =======================================================

def get_user_account_slots(user_id):
    """Return account slots in order (1),(2)."""
    accounts = db.get_user_accounts(user_id)
    # Sort by added_at so oldest = slot 1, newest = slot 2
    sorted_acc = sorted(accounts, key=lambda x: x.get("added_at", datetime.now()))
    return sorted_acc[:2]  # max 2 accounts allowed


def assign_account_slot(user_id, account_data):
    """
    Assign slot number automatically:
    • First added = Slot 1
    • Second added = Slot 2
    """
    accounts = get_user_account_slots(user_id)

    slot_number = len(accounts) + 1
    if slot_number > 2:
        return False, "❌ Sirf 2 accounts allowed."

    # Add slot number inside account data
    account_data["slot"] = slot_number
    account_data["added_at"] = datetime.now()

    # Save in DB
    db.db.accounts.insert_one(account_data)

    return True, f"✅ Account added as Slot ({slot_number})"
# =======================================================
# 🔄 BROADCAST ROTATION ENGINE (ACCOUNT 1 / ACCOUNT 2)
# =======================================================

class BroadcastRotator:
    """
    Handles broadcast rotation for multiple accounts.
    Sends saved messages cycle-wise (3 messages per cycle by default)
    """

    def __init__(self, user_id, messages, cycle_size=3):
        self.user_id = user_id
        self.messages = messages  # list of message IDs from saved
        self.cycle_size = cycle_size
        self.accounts = get_user_account_slots(user_id)
        self.current_index = 0  # message index
        self.current_account_idx = 0  # 0 = slot 1, 1 = slot 2

    def get_next_batch(self):
        """
        Returns next batch of messages and account to use
        """
        if not self.accounts:
            return [], None

        # Decide which account to use
        account = self.accounts[self.current_account_idx]

        # Slice messages
        batch = self.messages[self.current_index:self.current_index + self.cycle_size]

        # Update indices
        self.current_index += self.cycle_size
        if self.current_index >= len(self.messages):
            # Reset index for next cycle
            self.current_index = 0
            # Switch account
            self.current_account_idx = (self.current_account_idx + 1) % len(self.accounts)

        return batch, account

    async def start_broadcast(self, target_groups):
        """
        Start broadcasting messages to all target groups in rotation
        """
        if not self.accounts:
            logger.error(f"No accounts for user {self.user_id}")
            return

        if not target_groups:
            logger.warning(f"No target groups for user {self.user_id}")
            return

        while True:
            batch, account = self.get_next_batch()
            if not batch or not account:
                await asyncio.sleep(5)
                continue

            # Decrypt session
            try:
                session_str = cipher_suite.decrypt(account['session_string'].encode()).decode()
                credentials = db.get_user_api_credentials(self.user_id)
                if not credentials:
                    logger.error(f"No API credentials for user {self.user_id}")
                    await asyncio.sleep(5)
                    continue

                tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                await tg_client.connect()

                for msg_id in batch:
                    for group in target_groups:
                        try:
                            await tg_client.send_message(group['id'], f"<b>Message ID: {msg_id}</b>", parse_mode='html')
                        except Exception as e:
                            logger.warning(f"Failed to send {msg_id} to {group['title']}: {e}")

                await tg_client.disconnect()

                # Wait between cycles (optional, e.g., 2 sec)
                await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"Broadcast failed for account {account.get('phone_number')}: {e}")
                await asyncio.sleep(5)
# =======================================================
# 🖥️ UI & ACCOUNT LOGIN (OTP FLOW + SLOT DISPLAY)
# =======================================================

async def add_account_flow(user_id, phone_number):
    """
    Adds new account with OTP verification.
    Assigns slot (1) or (2) automatically.
    """
    try:
        # Create a temporary Telethon client for login
        client = TelegramClient(StringSession(), config.API_ID, config.API_HASH)
        await client.connect()

        # Send code
        sent = await client.send_code_request(phone_number)
        print(f"Code sent to {phone_number}. Enter the OTP:")

        # Wait for user input
        otp = input("Enter OTP: ")

        # Sign in
        try:
            me = await client.sign_in(phone_number, otp)
        except SessionPasswordNeededError:
            password = input("Two-step password required. Enter password: ")
            me = await client.sign_in(password=password)

        # Save session
        session_string = client.session.save()

        # Prepare account data
        account_data = {
            'user_id': user_id,
            'phone_number': phone_number,
            'session_string': cipher_suite.encrypt(session_string.encode()).decode()
        }

        # Assign slot
        success, msg = assign_account_slot(user_id, account_data)
        print(msg)

        await client.disconnect()
        return success

    except Exception as e:
        logger.error(f"Failed to add account {phone_number}: {e}")
        return False


# ===============================
# ✅ Example command: start broadcast
# ===============================

async def start_user_broadcast(user_id):
    """
    Example starter for broadcast with rotation
    """
    # Get saved messages IDs (example, you can replace with real saved messages)
    saved_messages = db.get_saved_messages(user_id)  # assume list of IDs or text

    if not saved_messages:
        logger.warning("No saved messages for broadcast.")
        return

    # Get target groups
    target_groups = get_ultra_fast_groups(user_id)
    if not target_groups:
        logger.warning("No target groups found.")
        return

    # Start rotator
    rotator = BroadcastRotator(user_id, saved_messages, cycle_size=3)
    await rotator.start_broadcast(target_groups)


# ===============================
# 🔗 CLI / UI EXAMPLE
# ===============================

async def user_interface_example():
    """
    Example CLI for adding 2 accounts and starting broadcast
    """
    user_id = int(input("Enter your user ID: "))

    # Add first account
    phone1 = input("Enter phone number for Account 1: ")
    await add_account_flow(user_id, phone1)

    # Add second account
    phone2 = input("Enter phone number for Account 2: ")
    await add_account_flow(user_id, phone2)

    print("✅ Both accounts added. Broadcasting will rotate between them.")

    # Start broadcast
    await start_user_broadcast(user_id)


# ===============================
# 🔄 AUTO START EXAMPLE
# ===============================
if __name__ == "__main__":
    MAIN_LOOP = asyncio.get_event_loop()
    try:
        MAIN_LOOP.run_until_complete(user_interface_example())
    except KeyboardInterrupt:
        print("Broadcast stopped by user.")
  