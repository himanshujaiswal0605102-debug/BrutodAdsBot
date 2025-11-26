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
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import os
import logging
from cryptography.fernet import Fernet
import sys
import io
import time

=======================================================

✅ FORCE UTF-8 OUTPUT

=======================================================

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="ignore")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="ignore")
os.environ["PYTHONIOENCODING"] = "utf-8"

=======================================================

🧱 LOGGING SETUP

=======================================================

os.makedirs('logs', exist_ok=True)
logging.basicConfig(
level=logging.INFO,
format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
handlers=[logging.FileHandler('logs/Brutod_bot.log', encoding='utf-8'),
logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(name)

=======================================================

🔐 ENCRYPTION SETUP

=======================================================

ENCRYPTION_KEY_FILE = 'encryption.key'
if os.path.exists(ENCRYPTION_KEY_FILE):
with open(ENCRYPTION_KEY_FILE, 'r', encoding='utf-8') as f:
ENCRYPTION_KEY = f.read().strip()
else:
ENCRYPTION_KEY = Fernet.generate_key().decode()
with open(ENCRYPTION_KEY_FILE, 'w', encoding='utf-8') as f:
f.write(ENCRYPTION_KEY)
cipher_suite = Fernet(ENCRYPTION_KEY.encode())

=======================================================

🧩 DUAL ACCOUNT MANAGEMENT

=======================================================

class DualAccountManager:
"""Manage multiple Telegram accounts for broadcasting"""
def init(self):
self.accounts = {}  # {1: client1, 2: client2}
self.account_order = []  # maintain order of login for cycles

async def add_account(self, phone_number, api_id, api_hash):  
    """Add a new account with phone number + OTP verification"""  
    account_id = len(self.accounts) + 1  
    client = TelegramClient(StringSession(), api_id, api_hash)  

    await client.connect()  
    try:  
        if not await client.is_user_authorized():  
            await client.send_code_request(phone_number)  
            print(f"📩 OTP sent to {phone_number}")  
            code = input(f"Enter OTP for account {account_id}: ")  
            try:  
                await client.sign_in(phone_number, code)  
            except SessionPasswordNeededError:  
                password = input(f"Two-step verification password for account {account_id}: ")  
                await client.sign_in(password=password)  
          
        # Store client & ID  
        self.accounts[account_id] = client  
        self.account_order.append(account_id)  
        print(f"✅ Account {account_id} logged in successfully!")  
        return account_id  
    except Exception as e:  
        print(f"❌ Failed to login account {account_id}: {e}")  
        await client.disconnect()  
        return None  

def get_accounts(self):  
    """Return all logged-in accounts"""  
    return self.accounts  

def get_ordered_accounts(self):  
    """Return account IDs in login order"""  
    return self.account_order

=======================================================

🧠 ACCOUNT HEALTH MONITOR

=======================================================

class AccountHealthMonitor:
"""Continuously monitors accounts for bans/freezes and auto-removes them"""
def init(self):
self.monitoring = {}
self.banned_accounts = set()

async def check_account_status(self, client, account_id):  
    try:  
        me = await client.get_me()  
        if me:  
            return True, "Active"  
    except UserDeactivatedBanError:  
        return False, "Account is banned/deactivated"  
    except AuthKeyUnregisteredError:  
        return False, "Account session expired"  
    except Exception as e:  
        err_str = str(e).lower()  
        if any(word in err_str for word in ['ban', 'deactivat', 'deleted', 'invalid']):  
            return False, f"Account issue: {str(e)[:50]}"  
        return True, "Unknown status"  

async def monitor_account(self, client, account_id):  
    while True:  
        try:  
            await asyncio.sleep(60)  
            is_active, status = await self.check_account_status(client, account_id)  
            if not is_active:  
                self.banned_accounts.add(account_id)  
                print(f"🚨 Account {account_id} banned/frozen: {status}")  
                break  
        except asyncio.CancelledError:  
            break  
        except Exception as e:  
            logger.error(f"Error monitoring account {account_id}: {e}")  
            await asyncio.sleep(30)  

def start_monitoring(self, account_id):  
    self.monitoring[account_id] = True  

def stop_monitoring(self, account_id):  
    self.monitoring[account_id] = False  

def is_account_banned(self, account_id):  
    return account_id in self.banned_accounts

Global instances

account_manager = DualAccountManager()
account_monitor = AccountHealthMonitor()

print("✅ Dual account management initialized. Ready for login.")
print("📌 Add your accounts using: await account_manager.add_account(phone, api_id, api_hash)")# =======================================================

🗄️ SAVED MESSAGES MANAGEMENT

=======================================================

class SavedMessageManager:
"""Manage messages saved for broadcasting"""
def init(self):
self.saved_messages = []  # List of message strings

def add_message(self, message_text):  
    self.saved_messages.append(message_text)  

def get_all_messages(self):  
    return self.saved_messages  

def count(self):  
    return len(self.saved_messages)

saved_messages = SavedMessageManager()

=======================================================

🔄 BROADCAST CYCLE LOGIC

=======================================================

class Broadcaster:
"""Send saved messages to all target groups using multiple accounts"""
def init(self, accounts, saved_messages, delay_between=2):
self.accounts = accounts  # {1: client1, 2: client2}
self.saved_messages = saved_messages.get_all_messages()
self.delay = delay_between
self.cycle_index = 0  # Track which account to use next

async def send_message_cycle(self, groups):  
    """  
    Send messages in cycle:  
    - 3 messages from account1  
    - 3 messages from account2  
    - Repeat  
    """  
    if not self.saved_messages or not groups:  
        print("❌ No messages or groups to broadcast")  
        return  

    total_msgs = len(self.saved_messages)  
    total_accounts = len(self.accounts)  
    msg_index = 0  

    while True:  
        # Determine which account to use for this cycle  
        account_id = self.cycle_index % total_accounts + 1  
        client = self.accounts[account_id]  

        # Send 3 messages per cycle from selected account  
        for i in range(3):  
            if msg_index >= total_msgs:  
                msg_index = 0  # Reset message index if reached end  

            message_text = self.saved_messages[msg_index]  

            # Send to all groups  
            for group in groups:  
                try:  
                    await client.send_message(group, message_text)  
                    print(f"✅ Sent msg {msg_index+1} via Account({account_id}) to group {group}")  
                except Exception as e:  
                    print(f"❌ Failed to send msg {msg_index+1} via Account({account_id}) to group {group}: {e}")  

            msg_index += 1  
            await asyncio.sleep(self.delay)  

        # Move to next account for next cycle  
        self.cycle_index += 1

=======================================================

🔄 SAMPLE USAGE FUNCTION

=======================================================

async def start_broadcast_example():
"""Example: login 2 accounts, add messages, start broadcasting"""
# --- Accounts login example ---
acc1_id = await account_manager.add_account("PHONE_1", API_ID_1, API_HASH_1)
acc2_id = await account_manager.add_account("PHONE_2", API_ID_2, API_HASH_2)

accounts_dict = account_manager.get_accounts()  

# --- Add saved messages ---  
saved_messages.add_message("Hello, this is message 1")  
saved_messages.add_message("Hello, this is message 2")  
saved_messages.add_message("Hello, this is message 3")  
saved_messages.add_message("Hello, this is message 4")  
saved_messages.add_message("Hello, this is message 5")  
saved_messages.add_message("Hello, this is message 6")  
saved_messages.add_message("Hello, this is message 7")  

# --- Groups to broadcast (replace with actual IDs) ---  
target_groups = [-1001234567890, -1009876543210]  # Example group IDs  

# --- Start broadcaster ---  
broadcaster = Broadcaster(accounts_dict, saved_messages, delay_between=2)  
await broadcaster.send_message_cycle(target_groups)

=======================================================

🗂️ GROUPS FETCHING & BACKGROUND PRELOAD

=======================================================

GROUPS_CACHE = {}  # {user_id: [group_id1, group_id2, ...]}
QUICK_CACHE = {}   # instant access cache
PRELOAD_TASKS = {}

async def fetch_user_groups(client):
"""Fetch all groups the account is part of"""
groups = []
try:
async for dialog in client.iter_dialogs():
if dialog.is_group:
groups.append(dialog.id)
return groups
except Exception as e:
print(f"❌ Failed to fetch groups: {e}")
return []

async def preload_all_groups(accounts_dict):
"""Fetch groups for all accounts in background"""
tasks = []
for acc_id, client in accounts_dict.items():
tasks.append(fetch_user_groups(client))
results = await asyncio.gather(*tasks)

# Merge groups and store in cache  
all_groups = set()  
for group_list in results:  
    all_groups.update(group_list)  
GROUPS_CACHE['all'] = list(all_groups)  
print(f"✅ Preloaded {len(all_groups)} groups from all accounts")  
return GROUPS_CACHE['all']

=======================================================

🔁 AUTO RECONNECT & BAN DETECTION

=======================================================

async def monitor_accounts_loop(accounts_dict):
"""Continuously monitor accounts for bans/disconnects"""
while True:
for acc_id, client in accounts_dict.items():
try:
is_active, status = await account_monitor.check_account_status(client, acc_id)
if not is_active:
print(f"🚨 Account {acc_id} issue: {status}. Disconnecting...")
await client.disconnect()
account_monitor.banned_accounts.add(acc_id)
except Exception as e:
print(f"⚠️ Monitoring error for account {acc_id}: {e}")
await asyncio.sleep(60)

=======================================================

🏁 RUN MAIN BOT

=======================================================

async def main():
"""Full bot startup: login, preload groups, broadcast"""
# --- LOGIN ACCOUNTS ---
acc1_id = await account_manager.add_account("PHONE_1", API_ID_1, API_HASH_1)
acc2_id = await account_manager.add_account("PHONE_2", API_ID_2, API_HASH_2)
accounts_dict = account_manager.get_accounts()

# --- ADD SAVED MESSAGES ---  
saved_messages.add_message("Hello, message 1")  
saved_messages.add_message("Hello, message 2")  
saved_messages.add_message("Hello, message 3")  
saved_messages.add_message("Hello, message 4")  
saved_messages.add_message("Hello, message 5")  
saved_messages.add_message("Hello, message 6")  
saved_messages.add_message("Hello, message 7")  

# --- PRELOAD GROUPS ---  
all_groups = await preload_all_groups(accounts_dict)  

# --- START ACCOUNT MONITOR LOOP ---  
asyncio.create_task(monitor_accounts_loop(accounts_dict))  

# --- START BROADCAST ---  
broadcaster = Broadcaster(accounts_dict, saved_messages, delay_between=2)  
await broadcaster.send_message_cycle(all_groups)

=======================================================

⚡ START BOT

=======================================================

if name == "main":
try:
asyncio.run(main())
except KeyboardInterrupt:
print("🛑 Bot stopped by user")

Now check tumne jo deya vo shi paste keya hai ?