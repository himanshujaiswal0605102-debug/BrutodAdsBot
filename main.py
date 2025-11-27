# =======================================================
# 👤 ACCOUNT LOGIN & MANAGEMENT UTILITY
# =======================================================

class AccountLoginUtility:
    """Handles the Telethon login process (phone, code, password) and session saving."""
    
    def __init__(self, db_manager, cipher_suite):
        self.db = db_manager
        self.cipher = cipher_suite

    async def start_login_flow(self, user_id, api_id, api_hash, phone_number):
        """Starts the Telethon login sequence."""
        try:
            # Use a temporary file for the session until successful
            temp_session_file = tempfile.NamedTemporaryFile(delete=True)
            temp_client = TelegramClient(temp_session_file.name, api_id, api_hash)
            
            await temp_client.connect()
            
            # Send code
            code_request = await temp_client.send_code_request(phone_number)
            
            return {
                'client': temp_client,
                'phone_code_hash': code_request.phone_code_hash,
                'status': 'code_sent'
            }
            
        except PhoneNumberInvalidError:
            raise ValueError("❌ Invalid phone number format.")
        except FloodWaitError as e:
            raise RuntimeError(f"⚠️ Flood Wait: Please try again after {e.seconds} seconds.")
        except Exception as e:
            await self.cleanup_temp_client(temp_client)
            raise RuntimeError(f"❌ Login initiation failed: {e}")

    async def verify_login_code(self, client, phone_number, phone_code_hash, phone_code):
        """Verifies the OTP code."""
        try:
            await client.sign_in(phone=phone_number, code=phone_code, phone_code_hash=phone_code_hash)
            return {'status': 'success'}
        except PhoneCodeInvalidError:
            raise ValueError("❌ Invalid verification code.")
        except PhoneCodeExpiredError:
            raise ValueError("❌ Verification code expired.")
        except SessionPasswordNeededError:
            return {'status': 'password_needed'}
        except Exception as e:
            await self.cleanup_temp_client(client)
            raise RuntimeError(f"❌ Login verification failed: {e}")

    async def verify_login_password(self, client, password):
        """Verifies the 2FA password."""
        try:
            await client.sign_in(password=password)
            return {'status': 'success'}
        except PasswordHashInvalidError:
            raise ValueError("❌ Invalid 2FA password.")
        except Exception as e:
            await self.cleanup_temp_client(client)
            raise RuntimeError(f"❌ 2FA login failed: {e}")

    async def save_session(self, user_id, client, phone_number):
        """Saves the encrypted session string to the database."""
        try:
            session_string = client.session.save()
            encrypted_session = self.cipher.encrypt(session_string.encode()).decode()
            
            me = await client.get_me()
            
            # Fetch current account count to assign the index (1), (2), etc.
            current_count = self.db.db.accounts.count_documents({'user_id': user_id})
            account_index = current_count + 1 
            
            self.db.db.accounts.insert_one({
                'user_id': user_id,
                'session_string': encrypted_session,
                'phone_number': phone_number,
                'added_on': datetime.now(),
                'status': 'active',
                'telegram_id': me.id,
                'account_index': account_index # New field to track (1), (2)
            })
            
            return f"✅ Account added successfully! Your account index is ({account_index})"
        
        except Exception as e:
            raise RuntimeError(f"❌ Failed to save account session: {e}")
        finally:
            await self.cleanup_temp_client(client)

    async def cleanup_temp_client(self, client):
        """Ensure client is disconnected."""
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass

# Initialize the new utility class
account_login_utility = AccountLoginUtility(db, cipher_suite)
# =======================================================
# 🔄 MULTI-ACCOUNT BOT HANDLER & LOADER
# =======================================================

async def get_account_clients(user_id):
    """
    Load all active accounts for a user and return a list of (client, index) tuples.
    We use telethon for session handling and pyrogram for broadcasting.
    """
    accounts = db.get_user_accounts(user_id) # Assumes db.get_user_accounts() exists and is functional
    if not accounts:
        return []
    
    # Sort by account_index for consistent (1), (2) display
    sorted_accounts = sorted(accounts, key=lambda x: x.get('account_index', 999))
    
    client_list = []
    credentials = db.get_user_api_credentials(user_id)
    if not credentials:
        logger.error(f"No API credentials found for user {user_id}")
        return []

    api_id = credentials['api_id']
    api_hash = credentials['api_hash']
    
    for account in sorted_accounts:
        acc_id = account['_id']
        acc_index = account.get('account_index', 0)
        
        if account_monitor.is_account_banned(acc_id):
            logger.warning(f"Skipping banned account {acc_id} for user {user_id}")
            continue

        try:
            # Decrypt the session string
            session_str = cipher_suite.decrypt(account['session_string'].encode()).decode()
            
            # Use Pyrogram Client for broadcasting (more robust)
            client = PyroClient(
                name=str(acc_id), # Unique ID for Pyrogram's session file
                session_string=session_str,
                api_id=api_id,
                api_hash=api_hash
            )
            
            # Start the client (needs to be awaited)
            await client.start()
            
            # Store the Pyrogram client, its DB ID, and its assigned index
            client_list.append({
                'client': client,
                'db_id': acc_id,
                'index': acc_index,
                'phone': account.get('phone_number', 'N/A')
            })
            
            # Start health monitoring for this account (using Telethon for monitoring)
            # This part needs Telethon Client, so let's adjust or assume health check is done externally/periodically.
            # For simplicity, we stick to Pyrogram for broadcasting.
            
        except InvalidToken:
            logger.error(f"Decryption failed for account {acc_id}. Key mismatch or corrupted data.")
            await account_monitor.remove_banned_account(user_id, acc_id, "Session String Corrupted/Invalid Token")
        except RPCError as e:
            logger.error(f"RPC Error starting client {acc_id}: {e}")
            await account_monitor.remove_banned_account(user_id, acc_id, f"RPC Error: {e}")
        except Exception as e:
            logger.error(f"Unknown error starting client {acc_id}: {e}")

    return client_list

async def format_account_status(user_id):
    """Formats the status list for the user, showing (1), (2), etc."""
    accounts = db.get_user_accounts(user_id)
    if not accounts:
        return "<i>No accounts added yet.</i>"
    
    # Sort by index
    sorted_accounts = sorted(accounts, key=lambda x: x.get('account_index', 999))
    
    status_text = "<b>📚 Current Accounts:</b>\n"
    for acc in sorted_accounts:
        index = acc.get('account_index', '?')
        phone = acc.get('phone_number', 'Unknown Phone')
        status_text += f" • <b>({index})</b> | <code>{phone}</code> | Status: 🟢 Active\n"
        
    return status_text

# OVERRIDE/Enhance db.get_user_accounts to ensure sorting by index is possible
def get_user_accounts_enhanced(self, user_id):
    """Retrieves all accounts for a user, sorted by account_index."""
    return list(self.db.accounts.find({'user_id': user_id}).sort('account_index', 1))

# Dynamically apply the enhanced function to the database manager
setattr(db.__class__, 'get_user_accounts', get_user_accounts_enhanced)
# =======================================================
# ⚙️ ADVANCED BROADCAST CYCLING LOGIC
# =======================================================

# Configuration
MESSAGES_PER_ACCOUNT = 3 

# Global or User-Specific State Tracking (Isse Database ya Redis mein store karna best hai, 
# lekin abhi hum memory mein simple rakhte hain, agar bot restart na ho to.)
BROADCAST_STATE = {} # Key: user_id, Value: {'current_account_index': 0, 'current_msg_count': 0}

async def start_broadcast_cycle(user_id, saved_messages, target_groups):
    """
    Handles the broadcast using multiple accounts in a round-robin cycle (3 messages per account).
    """
    
    # 1. Load Accounts
    all_clients = await get_account_clients(user_id)
    if len(all_clients) < 1:
        logger.error(f"No active accounts found for user {user_id}. Stopping broadcast.")
        return
        
    num_accounts = len(all_clients)
    
    # 2. Initialize State
    if user_id not in BROADCAST_STATE:
        BROADCAST_STATE[user_id] = {'current_account_index': 0, 'current_msg_count': 0}

    state = BROADCAST_STATE[user_id]
    
    # 3. Main Broadcast Loop
    total_messages_sent = 0
    
    for message in saved_messages: # saved_messages is a list of pyrogram Message objects or similar
        
        # Determine the current account to use
        current_client_info = all_clients[state['current_account_index']]
        client = current_client_info['client']
        acc_index = current_client_info['index']
        
        # 4. SEND MESSAGE (This is where your existing send logic goes)
        try:
            # --- REPLACE THIS SECTION WITH YOUR ACTUAL MESSAGE SENDING LOGIC ---
            # Example: Send the message to all target groups using the current 'client'
            
            logger.info(f"User {user_id} - Sending Message to {len(target_groups)} groups with Account ({acc_index})...")
            
            # A stub for sending: (assuming 'message' has a send_copy method or similar)
            # await client.send_copy(chat_id=target_groups[0], from_chat_id=config.SAVED_MESSAGES_CHAT_ID, message_id=message.id)
            # time.sleep(random.uniform(1, 3)) # Add delay to prevent flood
            # -------------------------------------------------------------------
            
            total_messages_sent += 1
            state['current_msg_count'] += 1
            
            logger.info(f"Sent message {total_messages_sent} with Account ({acc_index}). Account count: {state['current_msg_count']}/{MESSAGES_PER_ACCOUNT}")
            
        except Exception as e:
            logger.error(f"Broadcast failed for message by Account ({acc_index}): {e}")
            # Consider removing account if it's a ban/flood error
            pass
            
        # 5. Cycle Logic (Switch account after MESSAGES_PER_ACCOUNT messages)
        if state['current_msg_count'] >= MESSAGES_PER_ACCOUNT:
            # Reset message count
            state['current_msg_count'] = 0
            
            # Move to the next account (Round-Robin)
            state['current_account_index'] = (state['current_account_index'] + 1) % num_accounts
            
            logger.info(f"--- Switching to Account ({all_clients[state['current_account_index']]['index']}) ---")
            
            # Add a small delay after switching accounts
            await asyncio.sleep(2) 

    # Update Global State
    BROADCAST_STATE[user_id] = state
    logger.info(f"Broadcast cycle finished for user {user_id}. Total messages sent: {total_messages_sent}.")
    
    # Don't forget to stop all Pyrogram clients after the broadcast is truly over to release resources
    for client_info in all_clients:
        try:
            await client_info['client'].stop()
        except Exception:
            pass