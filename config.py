import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "7909863011:AAFH7skqKlsI1KJUVK-92NNkRPfVR5LFOFI")
LOGGER_BOT_TOKEN = os.getenv("LOGGER_BOT_TOKEN", "8421844467:AAG5qrlh-371y1MQM9lEVlbdvhj4-ixXEs4")
BOT_USERNAME = os.getenv("BOT_USERNAME", "Brutodadsbot")
BOT_NAME = os.getenv("BOT_NAME", "BRUTOD ADS BOT [FREE]")
LOGGER_BOT_USERNAME = os.getenv("LOGGER_BOT_USERNAME", "BrutodAdsLoggerbot")

# Telegram API Configuration
BOT_API_ID = int(os.getenv("BOT_API_ID", "39951415"))  # Replace with your actual API ID
BOT_API_HASH = os.getenv("BOT_API_HASH", "147d2e3d7645ba87fb80c4815f1c5b98")  # Replace with your actual API hash

# Social Media & Contact Information
OWNER_USERNAME = "Brutodhere"
UPDATES_CHANNEL = "BrutodTg"
SUPPORT_USERNAME = "Brutodhere"

# URLs for social links
SUPPORT_GROUP_URL = "https://t.me/Brutodhere"
GUIDE_URL = "https://t.me/Brutodhere"

# Admin Configuration - Multiple admins supported
ADMIN_ID = 7013643940  # Primary admin user ID
ADMIN_IDS = [7013643940, 7533666153]  # Both admin IDs (primary + alt)

# OTP Configuration
OTP_EXPIRY = 300  # 5 minutes in seconds
ADMIN_USERNAME = "Brutodhere"
# Premium Settings
PREMIUM_CONTACT = "@Brutodhere"

# Image URLs #must change 
START_IMAGE = "https://i.postimg.cc/52b9hyxf/photo-2025-11-23-23-47-28.jpg" 
BROADCAST_IMAGE = "https://i.postimg.cc/52b9hyxf/photo-2025-11-23-23-47-28.jpg"
FORCE_JOIN_IMAGE = "https://i.postimg.cc/52b9hyxf/photo-2025-11-23-23-47-28.jpg"

# Force Join Settings
ENABLE_FORCE_JOIN = True
MUST_JOIN_CHANNEL_ID = -1003281138527 # Updated to actual channel ID
MUSTJOIN_GROUP_ID = -1002933967876    # Updated to actual group ID
MUST_JOIN_CHANNEL_URL = "https://t.me/BrutodTg"  # Channel invite link or can be use public link t.me/{username} 
MUSTJOIN_GROUP_URL = "https://t.me/theforeplay"    # Group invite link

# Channel and Group IDs
SETUP_GROUP_ID = -1003281138527
TECH_LOG_CHANNEL_ID = -1003281138527
GROUP_ID = -1002933967876 # Private group chat ID for ad skipping

SUPPORT_GROUP_URL = "https://t.me/Brutodhere"
UPDATES_CHANNEL_URL = "https://t.me/BrutodTg"
GUIDE_URL = "https://t.me/BrutodTg"  # guide channel bna k usme video upload krne k baad uska link change krdena 
PRIVATE_CHANNEL_INVITE = "https://t.me/Brutodhere"
# Encryption Key (use env var in production; fallback kept for local dev)
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "RnVa0xtPfK1pm3qu_POAvFI9qkSyISKFShE37_JSQ2w=")

# Database Configuration
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://brutod:adpmz786@cluster0.yjtb15t.mongodb.net/?retryWrites=true&w=majority"
)
DB_NAME = "AdsBot_db"

# Broadcast Settings
DEFAULT_DELAY = 300
MIN_DELAY = 60
MAX_DELAY = 3600

# OTP Settings
OTP_LENGTH = 5
OTP_EXPIRY = 300

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FILE = "logs/BrutodAdsBot.log"

# Feature Toggles
ENABLE_FORCE_JOIN = True
ENABLE_OTP_VERIFICATION = True
ENABLE_BROADCASTING = True
ENABLE_ANALYTICS = True

# Success Messages
SUCCESS_MESSAGES = {
    "account_added": "Account added successfully!",
    "otp_sent": "OTP sent to your phone number!",
    "broadcast_started": "Broadcast started successfully!",
    "broadcast_completed": "Broadcast completed successfully!",
    "accounts_deleted": "All accounts deleted successfully!"  # Added for delete all accounts
}

# Error Messages
ERROR_MESSAGES = {
    "account_limit": "Adding accounts is a PREMIUM feature! Free users cannot add accounts. Contact @Brutodhere to upgrade.",
    "invalid_phone": "Invalid phone number format! Use +1234567890",
    "otp_expired": "OTP has expired. Please restart hosting.",
    "invalid_otp": "Invalid OTP. Please try again.",
    "login_failed": "Failed to login to Telegram account!",
    "no_groups": "No groups found in your account!",
    "no_messages": "No messages found in Saved Messages!",
    "broadcast_limit": "Daily broadcast limit reached! Get higher limit contact @Brutodhere",
    "unauthorized": "You are not authorized to perform this action!",
    "force_join_required": "Join required channels to access this feature!"
}

# Session Storage
SESSION_STORAGE_PATH = "sessions/"
