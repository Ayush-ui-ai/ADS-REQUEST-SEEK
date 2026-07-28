import asyncio
import os
import logging
import sys
import threading
import re
from flask import Flask, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters, ContextTypes
)
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.account import UpdateStatusRequest
import aiosqlite
import nest_asyncio

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
nest_asyncio.apply()

# ---------- DATABASE ----------
DB_PATH = "bot_data.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('CREATE TABLE IF NOT EXISTS owners (user_id INTEGER PRIMARY KEY)')
        await db.execute('CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY, username TEXT)')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone_number TEXT UNIQUE,
                session_string TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                action TEXT,
                target TEXT,
                account_phone TEXT
            )
        ''')
        await db.commit()

async def get_owners():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT user_id FROM owners')
        rows = await cursor.fetchall()
        return [row[0] for row in rows] if rows else []

async def is_owner(user_id: int) -> bool:
    owners = await get_owners()
    return user_id in owners

async def add_owner(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR IGNORE INTO owners (user_id) VALUES (?)', (user_id,))
        await db.commit()
        logger.info(f"Owner added: {user_id}")

async def remove_owner(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM owners WHERE user_id = ?', (user_id,))
        await db.commit()
        logger.info(f"Owner removed: {user_id}")

async def is_authorized(user_id: int) -> bool:
    if await is_owner(user_id):
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT 1 FROM admins WHERE user_id = ?', (user_id,))
        return await cursor.fetchone() is not None

async def add_admin(user_id: int, username: str = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR IGNORE INTO admins (user_id, username) VALUES (?, ?)', (user_id, username))
        await db.commit()

async def remove_admin(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM admins WHERE user_id = ?', (user_id,))
        await db.commit()

async def list_admins():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT user_id, username FROM admins')
        return await cursor.fetchall()

async def add_account_db(phone: str, session_string: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO accounts (phone_number, session_string) VALUES (?, ?)', (phone, session_string))
        await db.commit()

async def get_all_accounts():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id, phone_number, session_string FROM accounts')
        return await cursor.fetchall()

async def log_activity(action: str, target: str, account_phone: str = "system"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO activity_log (action, target, account_phone) VALUES (?, ?, ?)',
                         (action, target, account_phone))
        await db.commit()

async def get_activity_log(limit=50):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT timestamp, action, target, account_phone FROM activity_log ORDER BY timestamp DESC LIMIT ?', (limit,))
        return await cursor.fetchall()

# ---------- ACCOUNT MANAGER ----------
class AccountManager:
    def __init__(self, api_id: int, api_hash: str):
        self.api_id = api_id
        self.api_hash = api_hash
        self.clients = {}
        self.online_tasks = {}

    async def _join_channel(self, client, link: str):
        try:
            match = re.search(r't\.me/\+(.+)', link)
            if match:
                invite_hash = match.group(1).split('_')[0]
                await client(ImportChatInviteRequest(invite_hash))
                return True, "✅ join request sent"
            else:
                username = link.strip("/").replace("https://t.me/", "").replace("http://t.me/", "")
                if not username:
                    return False, "invalid link"
                entity = await client.get_entity(username)
                await client(JoinChannelRequest(entity))
                return True, f"✅ joined @{username}"
        except errors.FloodWaitError as e:
            return False, f"⏳ flood wait {e.seconds}s"
        except Exception as e:
            err = str(e).lower()
            if "successfully requested" in err:
                return True, "✅ join request sent"
            return False, f"❌ {str(e)}"

    async def _keep_online_for_1hour(self, client, phone):
        try:
            await client(UpdateStatusRequest(offline=False))
            logger.info(f"🟢 {phone} forced online")
        except:
            pass
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < 3600:
            await asyncio.sleep(60)
            try:
                await client(UpdateStatusRequest(offline=False))
            except:
                pass
        try:
            await client(UpdateStatusRequest(offline=True))
            logger.info(f"⏰ {phone} offline after 1 hour")
        except:
            pass
        if phone in self.online_tasks:
            del self.online_tasks[phone]

    async def start_all_accounts(self):
        accounts = await get_all_accounts()
        for _, phone, session_str in accounts:
            await self._add_client(phone, session_str)

    async def _add_client(self, phone: str, session_str: str):
        client = TelegramClient(
            StringSession(session_str),
            self.api_id,
            self.api_hash,
            connection_retries=5,
            retry_delay=3,
            timeout=60,
            request_retries=3
        )
        try:
            await client.connect()
            if await client.is_user_authorized():
                self.clients[phone] = client
                logger.info(f"✅ {phone} connected (idle)")
            else:
                logger.warning(f"⚠️ {phone} not authorized – re-login needed")
        except Exception as e:
            logger.error(f"❌ {phone} connection error: {e}")
            await asyncio.sleep(5)
            try:
                await client.connect()
                self.clients[phone] = client
                logger.info(f"✅ {phone} reconnected (idle)")
            except Exception as e2:
                logger.error(f"❌ {phone} failed to reconnect: {e2}")

    async def add_new_account(self, phone: str, session_str: str):
        await add_account_db(phone, session_str)
        await self._add_client(phone, session_str)

    async def join_and_go_online(self, invite_link: str, delay: int, count: int, progress_callback=None):
        all_phones = list(self.clients.keys())
        if count > len(all_phones):
            return [f"❌ Only {len(all_phones)} accounts available."], []
        selected = all_phones[:count]

        for phone in selected:
            if phone in self.online_tasks:
                self.online_tasks[phone].cancel()
            task = asyncio.create_task(self._keep_online_for_1hour(self.clients[phone], phone))
            self.online_tasks[phone] = task

        results = []
        success = []
        total = len(selected)
        for idx, phone in enumerate(selected):
            client = self.clients[phone]
            ok, msg = await self._join_channel(client, invite_link)
            if ok:
                success.append(phone)
                await log_activity("JOIN", invite_link, phone)
                results.append(f"✅ {phone}: {msg}")
            else:
                results.append(f"❌ {phone}: {msg}")
            if progress_callback:
                await progress_callback(idx+1, total, len(success), idx+1 - len(success))
            if idx < total - 1 and delay > 0:
                await asyncio.sleep(delay)

        for phone in selected:
            results.append(f"🟢 {phone} is ONLINE for 1 hour (forced)")
        summary = f"\n📊 Delay: {delay}s | Requested: {count} | Joined: {len(success)}"
        results.append(summary)
        return results, success

    async def leave_specific(self, entity_input: str):
        results = []
        for phone, client in self.clients.items():
            try:
                entity = await client.get_entity(entity_input)
                await client(LeaveChannelRequest(entity))
                results.append(f"✅ {phone} left {entity_input}")
                await log_activity("LEAVE", entity_input, phone)
            except Exception as e:
                results.append(f"❌ {phone} error: {str(e)}")
        return results

    async def leave_all_channels(self):
        all_results = []
        for phone, client in self.clients.items():
            results = []
            try:
                dialogs = await client.get_dialogs()
                for dialog in dialogs:
                    if dialog.is_channel or dialog.is_group:
                        try:
                            await client(LeaveChannelRequest(dialog.entity))
                            results.append(f"✅ left {dialog.name}")
                            await log_activity("LEAVE_ALL", dialog.name, phone)
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            results.append(f"⚠️ could not leave {dialog.name}: {str(e)}")
                if results:
                    all_results.append(f"📱 {phone}:\n" + "\n".join(results))
                else:
                    all_results.append(f"📱 {phone}: no channels/groups to leave.")
            except Exception as e:
                all_results.append(f"❌ {phone} error: {str(e)}")
        return all_results

    async def get_active_sessions(self):
        return len(self.clients)

    async def get_accounts_list(self):
        return list(self.clients.keys())

    async def stop_all(self):
        for t in self.online_tasks.values():
            t.cancel()
        for c in self.clients.values():
            await c.disconnect()

# ---------- BOT CONFIG ----------
BOT_TOKEN = "8663763583:AAFBuGDqldhCx1YhY94od9ga-jXgW7edCOY"   # CHANGE
API_ID = 35992936
API_HASH = "3af1b42e13d465518d2f474289961c2c"
OWNER_ID = 6871652449  # Initial owner – you can add more later

account_manager = AccountManager(API_ID, API_HASH)

# Conversation states
LINK, DELAY, COUNT = range(3)
PHONE, CODE, PASSWORD = range(3, 6)

# ---------- TASK QUEUE ----------
task_queue = asyncio.Queue()
is_processing = False

# ---------- AUTHORIZATION ----------
def authorized_only(func):
    async def wrapper(update, context):
        if update.callback_query:
            try:
                await update.callback_query.answer()
            except:
                pass
        uid = update.effective_user.id
        if await is_authorized(uid):
            return await func(update, context)
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text("⛔ Unauthorized.")
            except:
                pass
        else:
            await update.message.reply_text("⛔ Unauthorized.")
        return
    return wrapper

def owner_only(func):
    async def wrapper(update, context):
        uid = update.effective_user.id
        if await is_owner(uid):
            return await func(update, context)
        await update.message.reply_text("⛔ Only owners can use this command.")
        return
    return wrapper

# ---------- HELPERS ----------
async def send_long_message(target, text):
    if not text:
        return
    if hasattr(target, 'message'):
        reply = target.message.reply_text
    elif hasattr(target, 'reply_text'):
        reply = target.reply_text
    else:
        return
    for i in range(0, len(text), 4000):
        await reply(text[i:i+4000])

async def update_progress_message(message, current, total, success, failed):
    percent = int((current / total) * 100) if total else 0
    bar_length = 20
    filled = int(bar_length * current / total) if total else 0
    bar = "█" * filled + "░" * (bar_length - filled)
    new_text = (
        f"🔄 **Processing Task...**\n"
        f"`[{bar}] {percent}%`\n\n"
        f"✅ Success: {success}\n"
        f"❌ Failed: {failed}\n"
        f"⚡ Accounts are being forced Online.\n"
        f"📌 Progress: {current}/{total}"
    )
    if message.text.strip() != new_text.strip():
        try:
            await message.edit_text(new_text, parse_mode="Markdown")
        except Exception:
            pass

async def process_queue():
    global is_processing
    is_processing = True
    while not task_queue.empty():
        update, link, delay, count, original_msg = await task_queue.get()
        try:
            progress_msg = await original_msg.reply_text("🔄 Starting join requests...")
            success_count = 0
            failed_count = 0
            current = 0

            async def progress_callback(cur, total, succ, fail):
                nonlocal current, success_count, failed_count
                current = cur
                success_count = succ
                failed_count = fail
                await update_progress_message(progress_msg, current, total, success_count, failed_count)

            result_list, success_phones = await account_manager.join_and_go_online(
                link, delay, count, progress_callback
            )
            full_text = "\n".join(result_list)
            await send_long_message(update, full_text)
        except Exception as e:
            try:
                await update.message.reply_text(f"❌ Task failed: {str(e)}")
            except:
                pass
    is_processing = False

# ---------- START WITH IMAGE ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Set initial owner if no owners exist
    owners = await get_owners()
    if not owners:
        await add_owner(OWNER_ID)
        await add_admin(OWNER_ID, "Owner")
        logger.info(f"Initial owner set to {OWNER_ID}")

    # Send image (change the URL to your own image)
    image_url = "https://i.ibb.co/Lhq8TGRh/IMG-20260604-113856-990.jpg"  # Replace with your image URL
    try:
        await update.message.reply_photo(
            photo=image_url,
            caption=" ****",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"Image send failed: {e}")
        await update.message.reply_text(" ****", parse_mode="Markdown")

    # Show menu if authorized
    if await is_authorized(update.effective_user.id):
        await main_menu(update, context)
    else:
        await update.message.reply_text("⛔ Unauthorized. Only owners/admins can use this bot.")

# ---------- MAIN MENU ----------
@authorized_only
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    is_own = await is_owner(uid)
    active = await account_manager.get_active_sessions()
    admins = await list_admins()
    admin_count = len(admins)

    status = (
        f"🤖 **Manager Bot Pro**\n"
        f"• Active Sessions: `{active}`\n"
        f"• Database: `Connected`\n"
        f"• Admins: `{admin_count}`\n"
        f"• Developer: `BLAZY NXT`"
    )
    if is_own:
        owners = await get_owners()
        owners_list = "\n".join([f"• `{oid}`" for oid in owners])
        admin_list = "\n".join([f"• `{aid}` ({uname or '?'})" for aid, uname in admins])
        status += f"\n👑 **Owners Panel**\n{owners_list}\n**Admins**\n{admin_list}\n/addowner <id> – /rmowner <id>\n/addadmin <id> – /rmadmin <id>"

    keyboard = [
        [InlineKeyboardButton("➕ Add New Account", callback_data="add_account")],
        [
            InlineKeyboardButton("🔗 Joiner Mode", callback_data="joiner_mode"),
            InlineKeyboardButton("🚪 Leaver Mode", callback_data="leaver_mode")
        ],
        [
            InlineKeyboardButton("📋 List Accounts", callback_data="list_accounts"),
            InlineKeyboardButton("📜 Activity Log", callback_data="activity_log")
        ],
        [
            InlineKeyboardButton("💬 Engagement", callback_data="engagement"),
            InlineKeyboardButton("⚡ Start Mass", callback_data="start_mass")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(status, reply_markup=reply_markup, parse_mode="Markdown")

# ---------- BUTTON HANDLER ----------
@authorized_only
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if data == "add_account":
        await query.message.reply_text("📱 Send phone number with country code:\nExample: +1234567890")
        return PHONE
    elif data == "joiner_mode":
        await query.message.reply_text(
            "**Step 1: Send Channel Link**\nExample: `https://t.me/+abc123` or `@username`",
            parse_mode="Markdown"
        )
        return LINK
    elif data == "leaver_mode":
        await query.message.reply_text("Send command:\n• `/leave <link>` – leave specific\n• `/leave` – leave **all**")
        return
    elif data == "list_accounts":
        accs = await account_manager.get_accounts_list()
        txt = "📱 Logged-in accounts:\n" + "\n".join(accs) if accs else "No accounts."
        await send_long_message(query.message, txt)
    elif data == "activity_log":
        logs = await get_activity_log(10)
        if logs:
            txt = "📜 Last 10 activities:\n" + "\n".join(f"{ts} | {action} | {target}" for ts, action, target, _ in logs)
        else:
            txt = "No activity yet."
        await send_long_message(query.message, txt)
    elif data == "engagement":
        await query.message.reply_text("💬 Engagement features coming soon.")
    elif data == "start_mass":
        await query.message.reply_text("⚡ Use **Joiner Mode** for mass join.")
    else:
        await query.message.reply_text("❌ Invalid option.")
    return

# ---------- JOINER MODE ----------
@authorized_only
async def get_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['link'] = update.message.text.strip()
    await update.message.reply_text("**Step 2: Set Delay**\nEnter delay in seconds (e.g., 10):")
    return DELAY

@authorized_only
async def get_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        delay = int(update.message.text.strip())
        if delay < 0:
            raise ValueError
        context.user_data['delay'] = delay
    except:
        await update.message.reply_text("❌ Invalid delay. Enter a positive number.")
        return DELAY
    await update.
