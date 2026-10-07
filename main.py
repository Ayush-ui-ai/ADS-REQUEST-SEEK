import asyncio
import os
import logging
import sys
import threading
import re
import time
import json
import http.server
import socketserver
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, SendReactionRequest
from telethon.tl.functions.account import UpdateStatusRequest
from telethon.tl.types import ReactionEmoji
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

# ---------- CONFIG ----------
BOT_TOKEN = "8663763583:AAFBuGDqldhCx1YhY94od9ga-jXgW7edCOY"
API_ID = 35598561
API_HASH = "8f359688b1c446a45023045d9656ea37"
OWNER_ID = 6871652449
PORT = int(os.environ.get("PORT", 8080))

# ---------- HEALTH CHECK SERVER ----------
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "alive"}).encode())
    def log_message(self, format, *args):
        pass

def run_health_server(port):
    try:
        with socketserver.TCPServer(("0.0.0.0", port), HealthHandler) as httpd:
            logger.info(f"Health server on port {port}")
            httpd.serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}")

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
        # NEW: user state table (for button flow)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS user_state (
                user_id INTEGER PRIMARY KEY,
                state TEXT,
                data TEXT
            )
        ''')
        await db.commit()

# ---------- STATE MANAGEMENT (DATABASE) ----------
async def set_state(user_id, state, data=None):
    async with aiosqlite.connect(DB_PATH) as db:
        data_str = json.dumps(data) if data else None
        await db.execute(
            'INSERT OR REPLACE INTO user_state (user_id, state, data) VALUES (?, ?, ?)',
            (user_id, state, data_str)
        )
        await db.commit()

async def get_state(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT state, data FROM user_state WHERE user_id = ?', (user_id,))
        row = await cursor.fetchone()
        if row:
            return row[0], (json.loads(row[1]) if row[1] else {})
        return None, {}

async def clear_state(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM user_state WHERE user_id = ?', (user_id,))
        await db.commit()

# ---------- DB HELPERS ----------
async def get_owners():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT user_id FROM owners')
        rows = await cursor.fetchall()
        return [row[0] for row in rows] if rows else []

async def is_owner(user_id):
    return user_id in await get_owners()

async def add_owner(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR IGNORE INTO owners (user_id) VALUES (?)', (user_id,))
        await db.commit()

async def remove_owner(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM owners WHERE user_id = ?', (user_id,))
        await db.commit()

async def is_authorized(user_id):
    if await is_owner(user_id):
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT 1 FROM admins WHERE user_id = ?', (user_id,))
        return await cursor.fetchone() is not None

async def add_admin(user_id, username=None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR IGNORE INTO admins (user_id, username) VALUES (?, ?)', (user_id, username))
        await db.commit()

async def remove_admin(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM admins WHERE user_id = ?', (user_id,))
        await db.commit()

async def list_admins():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT user_id, username FROM admins')
        return await cursor.fetchall()

async def add_account_db(phone, session_string):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO accounts (phone_number, session_string) VALUES (?, ?)', (phone, session_string))
        await db.commit()

async def get_all_accounts():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id, phone_number, session_string FROM accounts')
        return await cursor.fetchall()

async def log_activity(action, target, account_phone="system"):
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
    def __init__(self, api_id, api_hash):
        self.api_id = api_id
        self.api_hash = api_hash
        self.clients = {}
        self.online_tasks = {}

    async def _join_channel(self, client, link):
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
            if "authorization key" in err and "different ip" in err:
                return False, "❌ Session invalid. Re-add account."
            return False, f"❌ {str(e)}"

    async def _keep_online_for_1hour(self, client, phone):
        try:
            await client(UpdateStatusRequest(offline=False))
            logger.info(f"🟢 {phone} online")
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
            logger.info(f"⏰ {phone} offline")
        except:
            pass
        if phone in self.online_tasks:
            del self.online_tasks[phone]

    async def start_all_accounts(self):
        accounts = await get_all_accounts()
        for _, phone, session_str in accounts:
            await self._add_client(phone, session_str)

    async def _add_client(self, phone, session_str):
        client = TelegramClient(StringSession(session_str), self.api_id, self.api_hash,
                                connection_retries=5, retry_delay=3, timeout=60, request_retries=3)
        try:
            await client.connect()
            if await client.is_user_authorized():
                self.clients[phone] = client
                logger.info(f"✅ {phone} connected")
            else:
                logger.warning(f"⚠️ {phone} not authorized")
        except Exception as e:
            logger.error(f"❌ {phone} error: {e}")

    async def add_new_account(self, phone, session_str):
        await add_account_db(phone, session_str)
        await self._add_client(phone, session_str)

    async def join_and_go_online(self, invite_link, delay, count, progress_callback=None):
        all_phones = list(self.clients.keys())
        if count > len(all_phones):
            return [f"❌ Only {len(all_phones)} accounts."], []
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
            results.append(f"🟢 {phone} ONLINE for 1 hour")
        results.append(f"\n📊 Requested: {count} | Joined: {len(success)}")
        return results, success

    async def leave_specific(self, entity_input):
        results = []
        for phone, client in self.clients.items():
            try:
                entity = await client.get_entity(entity_input)
                await client(LeaveChannelRequest(entity))
                results.append(f"✅ {phone} left")
            except Exception as e:
                results.append(f"❌ {phone}: {str(e)}")
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
                        except:
                            pass
                all_results.append(f"📱 {phone}:\n" + ("\n".join(results) if results else "nothing to leave."))
            except Exception as e:
                all_results.append(f"❌ {phone}: {str(e)}")
        return all_results

    async def get_active_sessions(self):
        return len(self.clients)

    async def get_accounts_list(self):
        return list(self.clients.keys())

account_manager = AccountManager(API_ID, API_HASH)

# Task queue
task_queue = asyncio.Queue()
is_processing = False
reaction_queue = asyncio.Queue()
is_reaction_processing = False

# ---------- AUTHORIZATION DECORATORS ----------
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
        await update.message.reply_text("⛔ Owner only.")
        return
    return wrapper

# ---------- HELPERS ----------
async def send_long_message(target, text):
    if not text:
        return
    reply = target.message.reply_text if hasattr(target, 'message') else target.reply_text
    for i in range(0, len(text), 4000):
        await reply(text[i:i+4000])

async def update_progress_message(message, current, total, success, failed):
    percent = int((current / total) * 100) if total else 0
    filled = int(20 * current / total) if total else 0
    bar = "█" * filled + "░" * (20 - filled)
    new_text = (
        f"🔄 **Processing...**\n"
        f"`[{bar}] {percent}%`\n\n"
        f"✅ Success: {success}\n"
        f"❌ Failed: {failed}\n"
        f"📌 Progress: {current}/{total}"
    )
    if message.text.strip() != new_text.strip():
        try:
            await message.edit_text(new_text, parse_mode="Markdown")
        except:
            pass

# ---------- PROCESSORS ----------
async def process_join_queue():
    global is_processing
    is_processing = True
    while not task_queue.empty():
        update, link, delay, count, original_msg = await task_queue.get()
        try:
            progress_msg = await original_msg.reply_text("🔄 Starting join...")
            async def cb(cur, total, succ, fail):
                await update_progress_message(progress_msg, cur, total, succ, fail)
            result_list, _ = await account_manager.join_and_go_online(link, delay, count, cb)
            await send_long_message(update, "\n".join(result_list))
        except Exception as e:
            try:
                await update.message.reply_text(f"❌ Failed: {str(e)}")
            except:
                pass
    is_processing = False

async def process_reaction_queue():
    global is_reaction_processing
    is_reaction_processing = True
    while not reaction_queue.empty():
        update, post_link, emoji, original_msg = await reaction_queue.get()
        try:
            match = re.search(r'https://t\.me/(c/)?([^/]+)/(\d+)', post_link)
            if not match:
                await original_msg.reply_text("❌ Invalid link.")
                continue
            channel_part = match.group(2)
            message_id = int(match.group(3))
            is_private = bool(match.group(1))
            if is_private:
                try:
                    channel_id = int(channel_part)
                except:
                    await original_msg.reply_text("❌ Invalid channel ID.")
                    continue
                channel_ids = [channel_id, -1000000000000 - channel_id, -100 + channel_id]
            else:
                channel_ids = [channel_part]

            progress_msg = await original_msg.reply_text(f"🔄 Adding {emoji}...")
            total = len(account_manager.clients)
            success = 0
            failed = 0
            current = 0
            for phone, client in account_manager.clients.items():
                current += 1
                entity = None
                for cid in channel_ids:
                    try:
                        entity = await client.get_entity(cid)
                        break
                    except:
                        continue
                if entity is None:
                    failed += 1
                    continue
                try:
                    await client(SendReactionRequest(peer=entity, msg_id=message_id,
                                                     reaction=[ReactionEmoji(emoticon=emoji)]))
                    success += 1
                except Exception as e:
                    failed += 1
                await asyncio.sleep(0.5)
                if current % 5 == 0 or current == total:
                    await update_progress_message(progress_msg, current, total, success, failed)
            await original_msg.reply_text(
                f"✅ **Done**\nTotal: {total}\nSuccess: {success}\nFailed: {failed}\nEmoji: {emoji}"
            )
        except Exception as e:
            try:
                await original_msg.reply_text(f"❌ {e}")
            except:
                pass
    is_reaction_processing = False

# ---------- START ----------
async def start(update, context):
    if not await get_owners():
        await add_owner(OWNER_ID)
        await add_admin(OWNER_ID, "Owner")
    try:
        await update.message.reply_photo(
            photo="https://i.ibb.co/kgm1fPh7/IMG-20260604-113856-990.jpg",
            caption="🔥 **AUTO REQUEST TOOLS**",
            parse_mode="Markdown"
        )
    except:
        await update.message.reply_text("🔥 **AUTO REQUEST TOOLS**", parse_mode="Markdown")
    if await is_authorized(update.effective_user.id):
        await main_menu(update, context)
    else:
        await update.message.reply_text("⛔ Unauthorized.")

@authorized_only
async def main_menu(update, context):
    uid = update.effective_user.id
    is_own = await is_owner(uid)
    active = await account_manager.get_active_sessions()
    admins = await list_admins()

    status = (
        f"🤖 **Manager Bot Pro**\n"
        f"• Active Sessions: `{active}`\n"
        f"• Database: `Connected`\n"
        f"• Admins: `{len(admins)}`\n"
        f"• Developer: `𓆩𝙎𝙃𝘼𝘿𝙊𝙒 𝙉𝙀𝙏𝙒𝙊𝙍𝙆𓆪🫆`"
    )
    if is_own:
        owners = await get_owners()
        owners_list = "\n".join([f"• `{o}`" for o in owners])
        admin_list = "\n".join([f"• `{a}` ({u or '?'})" for a, u in admins])
        status += f"\n👑 **Owners**\n{owners_list}\n**Admins**\n{admin_list}\n/addowner <id>\n/addadmin <id>"

    keyboard = [
        [InlineKeyboardButton("➕ Add New Account", callback_data="add_account")],
        [InlineKeyboardButton("🔗 Joiner Mode", callback_data="joiner_mode"),
         InlineKeyboardButton("🚪 Leaver Mode", callback_data="leaver_mode")],
        [InlineKeyboardButton("📋 List Accounts", callback_data="list_accounts"),
         InlineKeyboardButton("📜 Activity Log", callback_data="activity_log")],
        [InlineKeyboardButton("💬 Engagement", callback_data="engagement"),
         InlineKeyboardButton("⚡ Start Mass", callback_data="start_mass")],
        [InlineKeyboardButton("🎯 React to Post", callback_data="reaction_only")]
    ]
    await update.message.reply_text(status, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# ---------- BUTTON HANDLER ----------
@authorized_only
async def button_handler(update, context):
    query = update.callback_query
    data = query.data
    uid = update.effective_user.id

    if data == "add_account":
        await set_state(uid, "add_phone")
        await query.message.reply_text("📱 Send phone number with country code:\nExample: +1234567890")
    elif data == "joiner_mode":
        await set_state(uid, "join_link")
        await query.message.reply_text(
            "🔗 **Joiner Mode – Step 1 of 3**\n"
            "Send the **channel link** (e.g., `https://t.me/+abc123` or `@username`)\n\n"
            "_(Ya fir aap direct `/join <link> <delay> <count>` bhi use kar sakte ho)_",
            parse_mode="Markdown"
        )
    elif data == "leaver_mode":
        await clear_state(uid)
        await query.message.reply_text("🚪 Send `/leave <link>` for specific channel\nOr `/leave` for all channels")
    elif data == "list_accounts":
        accs = await account_manager.get_accounts_list()
        txt = "📱 Accounts:\n" + "\n".join(accs) if accs else "No accounts."
        await send_long_message(query.message, txt)
    elif data == "activity_log":
        logs = await get_activity_log(10)
        txt = "📜 Logs:\n" + "\n".join(f"{ts} | {a} | {t}" for ts, a, t, _ in logs) if logs else "No activity."
        await send_long_message(query.message, txt)
    elif data == "engagement":
        await query.message.reply_text("💬 Coming soon.")
    elif data == "start_mass":
        await query.message.reply_text("⚡ Use Joiner Mode.")
    elif data == "reaction_only":
        await set_state(uid, "reaction_link")
        await query.message.reply_text("🎯 Send the **post link** (e.g., `https://t.me/username/123`)")
    elif data.startswith("emoji_"):
        # Handled in text handler state
        emoji = data.split("_", 1)[1]
        _, state_data = await get_state(uid)
        post_link = state_data.get('post_link')
        if not post_link:
            await query.message.reply_text("❌ Session expired.")
            return
        if emoji == "custom":
            await set_state(uid, "reaction_emoji", state_data)
            await query.message.reply_text("📝 Type the emoji you want:")
            return
        await reaction_queue.put((update, post_link, emoji, query.message))
        if not is_reaction_processing:
            asyncio.create_task(process_reaction_queue())
        await clear_state(uid)
        await query.message.reply_text(f"✅ Queued {emoji} reactions.")
    return

# ---------- TEXT MESSAGE HANDLER (State Machine) ----------
async def text_handler(update, context):
    uid = update.effective_user.id
    text = update.message.text.strip()

    # Check authorization first
    if not await is_authorized(uid):
        return

    state, state_data = await get_state(uid)

    # ---- Add Account ----
    if state == "add_phone":
        if not text.startswith('+'):
            await update.message.reply_text("❌ Must start with '+'.")
            return
        state_data['phone'] = text
        client = TelegramClient(StringSession(), API_ID, API_HASH, connection_retries=2, timeout=30)
        await client.connect()
        try:
            await client.send_code_request(text)
            state_data['temp_client'] = client
            await set_state(uid, "add_code", state_data)
            await update.message.reply_text("✅ Enter the verification code:")
        except Exception as e:
            await update.message.reply_text(f"❌ {e}")
            await clear_state(uid)
        return

    if state == "add_code":
        client = state_data.get('temp_client')
        phone = state_data.get('phone')
        try:
            await client.sign_in(phone, text)
            session_str = client.session.save()
            await account_manager.add_new_account(phone, session_str)
            await update.message.reply_text(f"✅ Account {phone} added!")
            await client.disconnect()
            await clear_state(uid)
        except errors.SessionPasswordNeededError:
            await set_state(uid, "add_password", state_data)
            await update.message.reply_text("🔐 Enter your 2FA password:")
        except Exception as e:
            await update.message.reply_text(f"❌ {e}")
            await clear_state(uid)
        return

    if state == "add_password":
        client = state_data.get('temp_client')
        phone = state_data.get('phone')
        try:
            await client.sign_in(password=text)
            session_str = client.session.save()
            await account_manager.add_new_account(phone, session_str)
            await update.message.reply_text(f"✅ Account {phone} added (2FA)!")
            await client.disconnect()
        except Exception as e:
            await update.message.reply_text(f"❌ {e}")
        await clear_state(uid)
        return

    # ---- Joiner Mode Flow ----
    if state == "join_link":
        if not re.match(r'(https?://t\.me/|@)', text):
            await update.message.reply_text("❌ Invalid link. Send as `https://t.me/+abc123` or `@username`", parse_mode="Markdown")
            return
        state_data['link'] = text
        await set_state(uid, "join_delay", state_data)
        await update.message.reply_text("**Step 2 of 3** ✅ Link saved.\n\nNow send **delay in seconds** (e.g., `10`):", parse_mode="Markdown")
        return

    if state == "join_delay":
        try:
            delay = int(text)
            if delay < 0:
                raise ValueError
        except:
            await update.message.reply_text("❌ Send a positive number.")
            return
        state_data['delay'] = delay
        await set_state(uid, "join_count", state_data)
        await update.message.reply_text("**Step 3 of 3** ✅ Delay saved.\n\nNow send **number of accounts** to use (e.g., `5`):", parse_mode="Markdown")
        return

    if state == "join_count":
        try:
            count = int(text)
            if count <= 0:
                raise ValueError
        except:
            await update.message.reply_text("❌ Send a positive integer.")
            return
        link = state_data['link']
        delay = state_data['delay']
        total = await account_manager.get_active_sessions()
        if count > total:
            count = total
        await task_queue.put((update, link, delay, count, update.message))
        if not is_processing:
            asyncio.create_task(process_join_queue())
        await clear_state(uid)
        await update.message.reply_text(f"✅ **Task queued!**\n{count} accounts will join with {delay}s delay.")
        return

    # ---- Reaction Flow ----
    if state == "reaction_link":
        if not re.match(r'https://t\.me/(c/)?[^/]+/\d+', text):
            await update.message.reply_text("❌ Invalid post link.")
            return
        state_data['post_link'] = text
        await set_state(uid, "reaction_emoji", state_data)
        keyboard = [
            [InlineKeyboardButton("👍", callback_data="emoji_👍"),
             InlineKeyboardButton("❤️", callback_data="emoji_❤️"),
             InlineKeyboardButton("🎉", callback_data="emoji_🎉"),
             InlineKeyboardButton("😂", callback_data="emoji_😂")],
            [InlineKeyboardButton("🔥", callback_data="emoji_🔥"),
             InlineKeyboardButton("👏", callback_data="emoji_👏"),
             InlineKeyboardButton("😍", callback_data="emoji_😍"),
             InlineKeyboardButton("💯", callback_data="emoji_💯")],
            [InlineKeyboardButton("❓ Custom", callback_data="emoji_custom")]
        ]
        await update.message.reply_text("🎯 Choose reaction emoji:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if state == "reaction_emoji":
        emoji = text
        post_link = state_data.get('post_link')
        if not post_link:
            await update.message.reply_text("❌ Session expired.")
            await clear_state(uid)
            return
        await reaction_queue.put((update, post_link, emoji, update.message))
        if not is_reaction_processing:
            asyncio.create_task(process_reaction_queue())
        await clear_state(uid)
        await update.message.reply_text(f"✅ Queued {emoji} reactions.")
        return

    # ---- No state = show menu ----
    await main_menu(update, context)

# ---------- /join COMMAND (Direct) ----------
@authorized_only
async def join_command(update, context):
    if len(context.args) != 3:
        await update.message.reply_text(
            "❌ **Usage:** `/join <link> <delay_seconds> <count>`\n"
            "Example: `/join https://t.me/+abc123 10 5`",
            parse_mode="Markdown"
        )
        return
    link = context.args[0]
    try:
        delay = int(context.args[1])
        count = int(context.args[2])
        if delay < 0 or count <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Delay and count must be positive numbers.")
        return
    total = await account_manager.get_active_sessions()
    if count > total:
        count = total
    await task_queue.put((update, link, delay, count, update.message))
    if not is_processing:
        asyncio.create_task(process_join_queue())
    await update.message.reply_text(f"✅ **Task queued!**\n{count} accounts joining with {delay}s delay.", parse_mode="Markdown")

# ---------- Leave Commands ----------
@authorized_only
async def leave_command(update, context):
    if context.args:
        results = await account_manager.leave_specific(context.args[0])
    else:
        results = await account_manager.leave_all_channels()
    await send_long_message(update, "\n".join(results))

# ---------- Owner Commands ----------
@owner_only
async def add_owner_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /addowner <id>")
        return
    await add_owner(int(context.args[0]))
    await update.message.reply_text("✅ Owner added.")

@owner_only
async def remove_owner_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /rmowner <id>")
        return
    owners = await get_owners()
    if len(owners) <= 1:
        await update.message.reply_text("❌ Cannot remove only owner.")
        return
    await remove_owner(int(context.args[0]))
    await update.message.reply_text("✅ Owner removed.")

@owner_only
async def add_admin_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /addadmin <id> [username]")
        return
    uid = int(context.args[0])
    uname = context.args[1] if len(context.args) > 1 else None
    await add_admin(uid, uname)
    await update.message.reply_text("✅ Admin added.")

@owner_only
async def remove_admin_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /rmadmin <id>")
        return
    await remove_admin(int(context.args[0]))
    await update.message.reply_text("✅ Admin removed.")

async def cancel_command(update, context):
    await clear_state(update.effective_user.id)
    await update.message.reply_text("❌ Cancelled.")

# ---------- SETUP ----------
async def setup_bot():
    await init_db()
    await account_manager.start_all_accounts()
    app = Application.builder().token(BOT_TOKEN).build()

    async def error_handler(update, context):
        logger.error(f"Error: {context.error}", exc_info=context.error)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("join", join_command))
    app.add_handler(CommandHandler("leave", leave_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("addowner", add_owner_command))
    app.add_handler(CommandHandler("rmowner", remove_owner_command))
    app.add_handler(CommandHandler("addadmin", add_admin_command))
    app.add_handler(CommandHandler("rmadmin", remove_admin_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_error_handler(error_handler)

    return app

# ---------- RUNNER ----------
async def run_bot_async():
    app = await setup_bot()
    logger.info("🤖 Bot polling started")
    await app.run_polling(drop_pending_updates=True)

def main():
    health_thread = threading.Thread(target=run_health_server, args=(PORT,), daemon=True)
    health_thread.start()
    while True:
        try:
            asyncio.run(run_bot_async())
        except Exception as e:
            logger.error(f"Bot crashed: {e}", exc_info=True)
            logger.info("Restarting in 5s...")
            time.sleep(5)

if __name__ == '__main__':
    main()
