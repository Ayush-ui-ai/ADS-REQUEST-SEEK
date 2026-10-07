import asyncio
import os
import logging
import sys
import threading
import re
import time
from flask import Flask, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters, ContextTypes
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
            if "authorization key" in err and "different ip" in err:
                return False, "❌ Session invalid (IP conflict). Please re-add this account using /addaccount."
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
                logger.warning(f"⚠️ {phone} not authorized")
        except Exception as e:
            logger.error(f"❌ {phone} connection error: {e}")
            await asyncio.sleep(5)
            try:
                await client.connect()
                self.clients[phone] = client
                logger.info(f"✅ {phone} reconnected")
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

account_manager = AccountManager(API_ID, API_HASH)

# Conversation states
LINK, DELAY, COUNT = range(3)
PHONE, CODE, PASSWORD = range(3, 6)
REACTION_POST_LINK, REACTION_EMOJI = 10, 11

# Task queues
task_queue = asyncio.Queue()
is_processing = False
reaction_queue = asyncio.Queue()
is_reaction_processing = False

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
        f"📌 Progress: {current}/{total}"
    )
    if message.text.strip() != new_text.strip():
        try:
            await message.edit_text(new_text, parse_mode="Markdown")
        except Exception:
            pass

# ---------- PROCESSORS ----------
async def process_join_queue():
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

async def process_reaction_queue():
    global is_reaction_processing
    is_reaction_processing = True
    while not reaction_queue.empty():
        update, post_link, emoji, original_msg = await reaction_queue.get()
        try:
            match = re.search(r'https://t\.me/(c/)?([^/]+)/(\d+)', post_link)
            if not match:
                await original_msg.reply_text("❌ Invalid post link.")
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
                channel_ids_to_try = [
                    channel_id,
                    -1000000000000 - channel_id,
                    -100 + channel_id,
                    int(f"-100{channel_id}") if str(channel_id).isdigit() else None
                ]
                channel_ids_to_try = [c for c in channel_ids_to_try if c is not None]
            else:
                channel_ids_to_try = [channel_part]

            progress_msg = await original_msg.reply_text(f"🔄 Adding {emoji} reactions...")
            total_accounts = len(account_manager.clients)
            success_count = 0
            failed_count = 0
            skipped_count = 0
            current = 0

            for phone, client in account_manager.clients.items():
                current += 1
                entity = None
                for identifier in channel_ids_to_try:
                    try:
                        entity = await client.get_entity(identifier)
                        break
                    except Exception:
                        continue
                if entity is None:
                    failed_count += 1
                    continue

                try:
                    await client(SendReactionRequest(
                        peer=entity,
                        msg_id=message_id,
                        reaction=[ReactionEmoji(emoticon=emoji)]
                    ))
                    success_count += 1
                    await log_activity("REACTION", f"{post_link} ({emoji})", phone)
                except Exception as e:
                    failed_count += 1

                await asyncio.sleep(0.5)

                if current % 5 == 0 or current == total_accounts:
                    await update_progress_message(progress_msg, current, total_accounts, success_count, failed_count)

            summary = (
                f"✅ **Reaction Completed**\n"
                f"📊 Total accounts: {total_accounts}\n"
                f"✅ Success: {success_count}\n"
                f"❌ Failed: {failed_count}\n"
                f"⏭️ Skipped: {skipped_count}\n"
                f"🎯 Reaction: {emoji}"
            )
            await original_msg.reply_text(summary, parse_mode="Markdown")
        except Exception as e:
            try:
                await original_msg.reply_text(f"❌ Reaction task failed: {str(e)}")
            except:
                pass
    is_reaction_processing = False

# ---------- START ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owners = await get_owners()
    if not owners:
        await add_owner(OWNER_ID)
        await add_admin(OWNER_ID, "Owner")

    image_url = "https://i.ibb.co/kgm1fPh7/IMG-20260604-113856-990.jpg"
    try:
        await update.message.reply_photo(
            photo=image_url,
            caption="🔥 **AUTO REQUEST TOOLS**",
            parse_mode="Markdown"
        )
    except Exception:
        await update.message.reply_text("🔥 **AUTO REQUEST TOOLS**", parse_mode="Markdown")

    if await is_authorized(update.effective_user.id):
        await main_menu(update, context)
    else:
        await update.message.reply_text("⛔ Unauthorized.")

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
        f"• Developer: `𓆩𝙎𝙃𝘼𝘿𝙊𝙒 𝙉𝙀𝙏𝙒𝙊𝙍𝙆𓆪🫆`"
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
        ],
        [InlineKeyboardButton("🎯 React to Post", callback_data="reaction_only")]
    ]
    await update.message.reply_text(status, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# ---------- BUTTON HANDLER ----------
@authorized_only
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if data == "add_account":
        await query.message.reply_text("📱 Send phone number with country code:\nExample: +1234567890")
        return PHONE
    elif data == "joiner_mode":
        await query.message.reply_text("**Step 1: Send Channel Link**\nExample: `https://t.me/+abc123` or `@username`", parse_mode="Markdown")
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
    elif data == "reaction_only":
        await query.message.reply_text("📎 Send the **post link** (e.g., `https://t.me/username/123`)", parse_mode="Markdown")
        return REACTION_POST_LINK
    else:
        await query.message.reply_text("❌ Invalid option.")
    return

# ---------- REACTION CONVERSATION ----------
@authorized_only
async def reaction_get_post_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    post_link = update.message.text.strip()
    if not re.match(r'https://t\.me/(c/)?[^/]+/\d+', post_link):
        await update.message.reply_text("❌ Invalid link format. Please send a valid post link like https://t.me/username/123")
        return REACTION_POST_LINK
    context.user_data['post_link'] = post_link

    keyboard = [
        [
            InlineKeyboardButton("👍", callback_data="emoji_👍"),
            InlineKeyboardButton("❤️", callback_data="emoji_❤️"),
            InlineKeyboardButton("🎉", callback_data="emoji_🎉"),
            InlineKeyboardButton("😂", callback_data="emoji_😂")
        ],
        [
            InlineKeyboardButton("🔥", callback_data="emoji_🔥"),
            InlineKeyboardButton("👏", callback_data="emoji_👏"),
            InlineKeyboardButton("😍", callback_data="emoji_😍"),
            InlineKeyboardButton("💯", callback_data="emoji_💯")
        ],
        [InlineKeyboardButton("❓ Type Custom", callback_data="emoji_custom")]
    ]
    await update.message.reply_text("🎯 Choose a reaction emoji:", reply_markup=InlineKeyboardMarkup(keyboard))
    return REACTION_EMOJI

@authorized_only
async def reaction_get_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        data = query.data

        if data.startswith("emoji_"):
            emoji = data.split("_", 1)[1]
            if emoji == "custom":
                await query.message.reply_text("📝 Type the emoji you want to use (e.g., 🎉, 👍, ❤️):")
                return REACTION_EMOJI
            else:
                post_link = context.user_data.get('post_link')
                if not post_link:
                    await query.message.reply_text("❌ Session expired. Please start again.")
                    return ConversationHandler.END

                await reaction_queue.put((update, post_link, emoji, query.message))
                if not is_reaction_processing:
                    asyncio.create_task(process_reaction_queue())

                await query.message.reply_text(f"✅ **Task queued!** Adding {emoji} reactions.")
                return ConversationHandler.END
    else:
        emoji = update.message.text.strip()
        if not emoji:
            await update.message.reply_text("❌ Please send a valid emoji.")
            return REACTION_EMOJI

        post_link = context.user_data.get('post_link')
        if not post_link:
            await update.message.reply_text("❌ Session expired. Please start again.")
            return ConversationHandler.END

        await reaction_queue.put((update, post_link, emoji, update.message))
        if not is_reaction_processing:
            asyncio.create_task(process_reaction_queue())

        await update.message.reply_text(f"✅ **Task queued!** Adding {emoji} reactions.")
        return ConversationHandler.END

async def cancel_reaction(update: Update, context):
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END

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
        await update.message.reply_text("❌ Invalid delay.")
        return DELAY
    await update.message.reply_text("**Step 3: Custom Amount**\nEnter number of accounts to use:")
    return COUNT

@authorized_only
async def get_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        count = int(update.message.text.strip())
        if count <= 0:
            raise ValueError
        context.user_data['count'] = count
    except:
        await update.message.reply_text("❌ Invalid count.")
        return COUNT

    link = context.user_data['link']
    delay = context.user_data['delay']
    count = context.user_data['count']
    total_available = await account_manager.get_active_sessions()
    if count > total_available:
        count = total_available

    await task_queue.put((update, link, delay, count, update.message))
    global is_processing
    if not is_processing:
        asyncio.create_task(process_join_queue())

    await update.message.reply_text("✅ **Task queued!**")
    return ConversationHandler.END

async def cancel_joiner(update: Update, context):
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END

# ---------- ADD ACCOUNT ----------
@authorized_only
async def add_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = update.message.text.strip()
    if not phone.startswith('+'):
        await update.message.reply_text("❌ Phone must start with '+'.")
        return PHONE
    context.user_data['phone'] = phone
    client = TelegramClient(StringSession(), API_ID, API_HASH, connection_retries=2, timeout=30)
    await client.connect()
    try:
        await client.send_code_request(phone)
        context.user_data['temp_client'] = client
        await update.message.reply_text("✅ Code sent! Enter the code:")
        return CODE
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        return ConversationHandler.END

@authorized_only
async def add_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    client = context.user_data.get('temp_client')
    phone = context.user_data.get('phone')
    if not client:
        await update.message.reply_text("❌ Session expired.")
        return ConversationHandler.END
    try:
        await client.sign_in(phone, code)
        session_str = client.session.save()
        await account_manager.add_new_account(phone, session_str)
        await update.message.reply_text(f"✅ Account {phone} added.")
        await client.disconnect()
        return ConversationHandler.END
    except errors.SessionPasswordNeededError:
        await update.message.reply_text("🔐 2FA enabled. Enter password:")
        return PASSWORD
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {str(e)}")
        return ConversationHandler.END

@authorized_only
async def add_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pwd = update.message.text.strip()
    client = context.user_data.get('temp_client')
    phone = context.user_data.get('phone')
    if not client:
        await update.message.reply_text("❌ Session expired.")
        return ConversationHandler.END
    try:
        await client.sign_in(password=pwd)
        session_str = client.session.save()
        await account_manager.add_new_account(phone, session_str)
        await update.message.reply_text(f"✅ Account {phone} added (2FA).")
        await client.disconnect()
        return ConversationHandler.END
    except Exception as e:
        await update.message.reply_text(f"❌ 2FA error: {str(e)}")
        return ConversationHandler.END

async def cancel(update: Update, context):
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END

# ---------- LEAVE ----------
@authorized_only
async def leave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        results = await account_manager.leave_specific(context.args[0])
    else:
        results = await account_manager.leave_all_channels()
    await send_long_message(update, "\n".join(results))

# ---------- OWNER COMMANDS ----------
@owner_only
async def add_owner_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /addowner <user_id>")
        return
    await add_owner(int(context.args[0]))
    await update.message.reply_text(f"✅ Owner added.")

@owner_only
async def remove_owner_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /rmowner <user_id>")
        return
    owners = await get_owners()
    if len(owners) <= 1:
        await update.message.reply_text("❌ Cannot remove the only owner.")
        return
    await remove_owner(int(context.args[0]))
    await update.message.reply_text(f"✅ Owner removed.")

@owner_only
async def add_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /addadmin <user_id> [username]")
        return
    uid = int(context.args[0])
    uname = context.args[1] if len(context.args) > 1 else None
    await add_admin(uid, uname)
    await update.message.reply_text(f"✅ Admin added.")

@owner_only
async def remove_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /rmadmin <user_id>")
        return
    await remove_admin(int(context.args[0]))
    await update.message.reply_text(f"✅ Admin removed.")

@owner_only
async def owners_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owners = await get_owners()
    txt = "👑 **Owners**\n" + "\n".join([f"• `{oid}`" for oid in owners]) if owners else "No owners set."
    await update.message.reply_text(txt, parse_mode="Markdown")

# ---------- BOT SETUP (in background thread) ----------
async def setup_bot():
    await init_db()
    await account_manager.start_all_accounts()

    app = Application.builder().token(BOT_TOKEN).build()

    async def error_handler(update, context):
        logger.error(f"Update caused error: {context.error}", exc_info=context.error)

    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^joiner_mode$")],
        states={
            LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_link)],
            DELAY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_delay)],
            COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_count)],
        },
        fallbacks=[CommandHandler("cancel", cancel_joiner)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^add_account$")],
        states={
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_phone)],
            CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_code)],
            PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_password)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^reaction_only$")],
        states={
            REACTION_POST_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, reaction_get_post_link)],
            REACTION_EMOJI: [
                CallbackQueryHandler(reaction_get_emoji, pattern="^emoji_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, reaction_get_emoji)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_reaction)],
    ))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("leave", leave_command))
    app.add_handler(CommandHandler("addowner", add_owner_command))
    app.add_handler(CommandHandler("rmowner", remove_owner_command))
    app.add_handler(CommandHandler("addadmin", add_admin_command))
    app.add_handler(CommandHandler("rmadmin", remove_admin_command))
    app.add_handler(CommandHandler("owners", owners_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_error_handler(error_handler)

    return app

# ---------- BOT RUNNER (background thread with auto-restart) ----------
def run_bot_forever():
    """Run the bot polling in a dedicated event loop, restart on crash."""
    while True:
        try:
            logger.info("Starting bot polling...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            app = loop.run_until_complete(setup_bot())
            loop.run_until_complete(app.run_polling())
        except Exception as e:
            logger.error(f"Bot crashed: {e}", exc_info=True)
            logger.info("Restarting bot in 5 seconds...")
            time.sleep(5)

# ---------- FLASK HEALTH (main thread – keeps port open) ----------
flask_app = Flask(__name__)

@flask_app.route('/')
@flask_app.route('/health')
def health():
    return jsonify({"status": "alive", "bot": "running"}), 200

def main():
    logger.info("🚀 Starting Flask + Bot...")
    # Start bot in background thread
    bot_thread = threading.Thread(target=run_bot_forever, daemon=True)
    bot_thread.start()

    # Run Flask in main thread (port stays open – hosting won't kill us)
    logger.info(f"Flask listening on port {PORT}")
    flask_app.run(host='0.0.0.0', port=PORT, use_reloader=False, threaded=True)

if __name__ == '__main__':
    main()
