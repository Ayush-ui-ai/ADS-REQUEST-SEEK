import asyncio
import logging
import re
import json
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

# ---------- LOGGING ----------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ---------- CONFIG ----------
BOT_TOKEN = "8663763583:AAFBuGDqldhCx1YhY94od9ga-jXgW7edCOY"
API_ID = 35598561
API_HASH = "8f359688b1c446a45023045d9656ea37"
OWNER_ID = 6871652449

DB_PATH = "bot_data.db"

# ---------- IN-MEMORY STATE (Fast & Mobile Friendly) ----------
user_states = {}       # user_id -> {"state": "...", "data": {}}
cancel_flags = {}      # user_id -> True if cancel requested

# ---------- DATABASE ----------
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
                action TEXT, target TEXT, account_phone TEXT
            )
        ''')
        await db.commit()

async def get_owners():
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute('SELECT user_id FROM owners')
        return [r[0] for r in await c.fetchall()]

async def is_owner(uid):
    return uid in await get_owners()

async def add_owner(uid):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR IGNORE INTO owners (user_id) VALUES (?)', (uid,))
        await db.commit()

async def remove_owner(uid):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM owners WHERE user_id = ?', (uid,))
        await db.commit()

async def is_authorized(uid):
    if await is_owner(uid):
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute('SELECT 1 FROM admins WHERE user_id = ?', (uid,))
        return await c.fetchone() is not None

async def add_admin(uid, username=None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR IGNORE INTO admins (user_id, username) VALUES (?, ?)', (uid, username))
        await db.commit()

async def remove_admin(uid):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM admins WHERE user_id = ?', (uid,))
        await db.commit()

async def list_admins():
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute('SELECT user_id, username FROM admins')
        return await c.fetchall()

async def add_account_db(phone, session_str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO accounts (phone_number, session_string) VALUES (?, ?)', (phone, session_str))
        await db.commit()

async def get_all_accounts():
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute('SELECT id, phone_number, session_string FROM accounts')
        return await c.fetchall()

async def log_activity(action, target, phone="system"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO activity_log (action, target, account_phone) VALUES (?, ?, ?)', (action, target, phone))
        await db.commit()

async def get_activity_log(limit=10):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute('SELECT timestamp, action, target, account_phone FROM activity_log ORDER BY timestamp DESC LIMIT ?', (limit,))
        return await c.fetchall()

# ---------- ACCOUNT MANAGER ----------
class AccountManager:
    def __init__(self, api_id, api_hash):
        self.api_id = api_id
        self.api_hash = api_hash
        self.clients = {}
        self.online_tasks = {}

    async def _join(self, client, link):
        try:
            m = re.search(r't\.me/\+(.+)', link)
            if m:
                await client(ImportChatInviteRequest(m.group(1).split('_')[0]))
                return True, "✅ join request sent"
            else:
                username = link.strip("/").replace("https://t.me/", "").replace("http://t.me/", "")
                entity = await client.get_entity(username)
                await client(JoinChannelRequest(entity))
                return True, f"✅ joined @{username}"
        except errors.FloodWaitError as e:
            return False, f"⏳ flood {e.seconds}s"
        except Exception as e:
            err = str(e).lower()
            if "successfully requested" in err:
                return True, "✅ join request sent"
            return False, f"❌ {str(e)}"

    async def _keep_online(self, client, phone):
        try:
            await client(UpdateStatusRequest(offline=False))
        except: pass
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < 3600:
            await asyncio.sleep(60)
            try:
                await client(UpdateStatusRequest(offline=False))
            except: pass
        try:
            await client(UpdateStatusRequest(offline=True))
        except: pass
        self.online_tasks.pop(phone, None)

    async def start_all(self):
        for _, phone, sess in await get_all_accounts():
            await self._add(phone, sess)

    async def _add(self, phone, sess):
        client = TelegramClient(StringSession(sess), self.api_id, self.api_hash,
                                connection_retries=5, retry_delay=3, timeout=60, request_retries=3)
        try:
            await client.connect()
            if await client.is_user_authorized():
                self.clients[phone] = client
                logger.info(f"✅ {phone} connected")
        except Exception as e:
            logger.error(f"❌ {phone}: {e}")

    async def add_new(self, phone, sess):
        await add_account_db(phone, sess)
        await self._add(phone, sess)

    async def join_and_online(self, uid, link, delay, count, cb=None):
        cancel_flags.pop(uid, None)
        phones = list(self.clients.keys())
        if count > len(phones): count = len(phones)
        selected = phones[:count]

        for p in selected:
            if p in self.online_tasks:
                self.online_tasks[p].cancel()
            self.online_tasks[p] = asyncio.create_task(self._keep_online(self.clients[p], p))

        results, success = [], []
        total = len(selected)
        cancelled = False

        for idx, phone in enumerate(selected):
            if cancel_flags.get(uid):
                cancelled = True
                results.append("🛑 **Cancelled by user**")
                break

            ok, msg = await self._join(self.clients[phone], link)
            if ok:
                success.append(phone)
                await log_activity("JOIN", link, phone)
                results.append(f"✅ {phone}: {msg}")
            else:
                results.append(f"❌ {phone}: {msg}")

            if cb: await cb(idx + 1, total, len(success), idx + 1 - len(success))

            if idx < total - 1 and delay > 0:
                for _ in range(delay):
                    if cancel_flags.get(uid):
                        cancelled = True
                        break
                    await asyncio.sleep(1)
                if cancelled:
                    results.append("🛑 **Cancelled by user**")
                    break

        if cancelled:
            results.append(f"\n📊 Stopped at: {len(success)}/{total}")
        else:
            results.append(f"\n📊 Total: {total} | Joined: {len(success)}")

        cancel_flags.pop(uid, None)
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

    async def leave_all(self):
        out = []
        for phone, client in self.clients.items():
            try:
                dialogs = await client.get_dialogs()
                left = []
                for d in dialogs:
                    if d.is_channel or d.is_group:
                        try:
                            await client(LeaveChannelRequest(d.entity))
                            left.append(d.name)
                        except: pass
                out.append(f"📱 {phone}: " + (", ".join(left) if left else "nothing"))
            except Exception as e:
                out.append(f"❌ {phone}: {str(e)}")
        return out

    def active_count(self):
        return len(self.clients)

    def accounts_list(self):
        return list(self.clients.keys())

manager = AccountManager(API_ID, API_HASH)

# ---------- HELPERS ----------
def cancel_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Cancel", callback_data="cancel_task")]])

async def send_long(target, text):
    if not text: return
    reply = target.message.reply_text if hasattr(target, 'message') else target.reply_text
    for i in range(0, len(text), 4000):
        await reply(text[i:i+4000])

async def progress_msg(message, current, total, succ, fail):
    pct = int((current/total)*100) if total else 0
    filled = int(20*current/total) if total else 0
    bar = "█"*filled + "░"*(20-filled)
    txt = (f"🔄 **Processing...**\n`[{bar}] {pct}%`\n\n"
           f"✅ Success: {succ}\n❌ Failed: {fail}\n📌 {current}/{total}")
    if message.text.strip() != txt.strip():
        try: await message.edit_text(txt, parse_mode="Markdown")
        except: pass

# ---------- QUEUE PROCESSORS ----------
task_queue = asyncio.Queue()
is_processing = False

async def process_join_queue():
    global is_processing
    is_processing = True
    while not task_queue.empty():
        update, uid, link, delay, count, orig_msg = await task_queue.get()
        try:
            pm = await orig_msg.reply_text("🔄 Starting join requests...", reply_markup=cancel_kb())
            async def cb(c, t, s, f): await progress_msg(pm, c, t, s, f)
            res, _ = await manager.join_and_online(uid, link, delay, count, cb)
            try: await pm.edit_reply_markup(None)
            except: pass
            await send_long(update, "\n".join(res))
        except Exception as e:
            try: await update.message.reply_text(f"❌ {e}")
            except: pass
    is_processing = False

# ---------- AUTH DECORATORS ----------
def auth_only(func):
    async def wrapper(update, context):
        uid = update.effective_user.id
        if await is_authorized(uid):
            return await func(update, context)
        if update.callback_query:
            try: await update.callback_query.answer("⛔ Unauthorized", show_alert=True)
            except: pass
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

# ---------- MAIN MENU ----------
@auth_only
async def main_menu(update, context):
    uid = update.effective_user.id
    is_own = await is_owner(uid)
    active = manager.active_count()
    admins = await list_admins()

    status = (f"🤖 **Manager Bot Pro**\n"
              f"• Active Sessions: `{active}`\n"
              f"• Database: `Connected`\n"
              f"• Admins: `{len(admins)}`\n"
              f"• Developer: `𓆩𝙎𝙃𝘼𝘿𝙊𝙒 𝙉𝙀𝙏𝙒𝙊𝙍𝙆𓆪🫆`")
    if is_own:
        owners = await get_owners()
        olist = "\n".join([f"• `{o}`" for o in owners])
        alist = "\n".join([f"• `{a}` ({u or '?'})" for a, u in admins])
        status += f"\n👑 **Owners**\n{olist}\n**Admins**\n{alist}"

    kb = [
        [InlineKeyboardButton("➕ Add New Account", callback_data="add_account")],
        [InlineKeyboardButton("🔗 Joiner Mode", callback_data="joiner_mode"),
         InlineKeyboardButton("🚪 Leaver Mode", callback_data="leaver_mode")],
        [InlineKeyboardButton("📋 List Accounts", callback_data="list_accounts"),
         InlineKeyboardButton("📜 Activity Log", callback_data="activity_log")],
        [InlineKeyboardButton("🎯 React to Post", callback_data="reaction_only")],
        [InlineKeyboardButton("🛑 Cancel Running Task", callback_data="cancel_task")]
    ]
    await update.message.reply_text(status, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")

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

# ---------- BUTTON HANDLER (FIXED – Answers First) ----------
@auth_only
async def button_handler(update, context):
    query = update.callback_query
    # CRITICAL: Answer immediately to prevent "query too old" timeout
    try: await query.answer()
    except: pass

    data = query.data
    uid = update.effective_user.id
    logger.info(f"🔘 Button: {data} from {uid}")

    try:
        # ---- CANCEL BUTTON ----
        if data == "cancel_task":
            cancel_flags[uid] = True
            user_states.pop(uid, None)
            await query.message.reply_text("🛑 **Cancel requested!** Task will stop shortly.")
            return

        # ---- ADD ACCOUNT ----
        if data == "add_account":
            user_states[uid] = {"state": "add_phone", "data": {}}
            await query.message.reply_text("📱 Send phone number with country code:\nExample: `+1234567890`",
                                           parse_mode="Markdown", reply_markup=cancel_kb())

        # ---- JOINER MODE ----
        elif data == "joiner_mode":
            user_states[uid] = {"state": "join_link", "data": {}}
            await query.message.reply_text(
                "🔗 **Joiner Mode – Step 1 of 3**\n\n**Send the channel link:**\n"
                "Example: `https://t.me/+abc123` or `@username`",
                parse_mode="Markdown", reply_markup=cancel_kb())

        # ---- LEAVER MODE ----
        elif data == "leaver_mode":
            user_states.pop(uid, None)
            await query.message.reply_text("🚪 Send `/leave <link>` or `/leave` for all.")

        # ---- LIST ACCOUNTS ----
        elif data == "list_accounts":
            accs = manager.accounts_list()
            txt = "📱 **Accounts:**\n" + "\n".join(accs) if accs else "No accounts."
            await send_long(query.message, txt)

        # ---- ACTIVITY LOG ----
        elif data == "activity_log":
            logs = await get_activity_log(10)
            txt = "📜 **Logs:**\n" + "\n".join(f"{ts} | {a} | {t}" for ts, a, t, _ in logs) if logs else "No activity."
            await send_long(query.message, txt)

        # ---- REACT TO POST ----
        elif data == "reaction_only":
            user_states[uid] = {"state": "reaction_link", "data": {}}
            await query.message.reply_text("🎯 Send the post link:\nExample: `https://t.me/username/123`",
                                           parse_mode="Markdown", reply_markup=cancel_kb())

        # ---- EMOJI SELECTION ----
        elif data.startswith("emoji_"):
            emoji = data.split("_", 1)[1]
            st = user_states.get(uid, {})
            post_link = st.get("data", {}).get("post_link")
            if not post_link:
                await query.message.reply_text("❌ Session expired. Use /start.")
                return
            if emoji == "custom":
                st["state"] = "reaction_emoji"
                user_states[uid] = st
                await query.message.reply_text("📝 Type the emoji:", reply_markup=cancel_kb())
                return
            # Queue reaction task
            total = manager.active_count()
            await query.message.reply_text(f"⏳ Adding {emoji} reactions on {total} accounts...")
            asyncio.create_task(run_reactions(update, post_link, emoji, query.message))
            user_states.pop(uid, None)

        else:
            await query.message.reply_text(f"❓ Unknown: `{data}`", parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Button error: {e}", exc_info=True)
        try: await query.message.reply_text(f"⚠️ Error: {e}")
        except: pass

# ---------- REACTION TASK ----------
async def run_reactions(update, post_link, emoji, msg):
    try:
        m = re.search(r'https://t\.me/(c/)?([^/]+)/(\d+)', post_link)
        if not m:
            await msg.reply_text("❌ Invalid link.")
            return
        part = m.group(2)
        mid = int(m.group(3))
        if m.group(1):
            cid = int(part)
            ids = [cid, -1000000000000 - cid, -100 + cid]
        else:
            ids = [part]

        pm = await msg.reply_text(f"🔄 Adding {emoji}...")
        total = len(manager.clients)
        succ = fail = cur = 0

        for phone, client in manager.clients.items():
            cur += 1
            entity = None
            for c in ids:
                try:
                    entity = await client.get_entity(c); break
                except: continue
            if entity is None:
                fail += 1; continue
            try:
                await client(SendReactionRequest(peer=entity, msg_id=mid,
                                                 reaction=[ReactionEmoji(emoticon=emoji)]))
                succ += 1
            except: fail += 1
            await asyncio.sleep(0.5)
            if cur % 5 == 0 or cur == total:
                await progress_msg(pm, cur, total, succ, fail)

        await msg.reply_text(f"✅ Done!\nTotal: {total}\nSuccess: {succ}\nFailed: {fail}\nEmoji: {emoji}")
    except Exception as e:
        await msg.reply_text(f"❌ {e}")

# ---------- TEXT HANDLER ----------
async def text_handler(update, context):
    uid = update.effective_user.id
    text = update.message.text.strip()

    if not await is_authorized(uid): return

    st = user_states.get(uid, {})
    state = st.get("state")
    data = st.get("data", {})

    # ---- ADD PHONE ----
    if state == "add_phone":
        if not text.startswith('+'):
            await update.message.reply_text("❌ Must start with '+'."); return
        data['phone'] = text
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        try:
            await client.send_code_request(text)
            data['temp'] = client
            st["data"] = data; st["state"] = "add_code"; user_states[uid] = st
            await update.message.reply_text("✅ Enter verification code:", reply_markup=cancel_kb())
        except Exception as e:
            await update.message.reply_text(f"❌ {e}"); user_states.pop(uid, None)
        return

    # ---- ADD CODE ----
    if state == "add_code":
        client = data.get('temp'); phone = data.get('phone')
        try:
            await client.sign_in(phone, text)
            await manager.add_new(phone, client.session.save())
            await update.message.reply_text(f"✅ Account {phone} added!")
            await client.disconnect(); user_states.pop(uid, None)
        except errors.SessionPasswordNeededError:
            st["state"] = "add_password"; user_states[uid] = st
            await update.message.reply_text("🔐 Enter 2FA password:", reply_markup=cancel_kb())
        except Exception as e:
            await update.message.reply_text(f"❌ {e}"); user_states.pop(uid, None)
        return

    # ---- ADD PASSWORD ----
    if state == "add_password":
        client = data.get('temp'); phone = data.get('phone')
        try:
            await client.sign_in(password=text)
            await manager.add_new(phone, client.session.save())
            await update.message.reply_text(f"✅ Account {phone} added (2FA)!")
            await client.disconnect()
        except Exception as e:
            await update.message.reply_text(f"❌ {e}")
        user_states.pop(uid, None)
        return

    # ---- JOINER LINK ----
    if state == "join_link":
        if not (text.startswith("https://t.me/") or text.startswith("@") or text.startswith("t.me/")):
            await update.message.reply_text("❌ Invalid link.", reply_markup=cancel_kb()); return
        data['link'] = text
        st["state"] = "join_delay"; user_states[uid] = st
        await update.message.reply_text("✅ Link saved.\n**Step 2: Send delay (seconds):**\nExample: `10`",
                                        parse_mode="Markdown", reply_markup=cancel_kb())
        return

    # ---- JOINER DELAY ----
    if state == "join_delay":
        try:
            delay = int(text)
            if delay < 0: raise ValueError
        except:
            await update.message.reply_text("❌ Positive number.", reply_markup=cancel_kb()); return
        data['delay'] = delay
        total = manager.active_count()
        st["state"] = "join_count"; user_states[uid] = st
        await update.message.reply_text(
            f"✅ Delay: {delay}s\n**Step 3: How many accounts?**\n"
            f"• Number (e.g., `10`)\n• Or `all` for all {total}",
            parse_mode="Markdown", reply_markup=cancel_kb())
        return

    # ---- JOINER COUNT ----
    if state == "join_count":
        total = manager.active_count()
        if text.lower() == "all":
            count = total
        else:
            try:
                count = int(text)
                if count <= 0: raise ValueError
                if count > total: count = total
            except:
                await update.message.reply_text(f"❌ Number or `all` (have {total})",
                                                parse_mode="Markdown", reply_markup=cancel_kb()); return

        link = data['link']; delay = data['delay']
        user_states.pop(uid, None)
        await task_queue.put((update, uid, link, delay, count, update.message))
        global is_processing
        if not is_processing:
            asyncio.create_task(process_join_queue())
        await update.message.reply_text(
            f"✅ **Queued!**\nAccounts: `{count}` | Delay: `{delay}s`\nClick 🛑 to cancel.",
            parse_mode="Markdown", reply_markup=cancel_kb())
        return

    # ---- REACTION LINK ----
    if state == "reaction_link":
        if not re.match(r'https://t\.me/(c/)?[^/]+/\d+', text):
            await update.message.reply_text("❌ Invalid post link.", reply_markup=cancel_kb()); return
        data['post_link'] = text
        st["state"] = "reaction_emoji"; user_states[uid] = st
        kb = [
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
        await update.message.reply_text("🎯 Choose emoji:", reply_markup=InlineKeyboardMarkup(kb))
        return

    # ---- REACTION EMOJI (custom typed) ----
    if state == "reaction_emoji":
        emoji = text
        post_link = data.get('post_link')
        if not post_link:
            await update.message.reply_text("❌ Session expired."); user_states.pop(uid, None); return
        user_states.pop(uid, None)
        total = manager.active_count()
        await update.message.reply_text(f"⏳ Adding {emoji} on {total} accounts...")
        asyncio.create_task(run_reactions(update, post_link, emoji, update.message))
        return

    # No state → show menu
    await main_menu(update, context)

# ---------- /join DIRECT COMMAND ----------
@auth_only
async def join_command(update, context):
    if len(context.args) != 3:
        await update.message.reply_text("❌ Usage: `/join <link> <delay> <count|all>`", parse_mode="Markdown"); return
    link = context.args[0]
    try:
        delay = int(context.args[1])
        if delay < 0: raise ValueError
    except:
        await update.message.reply_text("❌ Delay positive number."); return
    total = manager.active_count()
    if context.args[2].lower() == "all":
        count = total
    else:
        try:
            count = int(context.args[2])
            if count <= 0: raise ValueError
            if count > total: count = total
        except:
            await update.message.reply_text("❌ Count number or `all`."); return
    uid = update.effective_user.id
    await task_queue.put((update, uid, link, delay, count, update.message))
    if not is_processing:
        asyncio.create_task(process_join_queue())
    await update.message.reply_text(f"✅ Queued! {count} accounts | {delay}s", reply_markup=cancel_kb())

# ---------- /leave ----------
@auth_only
async def leave_command(update, context):
    if context.args:
        r = await manager.leave_specific(context.args[0])
    else:
        r = await manager.leave_all()
    await send_long(update, "\n".join(r))

# ---------- /cancel ----------
async def cancel_command(update, context):
    uid = update.effective_user.id
    cancel_flags[uid] = True
    user_states.pop(uid, None)
    await update.message.reply_text("🛑 Cancelled!")

# ---------- OWNER COMMANDS ----------
@owner_only
async def add_owner_cmd(update, context):
    if not context.args: await update.message.reply_text("Usage: /addowner <id>"); return
    await add_owner(int(context.args[0]))
    await update.message.reply_text("✅ Owner added.")

@owner_only
async def rm_owner_cmd(update, context):
    if not context.args: await update.message.reply_text("Usage: /rmowner <id>"); return
    if len(await get_owners()) <= 1:
        await update.message.reply_text("❌ Only owner left."); return
    await remove_owner(int(context.args[0]))
    await update.message.reply_text("✅ Owner removed.")

@owner_only
async def add_admin_cmd(update, context):
    if not context.args: await update.message.reply_text("Usage: /addadmin <id> [name]"); return
    uid = int(context.args[0]); uname = context.args[1] if len(context.args) > 1 else None
    await add_admin(uid, uname); await update.message.reply_text("✅ Admin added.")

@owner_only
async def rm_admin_cmd(update, context):
    if not context.args: await update.message.reply_text("Usage: /rmadmin <id>"); return
    await remove_admin(int(context.args[0])); await update.message.reply_text("✅ Admin removed.")

# ---------- MAIN ----------
async def main():
    await init_db()
    await manager.start_all()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("join", join_command))
    app.add_handler(CommandHandler("leave", leave_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("addowner", add_owner_cmd))
    app.add_handler(CommandHandler("rmowner", rm_owner_cmd))
    app.add_handler(CommandHandler("addadmin", add_admin_cmd))
    app.add_handler(CommandHandler("rmadmin", rm_admin_cmd))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    print("🤖 Bot running...")
    await app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    asyncio.run(main())
