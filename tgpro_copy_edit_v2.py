#!/usr/bin/env python3
# TGPRO USERBOT – FINAL CLEAN (OWNER ONLY)

import os, re, json, asyncio, logging
from typing import Dict, Tuple, List
from flask import Flask
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("tgpro")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
OWNER_ID = int(os.environ["OWNER_ID"])
SESSION_STRING = os.environ["USER_SESSION_STRING"]
DESTINATION_CHANNEL = int(os.environ.get("DESTINATION_CHANNEL", "0"))

FILTER_FILE = "filters.json"
STOP_REQUESTED = False

app = Flask(__name__)

@app.route("/", methods=["GET", "HEAD"])
def home():
    return "TGPRO USERBOT ALIVE", 200

def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

def load_filters() -> Dict[str, str]:
    if not os.path.exists(FILTER_FILE):
        return {}
    with open(FILTER_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_filters(data: Dict[str, str]):
    with open(FILTER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

FILTERS = load_filters()

def owner_only(_, __, m: Message):
    return m.from_user and m.from_user.id == OWNER_ID

def parse_link(link: str) -> Tuple[int, List[int]]:
    m = re.search(r"/c/(\d+)/(\d+)(?:-(\d+))?", link)
    if not m:
        raise ValueError("Invalid link")
    chat_id = int("-100" + m.group(1))
    start = int(m.group(2))
    end = int(m.group(3) or start)
    return chat_id, list(range(start, end + 1))

async def apply_filters(text: str):
    changed = 0
    for k, v in FILTERS.items():
        if k in text:
            text = text.replace(k, v)
            changed += 1
    return text, changed

async def safe_edit(app: Client, chat_id: int, msg_id: int, text: str) -> bool:
    while True:
        try:
            await app.edit_message_caption(chat_id, msg_id, text)
            return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
        except Exception:
            try:
                await app.edit_message_text(chat_id, msg_id, text)
                return True
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception:
                return False

client = Client("tgpro_userbot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

@client.on_message(filters.command("addfilter") & filters.create(owner_only))
async def addfilter(_, m: Message):
    if len(m.command) < 3:
        await m.reply_text("Usage: /addfilter OLD NEW")
        return
    old = m.command[1]
    new = " ".join(m.command[2:])
    FILTERS[old] = new
    save_filters(FILTERS)
    await m.reply_text(f"✅ Added filter: {old} → {new}")

@client.on_message(filters.command("listfilters") & filters.create(owner_only))
async def listfilters(_, m: Message):
    if not FILTERS:
        await m.reply_text("No filters set.")
        return
    txt = "\n".join([f"{k} → {v}" for k, v in FILTERS.items()])
    await m.reply_text(f"📋 Filters:\n{txt}")

@client.on_message(filters.command("clearfilters") & filters.create(owner_only))
async def clearfilters(_, m: Message):
    FILTERS.clear()
    save_filters(FILTERS)
    await m.reply_text("🧹 All filters cleared.")

@client.on_message(filters.command("tgprostop") & filters.create(owner_only))
async def tgprostop(_, m: Message):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    await m.reply_text("⛔ Stop requested.")

@client.on_message(filters.command("tgprofilters_apply_edit") & filters.create(owner_only))
async def apply_edit(app: Client, m: Message):
    global STOP_REQUESTED
    STOP_REQUESTED = False
    if len(m.command) < 2:
        await m.reply_text("Usage: /tgprofilters_apply_edit <link>")
        return
    chat_id, ids = parse_link(m.command[1])
    applied = 0
    for mid in ids:
        if STOP_REQUESTED:
            break
        msg = await app.get_messages(chat_id, mid)
        if not msg or not msg.caption:
            continue
        new_text, c = await apply_filters(msg.caption)
        if c > 0:
            if await safe_edit(app, chat_id, mid, new_text):
                applied += 1
        await asyncio.sleep(1)
    await m.reply_text(f"✅ Applied edits: {applied}")

@client.on_message(filters.command("tgprofilters_apply_cp") & filters.create(owner_only))
async def apply_cp(app: Client, m: Message):
    global STOP_REQUESTED
    STOP_REQUESTED = False
    if len(m.command) < 2:
        await m.reply_text("Usage: /tgprofilters_apply_cp <link>")
        return
    if DESTINATION_CHANNEL == 0:
        await m.reply_text("DESTINATION_CHANNEL not set")
        return
    chat_id, ids = parse_link(m.command[1])
    reposted = 0
    for mid in ids:
        if STOP_REQUESTED:
            break
        msg = await app.get_messages(chat_id, mid)
        if not msg or not msg.caption:
            continue
        new_text, c = await apply_filters(msg.caption)
        if c == 0:
            continue
        try:
            sent = await msg.copy(DESTINATION_CHANNEL)
            await safe_edit(app, DESTINATION_CHANNEL, sent.id, new_text)
            reposted += 1
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
        except Exception:
            pass
        await asyncio.sleep(1)
    await m.reply_text(f"✅ Reposted (copy-paste) messages: {reposted}")

async def main():
    await client.start()
    log.info("TGPRO USERBOT STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    import threading
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
