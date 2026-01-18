import os
import re
import json
import asyncio
import logging
from flask import Flask
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait

# ================= CONFIG =================
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
USER_SESSION_STRING = os.environ["USER_SESSION_STRING"]
OWNER_ID = int(os.environ["OWNER_ID"])
DESTINATION_CHANNEL = int(os.environ.get("DESTINATION_CHANNEL", "0"))

FILTER_FILE = "filters.json"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tgpro")

# ================= FILTER STORAGE =================
def load_filters():
    if os.path.exists(FILTER_FILE):
        with open(FILTER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_filters(data):
    with open(FILTER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

FILTERS = load_filters()

# ================= HELPERS =================
def parse_link(link: str):
    """
    Supports:
    https://t.me/c/<id>/<msg>
    https://t.me/c/<id>/<start>-<end>
    """
    m = re.search(r"/c/(\d+)/(\d+|\d+-\d+)", link)
    if not m:
        return None, []

    chat_id = int("-100" + m.group(1))
    part = m.group(2)

    if "-" in part:
        a, b = part.split("-", 1)
        ids = list(range(int(a), int(b) + 1))
    else:
        ids = [int(part)]

    return chat_id, ids

def apply_filters(text: str):
    changed = False
    for bad, good in FILTERS.items():
        if bad in text:
            text = text.replace(bad, good)
            changed = True
    return text, changed

async def safe_edit(app: Client, chat_id: int, msg_id: int, text: str):
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

# ================= PYROGRAM USERBOT =================
app = Client(
    name="tgpro_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=USER_SESSION_STRING,
    in_memory=True
)

# ================= COMMANDS =================
@app.on_message(filters.user(OWNER_ID) & filters.command("addfilter"))
async def add_filter(_, m: Message):
    if len(m.command) < 3:
        await m.reply("Usage: /addfilter <old> <new>")
        return

    bad = m.command[1]
    good = " ".join(m.command[2:])
    FILTERS[bad] = good
    save_filters(FILTERS)
    await m.reply(f"✅ Added filter: {bad} → {good}")

@app.on_message(filters.user(OWNER_ID) & filters.command("tgprofilters_apply_edit"))
async def apply_edit(_, m: Message):
    if len(m.command) < 2:
        await m.reply("Usage: /tgprofilters_apply_edit <link>")
        return

    chat_id, ids = parse_link(m.command[1])
    if not chat_id:
        await m.reply("❌ Invalid link")
        return

    applied = 0

    for mid in ids:
        msg = await app.get_messages(chat_id, mid)
        if not msg:
            continue

        text = msg.caption or msg.text
        if not text:
            continue

        new_text, changed = apply_filters(text)
        if not changed:
            continue

        if await safe_edit(app, chat_id, mid, new_text):
            applied += 1

        await asyncio.sleep(1.2)

    await m.reply(f"✅ Applied edits: {applied}")

@app.on_message(filters.user(OWNER_ID) & filters.command("tgprofilters_apply_cp"))
async def apply_copy(_, m: Message):
    if DESTINATION_CHANNEL == 0:
        await m.reply("❌ DESTINATION_CHANNEL not set")
        return

    if len(m.command) < 2:
        await m.reply("Usage: /tgprofilters_apply_cp <link>")
        return

    chat_id, ids = parse_link(m.command[1])
    if not chat_id:
        await m.reply("❌ Invalid link")
        return

    reposted = 0

    for mid in ids:
        msg = await app.get_messages(chat_id, mid)
        if not msg:
            continue

        text = msg.caption or msg.text
        if not text:
            continue

        new_text, changed = apply_filters(text)
        if not changed:
            continue

        try:
            sent = await msg.copy(DESTINATION_CHANNEL)
            await safe_edit(app, DESTINATION_CHANNEL, sent.id, new_text)
            reposted += 1
            await asyncio.sleep(1.5)
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
        except Exception:
            pass

    await m.reply(f"✅ Reposted & edited: {reposted}")

# ================= FLASK (RENDER KEEPALIVE) =================
web = Flask(__name__)

@web.route("/")
def home():
    return "TGPRO RUNNING"

async def main():
    await app.start()
    log.info("TGPRO USERBOT STARTED")

if __name__ == "__main__":
    import threading
    threading.Thread(
        target=lambda: web.run(
            host="0.0.0.0",
            port=int(os.environ.get("PORT", 10000))
        ),
        daemon=True
    ).start()

    asyncio.run(main())