
import os
import re
import json
import asyncio
import logging
from flask import Flask
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message

# ================= CONFIG =================
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
OWNER_ID = int(os.environ["OWNER_ID"])
USER_SESSION_STRING = os.environ["USER_SESSION_STRING"]
PORT = int(os.environ.get("PORT", "10000"))

FILTER_FILE = "filters.json"
DEST_FILE = "destination.json"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tgpro")

# ================= UTILS =================
def load_filters():
    if os.path.exists(FILTER_FILE):
        return json.load(open(FILTER_FILE, "r", encoding="utf-8"))
    return {}

def save_filters(f):
    json.dump(f, open(FILTER_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

def load_dest():
    if os.path.exists(DEST_FILE):
        return int(json.load(open(DEST_FILE)).get("dest", 0))
    return 0

def save_dest(d):
    json.dump({"dest": int(d)}, open(DEST_FILE, "w"))

def parse_link(link):
    m = re.search(r"/c/(\\d+)/(\\d+)(?:-(\\d+))?", link)
    if not m:
        return None
    chat = -100 * int(m.group(1))
    start = int(m.group(2))
    end = int(m.group(3)) if m.group(3) else start
    return chat, start, end

async def safe_edit(app, chat_id, msg_id, text):
    while True:
        try:
            await app.edit_message_caption(chat_id, msg_id, text)
            return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
        except Exception:
            return False

# ================= APP =================
app = Client(
    "tgpro",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=USER_SESSION_STRING
)

filters_map = load_filters()
DEST = load_dest()

# ================= COMMANDS =================
OWNER_ONLY = filters.private & filters.user(OWNER_ID)

@app.on_message(OWNER_ONLY & filters.command("addfilter"))
async def addfilter(_, m: Message):
    if len(m.command) < 3:
        return await m.reply("Usage: /addfilter old new")
    old = m.command[1]
    new = " ".join(m.command[2:])
    filters_map[old] = new
    save_filters(filters_map)
    await m.reply(f"✅ Added: {old} → {new}")

@app.on_message(OWNER_ONLY & filters.command("listfilters"))
async def listfilters(_, m):
    if not filters_map:
        return await m.reply("No filters.")
    txt = "\\n".join(f"{k} → {v}" for k,v in filters_map.items())
    await m.reply(txt)

@app.on_message(OWNER_ONLY & filters.command("clearfilters"))
async def clearfilters(_, m):
    filters_map.clear()
    save_filters(filters_map)
    await m.reply("✅ All filters cleared")

@app.on_message(OWNER_ONLY & filters.command("setdestination"))
async def setdest(_, m):
    if len(m.command) < 2:
        return await m.reply("Usage: /setdestination -100xxxx")
    save_dest(m.command[1])
    await m.reply("✅ Destination set")

@app.on_message(OWNER_ONLY & filters.command("tgprofilters_apply_edit"))
async def apply_edit(_, m):
    if len(m.command) < 2:
        return await m.reply("Give message link")
    parsed = parse_link(m.command[1])
    if not parsed:
        return await m.reply("Invalid link")
    chat, s, e = parsed
    done = 0
    for mid in range(s, e+1):
        try:
            msg = await app.get_messages(chat, mid)
            if not msg or not msg.caption:
                continue
            new = msg.caption
            for k,v in filters_map.items():
                new = new.replace(k, v)
            if new != msg.caption:
                ok = await safe_edit(app, chat, mid, new)
                if ok:
                    done += 1
        except Exception:
            continue
    await m.reply(f"✅ Applied edits: {done}")

@app.on_message(OWNER_ONLY & filters.command("tgprofilters_apply_cp"))
async def apply_cp(_, m):
    if len(m.command) < 2:
        return await m.reply("Give message link")
    parsed = parse_link(m.command[1])
    if not parsed:
        return await m.reply("Invalid link")
    dest = load_dest()
    if not dest:
        return await m.reply("Set destination first")
    chat, s, e = parsed
    done = 0
    for mid in range(s, e+1):
        try:
            msg = await app.get_messages(chat, mid)
            if not msg:
                continue
            copied = await msg.copy(dest)
            if copied and copied.caption:
                new = copied.caption
                for k,v in filters_map.items():
                    new = new.replace(k, v)
                if new != copied.caption:
                    await safe_edit(app, dest, copied.id, new)
            done += 1
        except Exception:
            continue
    await m.reply(f"✅ Reposted: {done}")

# ================= FLASK =================
web = Flask(__name__)

@web.route("/")
def home():
    return "TGPRO USERBOT RUNNING", 200

async def main():
    await app.start()
    log.info("TGPRO USERBOT STARTED")
    web.run(host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    asyncio.run(main())
