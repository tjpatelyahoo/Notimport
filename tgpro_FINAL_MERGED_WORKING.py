
# tgpro_FINAL_MERGED_WORKING.py
# BOT commands + USER session executor (OWNER ONLY)
# Copy (bot/user) + Edit (user) | No preview | Batch safe | Render-ready

import os
import re
import asyncio
import logging
from typing import Dict, List, Tuple

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from flask import Flask

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
USER_SESSION_STRING = os.environ["USER_SESSION_STRING"]
OWNER_ID = int(os.environ["OWNER_ID"])
PORT = int(os.environ.get("PORT", "10000"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tgpro")

app = Flask(__name__)

@app.route("/", methods=["GET", "HEAD"])
def home():
    return "TGPRO OK", 200

def run_flask():
    app.run(host="0.0.0.0", port=PORT)

FILTERS: Dict[str, str] = {}
STOP_FLAG = False
DESTINATION = None

bot = Client("tgpro_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
user = Client("tgpro_user", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION_STRING, in_memory=True)

def owner_only(_, __, m):
    return m.from_user and m.from_user.id == OWNER_ID

def apply_filters(text: str):
    if not text:
        return text, False
    changed = False
    for b, r in FILTERS.items():
        if b in text:
            text = text.replace(b, r)
            changed = True
    return text, changed

def parse_link(link: str):
    m = re.search(r"/c/(-?\d+)/(\d+)(?:-(\d+))?$", link)
    if not m:
        raise ValueError("Invalid link")
    chat_id = int("-100" + m.group(1))
    start = int(m.group(2))
    end = int(m.group(3)) if m.group(3) else start
    return chat_id, list(range(start, end + 1))

async def safe_sleep(e):
    if isinstance(e, FloodWait):
        await asyncio.sleep(e.value + 1)

@bot.on_message(filters.command("addfilter") & filters.create(owner_only))
async def addfilter(_, m):
    if len(m.command) < 3:
        await m.reply("Usage: /addfilter \"old\" \"new\"")
        return
    FILTERS[m.command[1]] = " ".join(m.command[2:])
    await m.reply("✅ Filter added")

@bot.on_message(filters.command("listfilters") & filters.create(owner_only))
async def listfilters(_, m):
    if not FILTERS:
        await m.reply("No filters")
        return
    await m.reply("\n".join([f"{k} → {v}" for k, v in FILTERS.items()]))

@bot.on_message(filters.command("clearfilters") & filters.create(owner_only))
async def clearfilters(_, m):
    FILTERS.clear()
    await m.reply("✅ Filters cleared")

@bot.on_message(filters.command("setdestination") & filters.create(owner_only))
async def setdestination(_, m):
    global DESTINATION
    DESTINATION = int(m.command[1])
    await m.reply(f"✅ Destination set")

@bot.on_message(filters.command("tgprostop") & filters.create(owner_only))
async def stop(_, m):
    global STOP_FLAG
    STOP_FLAG = True
    await m.reply("⛔ Stopping")

@bot.on_message(filters.command("tgprofilters_apply_edit") & filters.create(owner_only))
async def apply_edit(_, m):
    global STOP_FLAG
    STOP_FLAG = False
    chat_id, mids = parse_link(m.command[1])
    edited = 0
    for mid in mids:
        if STOP_FLAG:
            break
        try:
            msg = await user.get_messages(chat_id, mid)
            if not msg or not msg.caption:
                continue
            new, ch = apply_filters(msg.caption)
            if not ch:
                continue
            await user.edit_message_caption(chat_id, mid, new)
            edited += 1
            await asyncio.sleep(0.4)
        except FloodWait as e:
            await safe_sleep(e)
        except RPCError:
            continue
    await m.reply(f"✅ Applied edits: {edited}")

@bot.on_message(filters.command("tgprofilters_apply_cp") & filters.create(owner_only))
async def apply_cp(_, m):
    global STOP_FLAG
    STOP_FLAG = False
    if not DESTINATION:
        await m.reply("❌ Set destination first")
        return
    chat_id, mids = parse_link(m.command[1])
    copied = 0
    for mid in mids:
        if STOP_FLAG:
            break
        try:
            try:
                sent = await bot.copy_message(DESTINATION, chat_id, mid)
            except RPCError:
                sent = await user.copy_message(DESTINATION, chat_id, mid)
            if sent.caption:
                new, ch = apply_filters(sent.caption)
                if ch:
                    await user.edit_message_caption(DESTINATION, sent.id, new)
            copied += 1
            await asyncio.sleep(0.4)
        except FloodWait as e:
            await safe_sleep(e)
        except RPCError:
            continue
    await m.reply(f"✅ Copied: {copied}")

async def main():
    await bot.start()
    await user.start()
    log.info("TGPRO STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    import threading
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
