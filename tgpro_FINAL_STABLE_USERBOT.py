#!/usr/bin/env python3
# tgpro_FINAL_STABLE_USERBOT.py
# Stable TGPRO userbot – EDIT + COPY modes

import os
import json
import asyncio
import logging
from typing import Dict

from flask import Flask
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
USER_SESSION = os.environ.get("USER_SESSION_STRING")
OWNER_ID = int(os.environ["OWNER_ID"])
PORT = int(os.environ.get("PORT", 10000))

DEST_FILE = "destination.json"
FILTER_FILE = "filters.json"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tgpro")

def load_json(path, default):
    if os.path.exists(path):
        return json.load(open(path, "r", encoding="utf-8"))
    return default

def save_json(path, data):
    json.dump(data, open(path, "w", encoding="utf-8"), indent=2)

FILTERS: Dict[str, str] = load_json(FILTER_FILE, {})
DESTINATION = load_json(DEST_FILE, {}).get("dest", 0)

async def safe_edit(app, chat_id, msg_id, text):
    while True:
        try:
            await app.edit_message_caption(chat_id, msg_id, text)
            await asyncio.sleep(1.2)
            return True
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            try:
                await app.edit_message_text(chat_id, msg_id, text)
                await asyncio.sleep(1.2)
                return True
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                return False

def apply_filters(text: str) -> str:
    for k, v in FILTERS.items():
        text = text.replace(k, v)
    return text

def owner_only(_, __, m: Message):
    return m.from_user and m.from_user.id == OWNER_ID

app = Client(
    "tgpro_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=USER_SESSION,
    no_updates=True,
    in_memory=True
)

@app.on_message(filters.command("addfilter") & filters.create(owner_only))
async def addfilter(_, m: Message):
    if len(m.command) < 3:
        return await m.reply("Usage: /addfilter old new")
    FILTERS[m.command[1]] = " ".join(m.command[2:])
    save_json(FILTER_FILE, FILTERS)
    await m.reply("✅ Filter added")

@app.on_message(filters.command("listfilters") & filters.create(owner_only))
async def listfilters(_, m: Message):
    if not FILTERS:
        return await m.reply("No filters.")
    await m.reply("\n".join([f"{k} → {v}" for k, v in FILTERS.items()]))

@app.on_message(filters.command("clearfilters") & filters.create(owner_only))
async def clearfilters(_, m: Message):
    FILTERS.clear()
    save_json(FILTER_FILE, FILTERS)
    await m.reply("✅ Cleared")

@app.on_message(filters.command("setdestination") & filters.create(owner_only))
async def setdest(_, m: Message):
    global DESTINATION
    DESTINATION = int(m.command[1])
    save_json(DEST_FILE, {"dest": DESTINATION})
    await m.reply("✅ Destination set")

@app.on_message(filters.command("tgprofilters_apply_edit") & filters.create(owner_only))
async def apply_edit(_, m: Message):
    if not m.reply_to_message:
        return
    msg = m.reply_to_message
    text = msg.caption or msg.text
    if not text:
        return
    new = apply_filters(text)
    if new != text:
        await safe_edit(app, msg.chat.id, msg.id, new)
        await m.reply("✅ Edited")

@app.on_message(filters.command("tgprofilters_apply_cp") & filters.create(owner_only))
async def apply_cp(_, m: Message):
    if DESTINATION == 0 or not m.reply_to_message:
        return
    src = m.reply_to_message
    copied = await src.copy(DESTINATION)
    await asyncio.sleep(1)
    if copied.caption:
        new = apply_filters(copied.caption)
        if new != copied.caption:
            await safe_edit(app, DESTINATION, copied.id, new)
    await m.reply("✅ Copied & edited")

web = Flask(__name__)

@web.route("/")
def home():
    return "TGPRO USERBOT RUNNING"

async def main():
    await app.start()
    log.info("TGPRO USERBOT STARTED")
    web.run(host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    asyncio.run(main())
