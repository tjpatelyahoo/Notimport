import os
import json
import asyncio
import logging
from typing import Tuple, List

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

# ===================== CONFIG =====================

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
OWNER_ID = int(os.environ["OWNER_ID"])

# ✅ USER STRING SESSION ONLY (NO FILE-BASED SESSION)
USER_SESSION_STRING = os.environ["USER_SESSION_STRING"]

DEST_DEFAULT = int(os.environ.get("DESTINATION_CHANNEL", "0"))

FILTERS_FILE = "filters.json"
DEST_FILE = "destination.json"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tgpro")

# ===================== HELPERS =====================

def owner_only(_, __, m: Message):
    return bool(m.from_user and m.from_user.id == OWNER_ID)


def load_filters() -> dict:
    if not os.path.exists(FILTERS_FILE):
        return {}
    with open(FILTERS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_filters(data: dict):
    with open(FILTERS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def apply_filters(text: str, rules: dict) -> str:
    if not text:
        return text
    for old, new in rules.items():
        text = text.replace(old, new)
    return text


def parse_tg_link(link: str) -> Tuple[int, List[int]]:
    parts = link.rstrip("/").split("/")
    msg_id = int(parts[-1])
    chat_id = int("-100" + parts[-2])
    return chat_id, [msg_id]


def extract_caption(msg: Message) -> str:
    return msg.caption or msg.text or ""


async def safe_edit_message(app: Client, chat_id: int, msg_id: int, text: str) -> bool:
    while True:
        try:
            await app.edit_message_caption(chat_id, msg_id, text)
            return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value or fw.seconds or 5)
        except RPCError:
            return False


def load_destination(default: int) -> int:
    if not os.path.exists(DEST_FILE):
        return default
    try:
        with open(DEST_FILE, "r", encoding="utf-8") as f:
            return int(json.load(f).get("dest", default))
    except Exception:
        return default


def save_destination(dest: int):
    with open(DEST_FILE, "w", encoding="utf-8") as f:
        json.dump({"dest": int(dest)}, f)

# ===================== APP =====================

app = Client(
    name="tgpro",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=USER_SESSION_STRING
)

FILTER_RULES = load_filters()

# ===================== FILTER MANAGEMENT =====================

@app.on_message(filters.command("addfilter") & filters.create(owner_only))
async def addfilter(_, m: Message):
    if len(m.command) < 3:
        await m.reply_text("Usage: /addfilter <old> <new>")
        return
    FILTER_RULES[m.command[1]] = " ".join(m.command[2:])
    save_filters(FILTER_RULES)
    await m.reply_text("✅ Filter added")


@app.on_message(filters.command("listfilters") & filters.create(owner_only))
async def listfilters(_, m: Message):
    if not FILTER_RULES:
        await m.reply_text("No filters set.")
        return
    txt = "\n".join(f"{k} → {v}" for k, v in FILTER_RULES.items())
    await m.reply_text(txt)

# ===================== PREVIEW =====================

@app.on_message(filters.command("tgprofilters") & filters.create(owner_only))
async def preview(_, m: Message):
    if len(m.command) < 2:
        await m.reply_text("Usage: /tgprofilters <message_link>")
        return
    chat_id, ids = parse_tg_link(m.command[1])
    msg = await app.get_messages(chat_id, ids[0])
    old = extract_caption(msg)
    new = apply_filters(old, FILTER_RULES)
    await m.reply_text(f"Preview:\n\n{new}")

# ===================== EDIT ORIGINAL =====================

@app.on_message(filters.command("tgprofilters_apply_edit") & filters.create(owner_only))
async def apply_edit(_, m: Message):
    if len(m.command) < 2:
        await m.reply_text("Usage: /tgprofilters_apply_edit <message_link>")
        return
    chat_id, ids = parse_tg_link(m.command[1])
    msg = await app.get_messages(chat_id, ids[0])

    old = extract_caption(msg)
    new = apply_filters(old, FILTER_RULES)

    if old == new:
        await m.reply_text("ℹ️ No change after filters.")
        return

    ok = await safe_edit_message(app, chat_id, msg.id, new)
    await m.reply_text("✅ Edited original message" if ok else "❌ Edit failed")

# ===================== COPY + EDIT =====================

@app.on_message(filters.command("tgprofilters_apply_cp") & filters.create(owner_only))
async def apply_cp(_, m: Message):
    if len(m.command) < 2:
        await m.reply_text("Usage: /tgprofilters_apply_cp <message_link>")
        return

    chat_id, ids = parse_tg_link(m.command[1])
    dest = load_destination(DEST_DEFAULT)
    msg = await app.get_messages(chat_id, ids[0])

    try:
        new_msg = await msg.copy(dest)
        new_cap = apply_filters(extract_caption(msg), FILTER_RULES)
        if new_cap:
            await safe_edit_message(app, dest, new_msg.id, new_cap)
        await m.reply_text("✅ Copied & edited")
    except RPCError:
        await m.reply_text("❌ Copy failed")

# ===================== DESTINATION =====================

@app.on_message(filters.command("setdestination") & filters.create(owner_only))
async def setdestination(_, m: Message):
    if len(m.command) < 2:
        await m.reply_text("Usage: /setdestination <chat_id>")
        return
    save_destination(int(m.command[1]))
    await m.reply_text("✅ Destination updated")

# ===================== RUN =====================

app.run()
