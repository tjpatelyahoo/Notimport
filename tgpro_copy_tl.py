
import os
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
TL_STRING = os.environ["TELETHON_STRING"]

client = TelegramClient(StringSession(TL_STRING), API_ID, API_HASH)
_started = False

async def ensure_started():
    global _started
    if not _started:
        await client.start()
        _started = True

async def telethon_copy(src_chat_id, msg_id, dest_chat_id, topic_id=None):
    await ensure_started()
    src = await client.get_entity(src_chat_id)
    dest = await client.get_entity(dest_chat_id)

    if topic_id:
        await client.forward_messages(dest, msg_id, from_peer=src, drop_author=True, reply_to=topic_id)
    else:
        await client.forward_messages(dest, msg_id, from_peer=src, drop_author=True)
