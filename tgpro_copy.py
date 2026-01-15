
import asyncio, random
from pyrogram import filters
from tgpro_copy_tl import telethon_copy

def register_copy_handler(app, owner_only, parse_link, extract_ids_from_link, dlog):
    @app.on_message(filters.command("tgprocopy") & owner_only)
    async def tgprocopy_cmd(c, m):
        if len(m.command) < 3:
            await m.reply_text("Usage: /tgprocopy <src_link> <dest_chat_id> [topic_id]")
            return

        link = m.command[1]
        dest_chat_id = int(m.command[2])
        topic_id = int(m.command[3]) if len(m.command) > 3 else None

        parsed = parse_link(link)
        ids = extract_ids_from_link(link)

        if parsed["kind"] == "internal":
            src_chat_id = int(f"-100{parsed['root']}")
        else:
            chat = await app.get_chat(parsed["root"])
            src_chat_id = chat.id

        ok = fail = 0
        for mid in ids:
            try:
                try:
                    await app.copy_message(dest_chat_id, src_chat_id, mid, message_thread_id=topic_id)
                except Exception:
                    await telethon_copy(src_chat_id, mid, dest_chat_id, topic_id)
                ok += 1
                await asyncio.sleep(random.uniform(0.7, 1.4))
            except Exception as e:
                fail += 1
                await m.reply_text(f"❌ {mid}: {e}")
        await m.reply_text(f"✅ Copied: {ok}\n❌ Failed: {fail}")
