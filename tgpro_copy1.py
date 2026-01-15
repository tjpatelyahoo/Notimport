# tgpro_copy.py
import asyncio
import random
from pyrogram import filters


async def universal_copy(
    app,
    src_chat_id: int,
    msg_id: int,
    dest_chat_id: int,
    dest_topic_id: int | None = None
):
    """
    Universal copy:
    channel ↔ group ↔ supergroup ↔ topic
    hides sender
    """
    try:
        if dest_topic_id is not None:
            return await app.copy_message(
                chat_id=dest_chat_id,
                from_chat_id=src_chat_id,
                message_id=msg_id,
                message_thread_id=dest_topic_id,
                protect_content=False
            )
    except Exception:
        pass

    return await app.copy_message(
        chat_id=dest_chat_id,
        from_chat_id=src_chat_id,
        message_id=msg_id,
        protect_content=False
    )


def register_copy_handler(
    app,
    owner_only,
    parse_link,
    extract_ids_from_link,
    dlog
):
    @app.on_message(filters.command("tgprocopy") & owner_only)
    async def tgprocopy_cmd(c, m):
        """
        /tgprocopy <source_link> <dest_chat_id> [dest_topic_id]
        """

        if len(m.command) < 3:
            await m.reply_text(
                "Usage:\n"
                "/tgprocopy <source_link> <dest_chat_id> [dest_topic_id]",
                quote=True
            )
            return

        link = m.command[1].strip()
        dest_chat_id = int(m.command[2])
        dest_topic_id = int(m.command[3]) if len(m.command) > 3 else None

        parsed = parse_link(link)
        ids = extract_ids_from_link(link)

        if not parsed or not ids:
            await m.reply_text("❌ Invalid source link.", quote=True)
            return

        # Resolve source chat
        try:
            if parsed["kind"] == "internal":
                src_chat_id = int(f"-100{parsed['root']}")
            else:
                chat = await app.get_chat(parsed["root"])
                src_chat_id = chat.id
        except Exception as e:
            dlog("source chat resolve failed:", e)
            await m.reply_text("❌ Cannot access source chat.", quote=True)
            return

        copied = 0
        failed = 0

        for mid in ids:
            try:
                await universal_copy(
                    app,
                    src_chat_id,
                    mid,
                    dest_chat_id,
                    dest_topic_id
                )
                copied += 1
                await asyncio.sleep(random.uniform(1.0, 2.0))
            except Exception as e:
                failed += 1
                dlog("universal_copy failed:", e)

        await m.reply_text(
            f"✅ Copied: {copied}\n"
            f"❌ Failed: {failed}\n"
            f"📌 Topic: {dest_topic_id or 'none'}",
            quote=True
        )
