# bot_raw_backup_final_v3.py
"""
Final RAW backup bot v2 (patched)
- /tgprobackup -> normal backup
- Robust peer-load fixes for fresh/new session strings:
  * larger dialog preload
  * RAW force-load via channels.GetFullChannel with access_hash=0
  * ensure_peer called before processing each message
"""

import asyncio
import logging
import os
import random
import re
import sys
from typing import Optional, List, Dict, Any

from flask import Flask
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

# raw
from pyrogram.raw import functions as raw_functions
from pyrogram.raw import types as raw_types

# ---------- config ----------
DEBUG = os.environ.get("DEBUG", "0") == "1"
PORT = int(os.environ.get("PORT", "10000"))

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("backup_final_v2")
logging.getLogger("pyrogram").setLevel(logging.INFO if DEBUG else logging.WARNING)

app = Flask(__name__)

@app.route("/")
def home():
    return "Backup Raw Final v2 Running"

@app.route("/health")
def health():
    return "OK"

def dlog(*args):
    if DEBUG:
        logger.info("[DEBUG] " + " ".join(str(a) for a in args))

# ---------- helpers ----------
def safe_filename(name: str) -> str:
    if not name:
        return "file"
    # remove problematic chars and trim
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = name.strip()
    if not name:
        return "file"
    return name

def parse_range(text: str) -> List[int]:
    out = []
    for piece in str(text).split(","):
        piece = piece.strip()
        if not piece:
            continue
        if "-" in piece:
            a, b = piece.split("-", 1)
            if a.isdigit() and b.isdigit():
                a_i, b_i = int(a), int(b)
                if a_i <= b_i:
                    out.extend(range(a_i, b_i + 1))
                else:
                    out.extend(range(b_i, a_i + 1))
        else:
            if piece.isdigit():
                out.append(int(piece))
    return sorted(set(out))

def extract_ids_from_link(link: str) -> Optional[List[int]]:
    try:
        # last part may be "topic/ids" or just ids
        last = link.rstrip("/").split("/")[-1]
        ids = parse_range(last)
        return ids if ids else None
    except Exception:
        return None

def parse_link(link: str) -> Optional[Dict[str, Any]]:
    """
    Return dict:
    - kind: 'internal' or 'username'
    - root: internal id or username
    - maybe topic: str if present (the second segment after root)
    - msg_part: the rest after topic or root
    """
    try:
        l = str(link).strip()
        if "://" in l:
            l = l.split("://", 1)[1]
        if l.startswith("t.me/"):
            l = l.split("t.me/", 1)[1]
        if l.startswith("c/") or "/c/" in l:
            after = l.split("c/", 1)[1]
            parts = after.split("/")
            # /c/<internal>/<topic>/<msg...>  or /c/<internal>/<msg...>
            if len(parts) == 1:
                return {"kind": "internal", "root": parts[0], "topic": None, "msg_part": None}
            elif len(parts) == 2:
                # ambiguous: could be topic or msg
                # we'll return topic=None and msg_part=parts[1]; later logic will detect numeric-topic pattern if needed
                return {"kind": "internal", "root": parts[0], "topic": None, "msg_part": parts[1]}
            else:
                return {"kind": "internal", "root": parts[0], "topic": parts[1], "msg_part": "/".join(parts[2:])}
        else:
            parts = l.split("/")
            if len(parts) >= 2:
                username = parts[0]
                if len(parts) == 2:
                    return {"kind": "username", "root": username, "topic": None, "msg_part": parts[1]}
                else:
                    return {"kind": "username", "root": username, "topic": parts[1], "msg_part": "/".join(parts[2:])}
        return None
    except Exception:
        return None

# magic-bytes detection for file type
def detect_type_by_magic(path: str) -> str:
    try:
        with open(path, "rb") as f:
            header = f.read(64)
        if header.startswith(b"%PDF"):
            return "document"
        if header.startswith(b"\xff\xd8\xff"):
            return "photo"
        if header.startswith(b"\x89PNG"):
            return "photo"
        if b"ftyp" in header[4:12] or header[4:8] == b"moov":
            return "video"
        if header.startswith(b"\x1A\x45\xDF\xA3"):
            return "video"
        if header.startswith(b"ID3") or header[0:2] == b"fL":
            return "audio"
        return "document"
    except Exception:
        return "document"

# ---------- bot ----------
class BackupBotFinalV2:
    def __init__(self):
        try:
            self.api_id = int(os.environ["API_ID"])
            self.api_hash = os.environ["API_HASH"]
            self.dest_channel = int(os.environ["DESTINATION_CHANNEL"])
            self.owner_id = int(os.environ["OWNER_ID"])
        except KeyError as e:
            logger.error("Missing environment variable: %s", e)
            sys.exit(1)

        self.min_delay = int(os.environ.get("MIN_DELAY", "4"))
        self.max_delay = int(os.environ.get("MAX_DELAY", "8"))
        if self.max_delay < self.min_delay:
            self.max_delay = self.min_delay + 3

        session = os.environ.get("USER_SESSION_STRING")
        client_kwargs = {"api_id": self.api_id, "api_hash": self.api_hash, "sleep_threshold": 60}
        if session:
            client_kwargs["session_string"] = session

        self.app = Client("backup_final_v2", **client_kwargs)
        self.queue: asyncio.Queue = asyncio.Queue()
        self.current_backup = None
        self.processor_task = None
        self.stop_flag = asyncio.Event()
        self.downloads = "downloads"
        os.makedirs(self.downloads, exist_ok=True)
        self._handlers_registered = False

    async def ensure_peer(self, peer_id: int):
        """
        Ensure peer is known to session.
        Steps:
         1) try get_chat(peer_id)
         2) scan dialogs (increased limit)
         3) RAW force-load using channels.GetFullChannel(InputChannel(channel_id, access_hash=0))
        Returns True if peer resolvable, False otherwise.
        """
        # 1) try get_chat
        try:
            await self.app.get_chat(peer_id)
            dlog("ensure_peer: get_chat succeeded", peer_id)
            return True
        except Exception as e:
            dlog("ensure_peer: get_chat failed", e)

        # 2) scan dialogs (larger limit)
        try:
            async for d in self.app.get_dialogs(limit=2000):
                if getattr(d.chat, "id", None) == peer_id:
                    dlog("ensure_peer: found in dialogs", peer_id)
                    return True
        except Exception as e:
            dlog("ensure_peer: dialog scan failed", e)

        # 3) RAW force-load via channels.GetFullChannel with access_hash=0
        try:
            # We expect internal form -100<id> when coming from parse_link
            pid = int(peer_id)
            if str(pid).startswith("-100"):
                channel_id_for_raw = abs(int(str(pid).replace("-100", "")))
            else:
                channel_id_for_raw = abs(pid)

            inp_channel = raw_types.InputChannel(channel_id=channel_id_for_raw, access_hash=0)
            try:
                dlog("ensure_peer: invoking channels.GetFullChannel for", channel_id_for_raw)
                await self.app.invoke(raw_functions.channels.GetFullChannel(channel=inp_channel))
                dlog("ensure_peer: RAW GetFullChannel succeeded for", channel_id_for_raw)
                return True
            except Exception as e:
                # try alternative: messages.GetFullChat (for groups) or channels.GetFullChannel fallback tried
                dlog("ensure_peer: RAW GetFullChannel failed", e)
                # try messages.GetPeerDialogs? not necessary; return False after this
        except Exception as e:
            dlog("ensure_peer: RAW fallback exception", e)

        dlog("ensure_peer: unable to resolve peer", peer_id)
        return False

    async def fetch_message(self, chat_id: int, msg_id: int):
        try:
            msg = await self.app.get_messages(chat_id, msg_id)
            if msg and not getattr(msg, "empty", False):
                return msg
        except Exception as e:
            dlog("fetch_message high-level failed:", e)
        # try list form
        try:
            msg = await self.app.get_messages(chat_id, [msg_id])
            if msg and not getattr(msg, "empty", False):
                return msg
        except Exception:
            pass
        return None

    # --------- NEW robust download fallback (5-step) ----------
    async def download_with_fallbacks(self, msg_obj, chat_id: int, msg_id: int, filename_hint: Optional[str] = None) -> Optional[str]:
        """
        Steps:
        1) app.download_media(msg_obj)
        2) download by file_id (document/photo/video)
        3) forward to "me" and download forwarded message
        4) re-fetch high-level message and download
        5) RAW messages.GetMessages -> convert to pyrogram message via _parse_message -> download_media
        """
        def build_dest(name: str):
            safe = safe_filename(name)
            dest = os.path.join(self.downloads, safe)
            base, ext = os.path.splitext(dest)
            i = 0
            final = dest
            while os.path.exists(final):
                i += 1
                final = f"{base}_{i}{ext}"
            return final

        # 1) direct download_media
        try:
            dlog("Attempting direct download_media for", msg_id)
            dest = filename_hint or f"{abs(chat_id)}_{msg_id}"
            path = await self.app.download_media(msg_obj, file_name=os.path.join(self.downloads, dest))
            if path:
                dlog("Direct download ok:", path)
                return path
        except Exception as e:
            dlog("direct download failed:", e)

        # 2) file_id download
        try:
            file_id = None
            fname = None
            if getattr(msg_obj, "document", None):
                file_id = msg_obj.document.file_id
                fname = getattr(msg_obj.document, "file_name", None)
            elif getattr(msg_obj, "photo", None):
                file_id = msg_obj.photo.file_id
            elif getattr(msg_obj, "video", None):
                file_id = msg_obj.video.file_id
                fname = getattr(msg_obj.video, "file_name", None)
            if file_id:
                dlog("Attempting download_media with file_id")
                dest_name = fname or filename_hint or f"{abs(chat_id)}_{msg_id}"
                path = await self.app.download_media(file_id, file_name=os.path.join(self.downloads, dest_name))
                if path:
                    dlog("file_id download ok:", path)
                    return path
        except Exception as e:
            dlog("file_id download failed:", e)

        # 3) forward to saved messages and download
        try:
            dlog("Attempting forward to Saved Messages for", msg_id)
            fwd = await self.app.forward_messages("me", chat_id, msg_id)
            if fwd:
                try:
                    fp = await self.app.download_media(fwd, file_name=os.path.join(self.downloads, filename_hint or f"{abs(chat_id)}_{msg_id}"))
                    try:
                        await self.app.delete_messages("me", fwd.message_id)
                    except Exception:
                        pass
                    if fp:
                        dlog("download from forwarded succeeded:", fp)
                        return fp
                except Exception as e:
                    dlog("download forwarded failed:", e)
        except Exception as e:
            dlog("forward to me failed:", e)

        # 4) re-fetch high-level message and try
        try:
            dlog("Attempting re-fetch and download for", msg_id)
            ref = await self.app.get_messages(chat_id, msg_id)
            if ref and not getattr(ref, "empty", False):
                try:
                    fp2 = await self.app.download_media(ref, file_name=os.path.join(self.downloads, filename_hint or f"{abs(chat_id)}_{msg_id}"))
                    if fp2:
                        dlog("re-fetch download ok:", fp2)
                        return fp2
                except Exception as e:
                    dlog("re-fetch download failed:", e)
        except Exception as e:
            dlog("re-fetch failed:", e)

        # 5) RAW get messages -> convert -> download
        try:
            dlog("Attempting RAW messages.GetMessages fallback for", msg_id)
            # build InputMessageID
            inpm = raw_types.InputMessageID(id=msg_id)
            # We need a peer. If chat_id is -100<id>, use that channel id
            channel_id_for_raw = abs(int(str(chat_id).replace("-100", ""))) if str(chat_id).startswith("-100") else abs(int(chat_id))
            # build InputPeerChannel with access_hash=0 (server will return full chat data)
            inp_channel = raw_types.InputChannel(channel_id=channel_id_for_raw, access_hash=0)
            try:
                raw_res = await self.app.invoke(raw_functions.messages.GetMessages(id=[inpm]))
            except Exception as e:
                # fallback: try with channel peer
                try:
                    raw_res = await self.app.invoke(raw_functions.channels.GetMessages(channel=inp_channel, id=[inpm]))
                except Exception as e2:
                    dlog("raw GetMessages both attempts failed:", e, e2)
                    raw_res = None
            if raw_res and getattr(raw_res, "messages", None):
                raw_msg = raw_res.messages[0]
                # convert raw_msg to a pyrogram Message using internal parser if available
                try:
                    # Pyrogram internal method: _parse_message exists in Client
                    parsed = None
                    if hasattr(self.app, "_parse_message"):
                        # _parse_message signature may differ by version; pass raw_msg and chat id
                        try:
                            parsed = self.app._parse_message(raw_msg, chat_id)
                        except Exception:
                            # older versions: try without chat_id
                            try:
                                parsed = self.app._parse_message(raw_msg)
                            except Exception:
                                parsed = None
                    # If parsed is a proper pyrogram Message-like, attempt download
                    if parsed:
                        dlog("Converted raw->pyrogram message; attempting download")
                        try:
                            path_raw = await self.app.download_media(parsed, file_name=os.path.join(self.downloads, filename_hint or f"{abs(chat_id)}_{msg_id}"))
                            if path_raw:
                                dlog("RAW->converted download ok:", path_raw)
                                return path_raw
                        except Exception as e:
                            dlog("download from converted parsed failed:", e)
                except Exception as e:
                    dlog("convert raw failed:", e)
        except Exception as e:
            dlog("RAW fallback exception:", e)

        dlog("All download fallbacks exhausted for", msg_id)
        return None

    def detect_media_type(self, msg_obj, path: Optional[str]) -> Dict[str, Any]:
        res = {"is_photo": False, "is_video": False, "is_document": False, "is_audio": False, "filename": None}
        try:
            if getattr(msg_obj, "photo", None):
                res["is_photo"] = True
            if getattr(msg_obj, "video", None):
                res["is_video"] = True
                res["filename"] = getattr(getattr(msg_obj, "video", None), "file_name", None)
            if getattr(msg_obj, "document", None):
                res["is_document"] = True
                res["filename"] = getattr(getattr(msg_obj, "document", None), "file_name", None)
                mt = getattr(getattr(msg_obj, "document", None), "mime_type", None)
                if mt and isinstance(mt, str):
                    if mt.startswith("image/"):
                        res["is_photo"] = True
                        res["is_document"] = False
                    elif mt.startswith("video/"):
                        res["is_video"] = True
                        res["is_document"] = False
                    elif mt == "application/pdf":
                        res["is_document"] = True
            if not any([res["is_photo"], res["is_video"], res["is_document"]]) and path:
                ext = os.path.splitext(path)[1].lower()
                if ext in (".jpg", ".jpeg", ".png", ".webp"):
                    res["is_photo"] = True
                elif ext in (".mp4", ".mkv", ".mov", ".webm"):
                    res["is_video"] = True
                elif ext in (".pdf", ".docx", ".pptx", ".zip", ".txt"):
                    res["is_document"] = True
                elif ext in (".mp3", ".ogg", ".wav", ".flac"):
                    res["is_audio"] = True
            if not any([res["is_photo"], res["is_video"], res["is_document"], res["is_audio"]]) and path:
                kind = detect_type_by_magic(path)
                if kind == "photo":
                    res["is_photo"] = True
                elif kind == "video":
                    res["is_video"] = True
                elif kind == "audio":
                    res["is_audio"] = True
                else:
                    res["is_document"] = True
            if not res["filename"]:
                try:
                    if getattr(msg_obj, "document", None):
                        res["filename"] = getattr(msg_obj.document, "file_name", None)
                    elif getattr(msg_obj, "video", None):
                        res["filename"] = getattr(msg_obj.video, "file_name", None)
                except Exception:
                    pass
        except Exception as e:
            dlog("detect_media_type error:", e)
        return res

    # ---------- handlers ----------
    def register_handlers(self):
        if self._handlers_registered:
            return
        self._handlers_registered = True

        @self.app.on_message(filters.command("tgprostart"))
        async def tgprostart(c, m: Message):
            if not m.from_user or m.from_user.id != self.owner_id:
                return
            if m.chat and m.chat.type == "private":
                await m.reply_text("✅ Pyrogram RAW Backup final v2 online (owner verified).")
            else:
                return

        owner_only = filters.create(lambda _, __, m: m.from_user and m.from_user.id == self.owner_id)

        @self.app.on_message(filters.command("tgprobackup") & owner_only)
        async def backup_cmd(c, m: Message):
            if len(m.command) < 2:
                await m.reply_text("❌ Provide link. Example: /tgprobackup https://t.me/c/<internal>/<topic?>/<start>-<end>", quote=True)
                return
            link = m.command[1].strip()
            await m.reply_text(f"🔍 Processing: `{link}`", quote=True)
            await self.enqueue_backup(link, m)

        @self.app.on_message(filters.command("status") & owner_only)
        async def status_cmd(c, m: Message):
            if self.current_backup:
                chat, idx, total, done = self.current_backup
                await m.reply_text(f"📊 Running: {idx}/{total} in {chat.get('title')} (done {done}). Queue: {self.queue.qsize()}", quote=True)
            else:
                await m.reply_text(f"📊 Idle. Queue: {self.queue.qsize()}", quote=True)

        @self.app.on_message(filters.command("tgprostop") & owner_only)
        async def stop_cmd(c, m: Message):
            if self.current_backup:
                self.current_backup = None
                await m.reply_text("🛑 Stop signal set. Stopping after current file.", quote=True)
            else:
                await m.reply_text("ℹ️ No active backup.", quote=True)

        @self.app.on_message(filters.command("chats") & owner_only)
        async def chats_cmd(c, m: Message):
            out = []
            async for d in self.app.get_dialogs():
                ch = d.chat
                if getattr(ch, "type", None) in ("channel", "supergroup", "group"):
                    out.append(f"{getattr(ch,'title',str(getattr(ch,'id','')))} — `{ch.id}`")
            if not out:
                await m.reply_text("❌ No chats found.", quote=True)
            else:
                await m.reply_text("📋 Your chats:\n\n" + "\n".join(out[:60]), quote=True)

    async def enqueue_backup(self, link: str, message: Message):
        parsed = parse_link(link)
        dlog("enqueue parsed:", parsed)
        if not parsed:
            await message.reply_text("❌ Invalid link format.", quote=True)
            return

        # Extract ids
        ids = extract_ids_from_link(link)
        dlog("ids:", ids)
        if not ids:
            await message.reply_text("❌ Could not extract message ids. Provide numeric or range in the link.", quote=True)
            return

        # Resolve chat id
        try:
            root = parsed["root"]
            if parsed["kind"] == "internal":
                chat_id = int(f"-100{root}")
            else:
                chat_obj = await self.app.get_chat(root)
                chat_id = chat_obj.id
        except Exception as e:
            dlog("resolve chat failed:", e)
            await message.reply_text("❌ Chat not found or you lack access.", quote=True)
            return

        # If parsed includes topic numeric in middle, treat as topic filtering
        topic_id = None
        if parsed.get("topic") and str(parsed["topic"]).isdigit():
            topic_id = int(parsed["topic"])
        else:
            # ambiguous: when msg_part is "<topic>/<ids>" as in /c/<internal>/<topic>/<ids>
            if parsed.get("msg_part") and "/" in parsed.get("msg_part"):
                segs = parsed["msg_part"].split("/")
                if len(segs) >= 2 and segs[0].isdigit():
                    topic_id = int(segs[0])
                    # ids are taken from last segment (already extracted)
        dlog("resolved chat_id:", chat_id, "topic_id:", topic_id)

        # ensure peer (attempt to resolve now)
        try:
            peer_ok = await self.ensure_peer(chat_id)
            dlog("enqueue ensure_peer result:", peer_ok)
        except Exception as e:
            dlog("enqueue ensure_peer exception:", e)
            peer_ok = False

        await self.queue.put(({"id": chat_id, "title": str(chat_id)}, topic_id, ids, message.chat.id))
        await message.reply_text(f"✅ Queued {len(ids)} messages from {chat_id} (topic filter: {topic_id}) — position #{self.queue.qsize()}", quote=True)

    async def processor(self):
        logger.info("Processor started")
        while not self.stop_flag.is_set():
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            chat_info, topic_id, ids, reply_chat = item
            try:
                await self.process_backup(chat_info, topic_id, ids, reply_chat)
            except Exception:
                logger.exception("process_backup failed")
            finally:
                self.queue.task_done()
        logger.info("Processor stopped")

    async def get_msg_topic_id(self, chat_id: int, msg_obj) -> Optional[int]:
        # Try high-level attributes first
        try:
            if hasattr(msg_obj, "reply_to_top_id") and getattr(msg_obj, "reply_to_top_id", None):
                return int(getattr(msg_obj, "reply_to_top_id"))
            if hasattr(msg_obj, "thread_id") and getattr(msg_obj, "thread_id", None):
                return int(getattr(msg_obj, "thread_id"))
            # nested reply_to
            rt = getattr(msg_obj, "reply_to", None) or getattr(msg_obj, "reply_to_message", None)
            if rt:
                if hasattr(rt, "reply_to_top_id") and getattr(rt, "reply_to_top_id", None):
                    return int(getattr(rt, "reply_to_top_id"))
        except Exception:
            pass
        # fallback: use raw messages.GetMessages to inspect reply_to metadata
        try:
            inpm = raw_types.InputMessageID(id=getattr(msg_obj, "message_id", getattr(msg_obj, "id", None) or getattr(msg_obj, "msg_id", None)))
            raw_res = await self.app.invoke(raw_functions.messages.GetMessages(id=[inpm]))
            if raw_res and getattr(raw_res, "messages", None):
                raw_msg = raw_res.messages[0]
                if hasattr(raw_msg, "reply_to") and raw_msg.reply_to:
                    top = getattr(raw_msg.reply_to, "reply_to_top_id", None)
                    if top:
                        return int(top)
        except Exception as e:
            dlog("get_msg_topic_id raw fallback fail:", e)
        return None

    async def process_backup(self, chat_info: Dict[str, Any], topic_id: Optional[int], ids: List[int], reply_chat: int):
        total = len(ids)
        done = 0
        status_msg = None
        try:
            try:
                status_msg = await self.app.send_message(reply_chat, f"🚀 Starting backup for {chat_info.get('title')} — {total} messages (topic_filter={topic_id})")
            except Exception:
                status_msg = None

            self.current_backup = (chat_info, 0, total, 0)
            for i, mid in enumerate(ids, start=1):
                if self.current_backup is None:
                    if status_msg:
                        await safe_edit(status_msg, "🛑 Backup stopped.")
                    break

                self.current_backup = (chat_info, i, total, done)
                dlog("Processing", mid, "in", chat_info.get("id"))

                # Ensure peer is present before each message attempt (helps fresh sessions)
                try:
                    await self.ensure_peer(chat_info["id"])
                except Exception as e:
                    dlog("process_backup: ensure_peer on message failed", e)

                msg = await self.fetch_message(chat_info["id"], mid)
                dlog("fetch result for", mid, ":", type(msg))
                if not msg:
                    logger.warning("Message %s not found in %s", mid, chat_info.get("id"))
                    if status_msg:
                        await safe_edit(status_msg, f"{i}/{total} processed — done {done} (missing {mid})")
                    await asyncio.sleep(0.5)
                    continue

                # If topic filter provided -> check message's topic id
                if topic_id is not None:
                    try:
                        msg_topic = await self.get_msg_topic_id(chat_info["id"], msg)
                        dlog("msg_topic:", msg_topic, "expected:", topic_id)
                        if msg_topic != topic_id:
                            dlog("Skipping", mid, "not in topic", topic_id)
                            if status_msg:
                                await safe_edit(status_msg, f"{i}/{total} processed — done {done} (skipped {mid} not in topic)")
                            await asyncio.sleep(0.3)
                            continue
                    except Exception:
                        dlog("topic check failed for", mid)
                        if status_msg:
                            await safe_edit(status_msg, f"{i}/{total} processed — done {done} (skipped {mid} topic-check error)")
                        await asyncio.sleep(0.3)
                        continue

                # caption/text
                caption = ""
                try:
                    caption = (getattr(msg, "caption", None) or getattr(msg, "text", None) or "")
                    if caption is None:
                        caption = ""
                except Exception:
                    caption = ""

                # filename hint
                filename_hint = None
                try:
                    if getattr(msg, "document", None):
                        filename_hint = getattr(msg.document, "file_name", None)
                    elif getattr(msg, "video", None):
                        filename_hint = getattr(msg.video, "file_name", None)
                except Exception:
                    filename_hint = None
                if not filename_hint:
                    filename_hint = f"{abs(chat_info.get('id'))}_{mid}"

                # attempt downloads with fallbacks
                downloaded = await self.download_with_fallbacks(msg, chat_info["id"], mid, filename_hint=filename_hint)

                if downloaded and os.path.exists(downloaded):
                    med = self.detect_media_type(msg, downloaded)
                    try:
                        if med.get("is_photo"):
                            await self.app.send_photo(self.dest_channel, downloaded, caption=caption)
                        elif med.get("is_video"):
                            await self.app.send_video(self.dest_channel, downloaded, caption=caption)
                        elif med.get("is_audio"):
                            try:
                                await self.app.send_audio(self.dest_channel, downloaded, caption=caption)
                            except Exception:
                                await self.app.send_document(self.dest_channel, downloaded, caption=caption)
                        else:
                            fname = med.get("filename") or os.path.basename(downloaded)
                            await self.app.send_document(self.dest_channel, downloaded, caption=caption, file_name=fname)
                        done += 1
                    except FloodWait as fw:
                        wait = getattr(fw, "value", None) or getattr(fw, "seconds", None) or 10
                        logger.warning("FloodWait %s sec", wait)
                        await asyncio.sleep(int(wait))
                        try:
                            if med.get("is_photo"):
                                await self.app.send_photo(self.dest_channel, downloaded, caption=caption)
                            elif med.get("is_video"):
                                await self.app.send_video(self.dest_channel, downloaded, caption=caption)
                            else:
                                await self.app.send_document(self.dest_channel, downloaded, caption=caption)
                            done += 1
                        except Exception:
                            logger.exception("retry upload failed")
                    except Exception:
                        logger.exception("upload failed fallback")
                    finally:
                        try:
                            os.remove(downloaded)
                        except Exception:
                            pass
                else:
                    # Not downloadable: try to send caption or placeholder
                    if caption and caption.strip():
                        try:
                            await self.app.send_message(self.dest_channel, caption)
                            done += 1
                        except Exception:
                            logger.exception("send_message failed for text-only")
                    else:
                        placeholder = f"[deleted or non-downloadable message preserved]\nSource: {chat_info.get('title')} ({chat_info.get('id')})\nMsgID: {mid}"
                        try:
                            await self.app.send_message(self.dest_channel, placeholder)
                            done += 1
                        except Exception:
                            try:
                                await self.app.forward_messages(self.dest_channel, chat_info.get("id"), mid)
                                done += 1
                            except Exception:
                                logger.exception("final forward fallback failed")

                if status_msg:
                    await safe_edit(status_msg, f"Processing {i}/{total} — done {done}")

                await asyncio.sleep(random.randint(self.min_delay, self.max_delay))

            if status_msg:
                await safe_edit(status_msg, f"✅ Completed — processed {done}/{total}")
        finally:
            self.current_backup = None

    async def start(self):
        await self.app.start()
        me = await self.app.get_me()
        logger.info("Logged in as: %s", getattr(me, "first_name", getattr(me, "id", str(me))))
        # prefetch dialogs - increased to 2000 to better populate peers for fresh sessions
        try:
            async for _ in self.app.get_dialogs(limit=2000):
                pass
        except Exception:
            pass
        self.register_handlers()
        self.processor_task = asyncio.create_task(self.processor())

    async def stop(self):
        self.stop_flag.set()
        if self.processor_task:
            try:
                await asyncio.wait_for(self.processor_task, timeout=5)
            except Exception:
                pass
        try:
            await self.app.stop()
        except Exception:
            pass

# ---------- helpers ----------
async def safe_edit(msg, text: str):
    try:
        await msg.edit_text(text)
    except Exception:
        try:
            await msg.edit_text(text[:4000])
        except Exception:
            pass

# ---------- entrypoint ----------
async def main():
    bot = BackupBotFinalV2()
    await bot.start()
    # run flask health endpoint
    asyncio.create_task(asyncio.to_thread(app.run, "0.0.0.0", PORT))
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        pass
    finally:
        await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted, exiting")
