
DEST_FILE = "destination.json"

def load_destination(default_dest):
    try:
        with open(DEST_FILE, "r", encoding="utf-8") as f:
            return int(json.load(f).get("dest"))
    except Exception:
        return default_dest

def save_destination(dest):
    with open(DEST_FILE, "w", encoding="utf-8") as f:
        json.dump({"dest": int(dest)}, f)



def _parse_link_or_pending(m):
    parts = m.text.split(maxsplit=1)
    if len(parts) > 1:
        return parse_tg_link(parts[1])
    try:
        with open(PENDING_FILE, "r", encoding="utf-8") as pf:
            data = json.load(pf)
            return data.get("chat_id"), data.get("ids", [])
    except Exception:
        return None, []


# bot_raw_backup_final_v3.py working but not working in topics enabled group 
"""
Final RAW backup bot v2 (patched)
- /tgprobackup -> normal backup
- Robust peer-load fixes for fresh/new session strings:
  * larger dialog preload
  * RAW force-load via channels.GetFullChannel with access_hash=0
  * ensure_peer called before processing each message
"""
# ================= FLOODWAIT SAFE EDIT HELPER =================
from pyrogram.errors import FloodWait
import random

async def safe_edit_message(app, chat_id: int, msg_id: int, text: str) -> bool:
    """
    Safely edit caption or text with FloodWait handling.
    """
    while True:
        try:
            await app.edit_message_caption(chat_id, msg_id, text)
            await asyncio.sleep(random.uniform(1.5, 2.5))
            return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value or fw.seconds or 10)
        except Exception:
            try:
                await app.edit_message_text(chat_id, msg_id, text)
                await asyncio.sleep(random.uniform(1.5, 2.5))
                return True
            except FloodWait as fw:
                await asyncio.sleep(fw.value or fw.seconds or 10)
            except Exception:
                return False


import asyncio
import logging
import os
import random
import re
import sys
import json
from typing import Optional, List, Dict, Any

from flask import Flask
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

# raw
from pyrogram.raw import functions as raw_functions
from pyrogram.raw import types as raw_types

def extract_src_caption(msg):
    """Hybrid SRC extractor (simpler): try HTML then Markdown then fallback to raw caption/text."""
    try:
        # Try HTML
        try:
            cap = getattr(msg, "caption", None)
            if cap:
                html = getattr(cap, "html", None)
                if html:
                    return html
                if isinstance(cap, str) and cap.strip():
                    return cap
        except Exception:
            pass
        try:
            txt = getattr(msg, "text", None)
            if txt:
                html = getattr(txt, "html", None)
                if html:
                    return html
                if isinstance(txt, str) and txt.strip():
                    return txt
        except Exception:
            pass
        # Try markdown
        try:
            cap = getattr(msg, "caption", None)
            if cap:
                md = getattr(cap, "markdown", None)
                if md:
                    return md
        except Exception:
            pass
        try:
            txt = getattr(msg, "text", None)
            if txt:
                md = getattr(txt, "markdown", None)
                if md:
                    return md
        except Exception:
            pass
    except Exception:
        pass
    try:
        return getattr(msg, "caption", None) or getattr(msg, "text", None) or ""
    except Exception:
        return ""



# ---------- config ----------
DEBUG = os.environ.get("DEBUG", "0") == "1"
PORT = int(os.environ.get("PORT", "10000"))


# ---------- watermark / thumbnail config ----------
WATERMARK_PATH = "watermarkstemp.png"
WATERMARK_STATE_FILE = "watermark.json"

def load_watermark_state() -> bool:
    try:
        if os.path.exists(WATERMARK_STATE_FILE):
            with open(WATERMARK_STATE_FILE, "r") as f:
                return bool(json.load(f).get("enabled", True))
    except Exception:
        pass
    return True

def save_watermark_state(enabled: bool):
    try:
        with open(WATERMARK_STATE_FILE, "w") as f:
            json.dump({"enabled": enabled}, f)
    except Exception:
        pass

async def apply_video_watermark(input_path: str) -> str | None:
    try:
        base, ext = os.path.splitext(input_path)
        if not ext:
            ext = ".mp4"

        out = f"{base}_wm{ext}"

        # 🎯 480p cap + white text + soft shadow (top-right)
        vf = (
            "scale='if(gt(ih,480),-2,iw)':'if(gt(ih,480),480,ih)',"
            "drawtext="
            "fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf:"
            "text='EduVision':"
            "fontcolor=white@0.35:"
            "shadowcolor=black@0.45:"
            "shadowx=2:"
            "shadowy=2:"
            "fontsize=h*0.035:"
            "x=w-tw-20:"
            "y=20"
        )

        cmd = [
            "ffmpeg",
            "-y",
            "-loglevel", "error",
            "-i", input_path,
            "-vf", vf,
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "25",
            "-c:a", "copy",
            out
        ]

        dlog("🎨 [WATERMARK] ffmpeg cmd:", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=1800)
        except asyncio.TimeoutError:
            dlog("❌ [WATERMARK] ffmpeg TIMEOUT → skipping watermark")
            proc.kill()
            return None

        if stderr:
            dlog("🎨 [WATERMARK] ffmpeg stderr:", stderr.decode(errors="ignore")[:200])

        if os.path.exists(out) and os.path.getsize(out) > 0:
            return out

    except Exception as e:
        dlog("❌ [WATERMARK] exception:", repr(e))

    return None

async def generate_video_thumbnail(video_path: str, second: int = 10) -> str | None:
    try:
        thumb = video_path.rsplit(".", 1)[0] + "_thumb.jpg"
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(second),
            "-i", video_path,
            "-frames:v", "1",
            "-q:v", "3",
            thumb
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await proc.communicate()
        if os.path.exists(thumb):
            return thumb
    except Exception:
        pass
    return None

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
def extract_src_caption(msg):
    try:
        if DEBUG:
            try:
                dlog("extract_full_caption: raw caption object:", getattr(msg, "caption", None))
                dlog("extract_full_caption: raw text object:", getattr(msg, "text", None))
                dlog("extract_full_caption: caption_entities:", getattr(msg, "caption_entities", None))
            except Exception:
                pass
    except Exception:
        pass

    try:
        # 1) Prefer caption.markdown or caption string, else text.markdown or text string
        try:
            cap_obj = getattr(msg, "caption", None)
            if cap_obj:
                # some pyrogram versions expose .markdown on caption/text objects
                cap_text = getattr(cap_obj, "markdown", None)
                if cap_text is None:
                    # if it's a MessageEntity or plain string
                    cap_text = str(cap_obj)
                if cap_text and str(cap_text).strip():
                    return cap_text
        except Exception:
            pass
        try:
            text_obj = getattr(msg, "text", None)
            if text_obj:
                txt = getattr(text_obj, "markdown", None)
                if txt is None:
                    txt = str(text_obj)
                if txt and str(txt).strip():
                    return txt
        except Exception:
            pass

        # 2) If no caption/text, try to use reply_to_message (object) or fetch parent via id
        parent = getattr(msg, "reply_to_message", None)
        parent_id = getattr(msg, "reply_to_message_id", None)
        if parent is None and parent_id:
            # attempt to fetch parent via available clients
            try:
                # try to use msg._client if available (some wrappers set it)
                client = getattr(msg, "_client", None) or globals().get("app", None)
                if client and hasattr(client, "get_messages"):
                    # msg.chat can be Chat object or chat id
                    chat = getattr(msg, "chat", None)
                    cid = None
                    if chat and hasattr(chat, "id"):
                        cid = chat.id
                    else:
                        cid = getattr(msg, "chat_id", None) or getattr(msg, "chat", None)
                    if cid is not None:
                        parent = client.get_messages(cid, int(parent_id))
                        # client.get_messages may be coroutine or return Message; handle both
                        if hasattr(parent, "__await__"):
                            parent = parent.__await__().__next__()
            except Exception:
                parent = None

        if parent:
            try:
                pcap = getattr(parent, "caption", None)
                if pcap:
                    pcap_text = getattr(pcap, "markdown", None) or str(pcap)
                    if pcap_text and str(pcap_text).strip():
                        quoted = "\\n".join(["> " + line for line in str(pcap_text).splitlines()])
                        return quoted
                ptext = getattr(parent, "text", None)
                if ptext:
                    ptext_text = getattr(ptext, "markdown", None) or str(ptext)
                    if ptext_text and str(ptext_text).strip():
                        quoted = "\\n".join(["> " + line for line in str(ptext_text).splitlines()])
                        return quoted
            except Exception:
                pass

        # 3) fallback: try existing caption_entities extraction on available raw text
        try:
            entities = getattr(msg, "caption_entities", None) or getattr(msg, "entities", None) or []
            raw = (getattr(msg, "caption", None) or getattr(msg, "text", None) or "") or ""
            raw = raw if isinstance(raw, str) else str(raw)
            for ent in entities:
                try:
                    t = getattr(ent, "type", None)
                    if t in ("blockquote", "citation", "quoted"):
                        off = getattr(ent, "offset", None)
                        ln = getattr(ent, "length", None)
                        if off is not None and ln is not None and off + ln <= len(raw):
                            return raw[off:off+ln]
                except Exception:
                    continue
        except Exception:
            pass

        return ""
    except Exception:
        try:
            return getattr(msg, "caption", None) or getattr(msg, "text", None) or ""
        except Exception:
            return ""

def extract_src_caption(msg):
    try:
        if DEBUG:
            try:
                dlog("extract_full_caption: caption:", getattr(msg, "caption", None))
                dlog("extract_full_caption: text:", getattr(msg, "text", None))
                dlog("extract_full_caption: caption_entities:", getattr(msg, "caption_entities", None))
                dlog("extract_full_caption: entities:", getattr(msg, "entities", None))
            except Exception:
                pass
    except Exception:
        pass

    try:
        # Prefer caption or text on the message itself
        raw = msg.caption if getattr(msg, "caption", None) is not None else (msg.text if getattr(msg, "text", None) is not None else "")
        raw = "" if raw is None else str(raw)
        if raw and raw.strip():
            return raw

        # If no caption/text, try to use reply_to_message if present (nested in msg)
        parent = getattr(msg, "reply_to_message", None)
        if parent is None:
            # sometimes only id present; try reply_to_message_id (but cannot fetch here synchronously)
            parent_id = getattr(msg, "reply_to_message_id", None)
            if parent_id is not None:
                # best-effort: see if msg has attribute to fetch nested parent (some libs include it)
                try:
                    parent = getattr(msg, "_reply_to_message", None)
                except Exception:
                    parent = None

        if parent:
            try:
                pcap = getattr(parent, "caption", None) or getattr(parent, "text", None) or ""
                pcap = "" if pcap is None else str(pcap)
                if pcap.strip():
                    quoted = "\n".join(["> " + line for line in pcap.splitlines()])
                    return quoted
            except Exception:
                pass

        # Fallback: try to reconstruct from caption_entities if they reference text within raw
        try:
            entities = getattr(msg, "caption_entities", None) or getattr(msg, "entities", None) or []
            if entities:
                raw2 = raw or ""
                base = list(raw2)
                shift = 0
                for ent in entities:
                    try:
                        t = getattr(ent, "type", None)
                        if t in ("blockquote", "citation", "quoted"):
                            off = getattr(ent, "offset", None)
                            ln = getattr(ent, "length", None)
                            if off is not None and ln is not None and off + ln <= len(raw2):
                                part = raw2[off:off+ln]
                                insert_at = off + shift
                                ins = list(part)
                                base[insert_at:insert_at] = ins + ["\n"]
                                shift += len(ins) + 1
                            else:
                                fulltext = getattr(msg, "text", None) or ""
                                if fulltext and off is not None and ln is not None and off + ln <= len(fulltext):
                                    part = fulltext[off:off+ln]
                                    base.extend(["\n"] + list(part))
                    except Exception:
                        continue
                rebuilt = "".join(base).rstrip("\n")
                if rebuilt:
                    return rebuilt
        except Exception:
            pass

        return ""
    except Exception:
        try:
            return msg.caption or msg.text or ""
        except Exception:
            return ""

# ---------- dynamic filters helpers ----------
FILTERS_FILE = "filters.json"
PENDING_FILE = "filters_pending.json"

def load_filters() -> dict:
    try:
        if not os.path.exists(FILTERS_FILE):
            return {}
        with open(FILTERS_FILE, "r", encoding="utf-8") as _f:
            data = json.load(_f)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
    except Exception as e:
        dlog("load_filters failed:", e)
    return {}

def save_filters(filters: dict):
    try:
        with open(FILTERS_FILE, "w", encoding="utf-8") as _f:
            json.dump(filters, _f, ensure_ascii=False, indent=2)
    except Exception as e:
        dlog("save_filters failed:", e)

def apply_filters(text: str, filters: dict) -> str:
    if text is None:
        return ""
    out = str(text)
    # sort keys by length desc to avoid partial overlapping replacements
    for bad in sorted(filters.keys(), key=lambda s: -len(s)):
        rep = filters.get(bad, "")
        try:
            pattern = re.compile(re.escape(bad), re.IGNORECASE)
            out = pattern.sub(rep, out)
        except Exception:
            try:
                out = out.replace(bad, rep)
            except Exception:
                dlog("apply_filters replace error for", bad)
    return out

# ensure filters.json exists
try:
    if not os.path.exists(FILTERS_FILE):
        with open(FILTERS_FILE, "w", encoding="utf-8") as _f:
            json.dump({}, _f)
except Exception:
    pass

# ensure pending file exists (empty)
try:
    if not os.path.exists(PENDING_FILE):
        with open(PENDING_FILE, "w", encoding="utf-8") as _f:
            json.dump({}, _f)
except Exception:
    pass

# ---------- filters (dynamic) ----------
FILTERS_FILE = "filters.json"

def load_filters() -> dict:
    try:
        if not os.path.exists(FILTERS_FILE):
            return {}
        with open(FILTERS_FILE, "r", encoding="utf-8") as _f:
            data = json.load(_f)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
    except Exception as e:
        dlog("load_filters failed:", e)
    return {}

def save_filters(filters: dict):
    try:
        with open(FILTERS_FILE, "w", encoding="utf-8") as _f:
            json.dump(filters, _f, ensure_ascii=False, indent=2)
    except Exception as e:
        dlog("save_filters failed:", e)

def apply_filters(text: str, filters: dict) -> str:
    if text is None:
        return ""
    out = str(text)
    # sort keys by length desc to avoid partial overlapping replacements
    for bad in sorted(filters.keys(), key=lambda s: -len(s)):
        rep = filters.get(bad, "")
        try:
            # case-insensitive replacement: replace all casings by using regex with re.IGNORECASE
            pattern = re.compile(re.escape(bad), re.IGNORECASE)
            out = pattern.sub(rep, out)
        except Exception:
            try:
                out = out.replace(bad, rep)
            except Exception:
                dlog("apply_filters replace error for", bad)
    return out

# Ensure filters.json exists (created on startup if not present)
try:
    if not os.path.exists(FILTERS_FILE):
        with open(FILTERS_FILE, "w", encoding="utf-8") as _f:
            json.dump({}, _f)
except Exception:
    pass

# ---------- bot ----------
class BackupBotFinalV2:
    def __init__(self):
        try:
            self.api_id = int(os.environ["API_ID"])
            self.api_hash = os.environ["API_HASH"]
            self.dest_channel = load_destination( int(os.environ["DESTINATION_CHANNEL"])
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
        # dynamic filters loaded at start
        self.filters = load_filters()

        # watermark state
        self.watermark_enabled = load_watermark_state()

        # dynamic filters loaded at start
        self.filters = load_filters()

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
        2) download by file_id (document/photo/video)
        3) forward to "me" and download forwarded message
        4) re-fetch high-level message and download
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

        try:
            dest = filename_hint or f"{abs(chat_id)}_{msg_id}"
            if path:
                dlog("📁 [DOWNLOAD] file saved at", path, "exists?", os.path.exists(path))
                try:
                    dlog("📦 [DOWNLOAD] file size:", os.path.getsize(path))
                except Exception as e:
                    dlog("❌ [DOWNLOAD] getsize failed:", e)
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
                dest_name = fname or filename_hint or f"{abs(chat_id)}_{msg_id}"
                if path:
                    dlog("📁 [DOWNLOAD] file saved at", path, "exists?", os.path.exists(path))
                try:
                    dlog("📦 [DOWNLOAD] file size:", os.path.getsize(path))
                except Exception as e:
                    dlog("❌ [DOWNLOAD] getsize failed:", e)
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

        self._handlers_registered = True

        @self.app.on_message(filters.command("tgprostart"))
        async def tgprostart(c, m: Message):
            if not m.from_user or m.from_user.id != self.owner_id:

            if m.chat and m.chat.type == "private":
                await m.reply_text("✅ Pyrogram RAW Backup final v2 online (owner verified).")
            else:

        owner_only = filters.create(lambda _, __, m: m.from_user and m.from_user.id == self.owner_id)

        @self.app.on_message(filters.command("tgdebug") & owner_only)
        async def tgdebug_cmd(c, m: Message):
            # usage: /tgdebug <link> - owner-only; prints safe summary to owner; full raw dump to Render logs only
            if len(m.command) < 2:
                await m.reply_text("Usage: /tgdebug <link>", quote=True)

            link = m.command[1].strip()
            await m.reply_text("🔎 Gathering debug info (check Render logs)...", quote=True)
            parsed = parse_link(link)
            if not parsed:
                await m.reply_text("❌ Invalid link format.", quote=True)

            ids = extract_ids_from_link(link)
            if not ids:
                await m.reply_text("❌ Could not extract message ids.", quote=True)

            results = []
            for mid in ids:
                try:
                    # determine chat_id
                    if parsed.get('kind') == 'internal':
                        chat_id = int(f"-100{parsed['root']}")
                    else:
                        chat_obj = await self.app.get_chat(parsed['root'])
                        chat_id = chat_obj.id
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        results.append((mid, "missing"))
                        dlog("tgdebug: message missing", chat_id, mid)
                        continue
                    # safe summary
                    summary = {
                        "mid": mid,
                        "chat_id": chat_id,
                        "caption_present": bool(getattr(msg, "caption", None)),
                        "text_present": bool(getattr(msg, "text", None)),
                        "caption": (getattr(msg, "caption", None) or "")[:1000],
                        "text": (getattr(msg, "text", None) or "")[:1000],
                        "caption_entities": repr(getattr(msg, "caption_entities", None)),
                        "entities": repr(getattr(msg, "entities", None))
                    }
                    # log full raw message to Render logs (not sent to telegram)
                    try:
                        dlog("TGDEBUG RAW MESSAGE:", repr(msg))
                        logger.info("TGDEBUG RAW MSG REPR: %s", repr(msg))
                    except Exception as e:
                        dlog("tgdebug raw log failed:", e)
                    results.append((mid, "ok", summary))
                except Exception as e:
                    dlog("tgdebug loop error:", e)
                    results.append((mid, "error", str(e)))
            lines = []
            for item in results:
                if item[1] == "missing":
                    lines.append(f"{item[0]} — missing")
                elif item[1] == "error":
                    lines.append(f"{item[0]} — error: {item[2]}")
                else:
                    lines.append(f"{item[0]} — caption_present={item[2]['caption_present']} text_present={item[2]['text_present']} entities={item[2]['caption_entities']}")
            msg_text = "Debug summary:\\n" + "\\n".join(lines)
            try:
                await self.app.send_message(self.owner_id, msg_text)
            except Exception:
                try:
                    await m.reply_text("Could not DM owner; here's summary:\\n" + msg_text, quote=True)
                except Exception:
                    pass

        # ---- Extra commands (forward / filters) ----
        
        @self.app.on_message(filters.command("watermark") & owner_only)
        async def watermark_cmd(c, m: Message):
            if len(m.command) < 2:
                status = "ON ✅" if self.watermark_enabled else "OFF ❌"
                await m.reply_text(
                    f"🎥 Watermark status: {status}\n\nUse:\n`/watermark on`\n`/watermark off`",
                    quote=True
                )

            arg = m.command[1].lower()
            if arg == "on":
                self.watermark_enabled = True
                save_watermark_state(True)
                await m.reply_text("✅ Watermark ENABLED", quote=True)
            elif arg == "off":
                self.watermark_enabled = False
                save_watermark_state(False)
                await m.reply_text("❌ Watermark DISABLED", quote=True)
            else:
                await m.reply_text("❌ Use on/off", quote=True)


        @self.app.on_message(filters.command("tgproforward") & owner_only)
        async def tgproforward_cmd(c, m: Message):

            # usage: /tgproforward <link>
            if len(m.command) < 2:
                await m.reply_text("Usage: /tgproforward <link>", quote=True)

            link = m.command[1].strip()
            dlog("tgproforward invoked by", getattr(m.from_user, "id", None), "link:", link)
            await m.reply_text(f"🔍 Processing forward: `{link}`", quote=True)
            parsed = parse_link(link)
            if not parsed:
                await m.reply_text("❌ Invalid link format.", quote=True)

            ids = extract_ids_from_link(link)
            if not ids:
                await m.reply_text("❌ Could not extract message ids.", quote=True)

            try:
                if parsed.get('kind') == 'internal':
                    chat_id = int(f"-100{parsed['root']}")
                else:
                    chat_obj = await self.app.get_chat(parsed['root'])
                    chat_id = chat_obj.id
            except Exception as e:
                await m.reply_text("❌ Chat not found.", quote=True)

            forwarded = 0
            failures = 0
            for mid in ids:
                try:
                    dlog("tgproforward: processing", chat_id, mid)
                    msg_obj = await self.fetch_message(chat_id, mid)
                    if not msg_obj:
                        failures += 1
                        dlog("tgproforward: msg not found", mid)
                        continue
                    # Detect protected content at chat or message level
                    is_protected = False
                    try:
                        is_protected = bool(getattr(msg_obj.chat, "has_protected_content", False) or getattr(msg_obj, "has_protected_content", False))
                    except Exception:
                        is_protected = False
                    if is_protected:
                        dlog("tgproforward: protected content, using re-upload for", mid)
                        try:
                            cap = extract_src_caption(msg_obj)
                            cap = apply_filters(cap, self.filters)
                            filename_hint = None
                            try:
                                if getattr(msg_obj, "document", None):
                                    filename_hint = getattr(msg_obj.document, "file_name", None)
                                elif getattr(msg_obj, "video", None):
                                    filename_hint = getattr(msg_obj.video, "file_name", None)
                            except Exception:
                                filename_hint = None
                            path = await self.download_with_fallbacks(msg_obj, chat_id, mid, filename_hint=filename_hint)
                            if path and os.path.exists(path):
                                med = self.detect_media_type(msg_obj, path)
                                try:
                                    # [CLEANED] legacy media handling removed
                                    pass
                                except Exception as e_send:
                                    failures += 1
                                    dlog("tgproforward: re-upload send failed:", e_send)
                            else:
                                failures += 1
                                dlog("tgproforward: download failed for re-upload", mid)
                        except Exception as e_reup:
                            failures += 1
                            dlog("tgproforward: re-upload fallback error:", e_reup)
                        continue
                    # Not protected -> use copy_message to hide sender and preserve media/caption
                    try:
                        await self.app.copy_message(self.dest_channel, chat_id, mid)
                        forwarded += 1
                        dlog("tgproforward: copy_message succeeded for", mid)
                        await asyncio.sleep(random.uniform(1.0, 2.0))
                    except Exception as e_copy:
                        failures += 1
                        dlog("tgproforward: copy_message failed for", mid, e_copy)
                except Exception as e:
                    failures += 1
                    dlog("tgproforward loop error:", e)
            await m.reply_text(f"✅ Forwarded {forwarded}, failures reported {failures}", quote=True)

        @self.app.on_message(filters.command("tgprofilters") & owner_only)
        async def tgprofilters_cmd(c, m: Message):
            # usage: /tgprofilters <link>
            if len(m.command) < 2:
                await m.reply_text("Usage: /tgprofilters <link>", quote=True)

            link = m.command[1].strip()
            parsed = parse_link(link)
            if not parsed:
                await m.reply_text("❌ Invalid link format.", quote=True)

            ids = extract_ids_from_link(link)
            if not ids:
                await m.reply_text("❌ Could not extract message ids.", quote=True)

            try:
                if parsed.get('kind') == 'internal':
                    chat_id = int(f"-100{parsed['root']}")
                else:
                    chat_obj = await self.app.get_chat(parsed['root'])
                    chat_id = chat_obj.id
            except Exception as e:
                await m.reply_text("❌ Chat not found.", quote=True)

            preview = []
            owner_me = await self.app.get_me()
            for mid in ids:
                try:
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        preview.append((mid, "missing", None))
                        continue
                    if not getattr(msg, 'from_user', None) or getattr(msg.from_user, 'id', None) != owner_me.id:
                        preview.append((mid, "skip_not_owner", None))
                        continue
                    cap = extract_src_caption(msg)
                    new_cap = apply_filters(cap, self.filters)
                    if new_cap != (cap or ""):
                        preview.append((mid, "will_change", new_cap))
                    else:
                        preview.append((mid, "no_change", None))
                except Exception as e:
                    dlog("tgprofilters preview error:", e)
                    preview.append((mid, "error", None))
            try:
                with open(PENDING_FILE, "w", encoding="utf-8") as pf:
                    json.dump({"chat_id": chat_id, "ids": ids, "preview": preview}, pf, ensure_ascii=False, indent=2)
            except Exception:
                pass
            lines = []
            for mid, status, info in preview:
                if status == "missing":
                    lines.append(f"{mid} — missing")
                elif status == "skip_not_owner":
                    lines.append(f"{mid} — skip (not owner)")
                elif status == "will_change":
                    lines.append(f"{mid} — will change to: {info[:200]}")
                elif status == "no_change":
                    lines.append(f"{mid} — no change")
                else:
                    lines.append(f"{mid} — error")
            await m.reply_text("Preview:\\n" + "\\n".join(lines) + "\\n\\nConfirm with /tgprofilters_apply", quote=True)

            chat_id = data.get("chat_id")
            ids = data.get("ids", [])
            reposted = 0

            for mid in ids:
                try:
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        continue

                    cap = extract_src_caption(msg)
                    new_cap = apply_filters(cap, self.filters)

                    filename_hint = None
                    if getattr(msg, "document", None):
                        filename_hint = msg.document.file_name
                    elif getattr(msg, "video", None):
                        filename_hint = msg.video.file_name

                    path = await self.download_with_fallbacks(
                        msg, chat_id, mid, filename_hint=filename_hint
                    )

                    if path and os.path.exists(path):
                        med = self.detect_media_type(msg, path)

                        # [CLEANED] legacy media handling removed
                        pass

                        reposted += 1
                        try:
                            os.remove(path)
                        except Exception:
                            pass

                        await asyncio.sleep(random.uniform(1.2, 2.0))

                    else:
                        if new_cap.strip():
                            await self.app.send_message(self.dest_channel, new_cap)
                            reposted += 1

                except FloodWait as fw:
                    await asyncio.sleep(fw.value or fw.seconds or 10)
                except Exception as e:
                    dlog("tgprofilters_apply_cp error:", e)

            await m.reply_text(f"✅ Reposted (copy-paste) messages: {reposted}", quote=True)

        async def tgprofilters_apply_cmd(c, m: Message):
            try:
                with open(PENDING_FILE, "r", encoding="utf-8") as pf:
                    data = json.load(pf)
            except Exception:
                await m.reply_text("No pending preview found.", quote=True)

            chat_id = data.get("chat_id")
            ids = data.get("ids", [])
            applied = 0
            owner_me = await self.app.get_me()
            for mid in ids:
                try:
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        continue
                    if not getattr(msg, 'from_user', None) or getattr(msg.from_user, 'id', None) != owner_me.id:
                        continue
                    cap = extract_src_caption(msg)
                    new_cap = apply_filters(cap, self.filters)
                    if new_cap != (cap or ""):
                        edited = await safe_edit_message(self.app, chat_id, mid, new_cap)
                        if edited:
                            applied += 1
                        else:
                            dlog("edit failed permanently for", mid)
                        applied += 1
                except Exception as e:
                    dlog("tgprofilters apply error:", e)
            await m.reply_text(f"✅ Applied edits: {applied}", quote=True)

        # ---- Filter management commands (owner only) ----
        @self.app.on_message(filters.command("addfilter") & owner_only)
        async def addfilter_cmd(c, m: Message):
            if len(m.command) < 3:
                await m.reply_text("Usage: /addfilter <bad> <replacement>\nUse "" (two quotes) as replacement to remove the word.", quote=True)

            bad = m.command[1]
            rep = " ".join(m.command[2:])
            if rep in ('""', "''"):
                rep = ""
            try:
                self.filters[bad] = rep
                save_filters(self.filters)
                await m.reply_text(f"✅ Added filter: `{bad}` → `{rep}`", quote=True)
            except Exception as e:
                dlog("addfilter failed:", e)
                await m.reply_text("❌ Failed to add filter.", quote=True)

        @self.app.on_message(filters.command("listfilters") & owner_only)
        async def listfilters_cmd(c, m: Message):
            if not self.filters:
                await m.reply_text("No filters set.", quote=True)

            lines = [f"`{k}` → `{v}`" for k, v in self.filters.items()]
            out = "Current filters:\n\n" + "\n".join(lines)
            await m.reply_text(out, quote=True)

        @self.app.on_message(filters.command("tgprobackup") & owner_only)
        async def backup_cmd(c, m: Message):
            if len(m.command) < 2:
                await m.reply_text("❌ Provide link. Example: /tgprobackup https://t.me/c/<internal>/<topic?>/<start>-<end>", quote=True)

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

        # Extract ids
        ids = extract_ids_from_link(link)
        dlog("ids:", ids)
        if not ids:
            await message.reply_text("❌ Could not extract message ids. Provide numeric or range in the link.", quote=True)

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
                try:
                    if DEBUG:
                        dlog("PROCESS_BACKUP MSG PREVIEW:", "caption:", getattr(msg, "caption", None), "text:", getattr(msg, "text", None))
                        dlog("PROCESS_BACKUP ENTITIES:", getattr(msg, "caption_entities", None), getattr(msg, "entities", None))
                except Exception:
                    pass

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
                caption = extract_src_caption(msg)
                try:
                    caption = (getattr(msg, "caption", None) or getattr(msg, "text", None) or "")
                    if caption is None:
                        caption = ""
                except Exception:
                    caption = ""

                
                # apply dynamic filters to caption (and later to filename)
                try:
                    caption = apply_filters(caption, self.filters)
                except Exception as e:
                    dlog("apply_filters caption failed:", e)
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

                # apply dynamic filters to filename_hint
                try:
                    filename_hint = apply_filters(filename_hint or "", self.filters)
                except Exception as e:
                    dlog("apply_filters filename failed:", e)
                # attempt downloads with fallbacks
                
                # Auto-forward if allowed (hide sender) - skip download logic
                try:
                    if hasattr(msg, "has_protected_content") and not msg.has_protected_content:
                        try:
                            await self.app.forward_messages(self.dest_channel, chat_info["id"], mid, protect_content=False)
                            done += 1
                            if status_msg:
                                await safe_edit(status_msg, f"Processing {i}/{total} — forwarded {mid} (no protection) — done {done}")
                            await asyncio.sleep(random.randint(self.min_delay, self.max_delay))
                            continue
                        except Exception as e:
                            dlog("auto-forward failed (send forward):", e)
                except Exception as e:
                    dlog("auto-forward check failed:", e)
                downloaded = await self.download_with_fallbacks(msg, chat_info["id"], mid, filename_hint=filename_hint)

                if downloaded and os.path.exists(downloaded):
                    med = self.detect_media_type(msg, downloaded)
                    try:
                        # [CLEANED] legacy media handling removed
                        pass
                        
                        # [CLEANED] legacy media handling removed
                        pass

                        # [CLEANED] legacy media handling removed
                        pass
                    except FloodWait as fw:
                        wait = getattr(fw, "value", None) or getattr(fw, "seconds", None) or 10
                        logger.warning("FloodWait %s sec", wait)
                        await asyncio.sleep(int(wait))

                        try:
                            # [CLEANED] legacy media handling removed
                            pass

                            # [CLEANED] legacy media handling removed
                            pass

                                if self.watermark_enabled:
                                    dlog("🎨 [WATERMARK] applying watermark to", downloaded)
                                    wm = await apply_video_watermark(downloaded)
                                    dlog("🎨 [WATERMARK] watermark output:", wm)

                                    if wm:
                                        final_video = wm

                                dlog("🖼️ [THUMBNAIL] generating thumbnail at 10s for", final_video)
                                thumb = await generate_video_thumbnail(final_video, second=10)
                                dlog("🖼️ [THUMBNAIL] thumbnail path:", thumb)

                                dlog("⬆️ [UPLOAD] sending video", final_video)
                                    self.dest_channel,
                                    final_video,
                                    caption=caption,
                                    thumb=thumb
                                )

                                if final_video != downloaded:
                                    try:
                                        os.remove(final_video)
                                    except Exception:
                                        pass

                                if thumb:
                                    try:
                                        os.remove(thumb)
                                    except Exception:
                                        pass

                            # [CLEANED] legacy media handling removed
                            pass

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


        # --- OPTIONAL PREVIEW APPLY (NO PREVIEW REQUIRED) ---

            applied = 0
            for mid in ids:
                try:
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        continue

                    cap = extract_src_caption(msg)
                    new_cap = apply_filters(cap, self.filters)
                    if cap == new_cap:
                        continue

                    edited = await safe_edit_message(self.app, chat_id, mid, new_cap)
                    if edited:
                        applied += 1

                except FloodWait as fw:
                    await asyncio.sleep(fw.value or fw.seconds or 10)
                except Exception as e:
                    dlog("tgprofilters_apply error:", e)

            await m.reply_text(f"✅ Applied edits: {applied}", quote=True)

        @self.app.on_message(filters.command("tgprofilters_apply_cp") & owner_only)
        async def tgprofilters_apply_cp_cmd(c, m: Message):
            chat_id, ids = _parse_link_or_pending(m)
            if not chat_id or not ids:
                await m.reply_text("No link/range provided and no pending preview found.", quote=True)

            reposted = 0
            for mid in ids:
                try:
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        continue

                    cap = extract_src_caption(msg)
                    new_cap = apply_filters(cap, self.filters)

                    filename_hint = None
                    if getattr(msg, "document", None):
                        filename_hint = msg.document.file_name
                    elif getattr(msg, "video", None):
                        filename_hint = msg.video.file_name

                    path = await self.download_with_fallbacks(
                        msg, chat_id, mid, filename_hint=filename_hint
                    )

                    if path and os.path.exists(path):
                        med = self.detect_media_type(msg, path)

                        # [CLEANED] legacy media handling removed
                        pass

                        reposted += 1
                        try:
                            os.remove(path)
                        except Exception:
                            pass

                        await asyncio.sleep(random.uniform(1.2, 2.0))

                    else:
                        if new_cap.strip():
                            await self.app.send_message(self.dest_channel, new_cap)
                            reposted += 1

                except FloodWait as fw:
                    await asyncio.sleep(fw.value or fw.seconds or 10)
                except Exception as e:
                    dlog("tgprofilters_apply_cp error:", e)

            await m.reply_text(f"✅ Reposted (copy-paste) messages: {reposted}", quote=True)



        @self.app.on_message(filters.command("setdestination") & owner_only)
        async def setdestination_cmd(c, m: Message):
            if len(m.command) < 2:
                cur = load_destination(self.dest_channel)
                await m.reply_text(
                    f"📍 Current destination: `{cur}`\n"
                    "Usage: /setdestination <chat_id>",
                    quote=True
                )

            try:
                dest = int(m.command[1])
            except Exception:
                await m.reply_text("❌ Invalid chat_id.", quote=True)

            try:
                # Test permission by sending a silent message
                test = await self.app.send_message(dest, "✅ Destination set", disable_notification=True)
                await test.delete()
            except Exception as e:
                await m.reply_text("❌ Bot has no permission to post there.", quote=True)

            save_destination(dest)
            self.dest_channel = dest
            await m.reply_text(f"✅ Destination updated to `{dest}`", quote=True)



        # ================= FINAL: EDIT ORIGINAL POSTS ONLY =================
        @self.app.on_message(filters.command("tgprofilters_apply_edit") & owner_only)
        async def tgprofilters_apply_edit_cmd(c, m: Message):
            chat_id, ids = _parse_link_or_pending(m)
            if not chat_id or not ids:
                await m.reply_text("No link/range provided.", quote=True)

            applied = 0
            for mid in ids:
                try:
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        continue

                    cap = extract_src_caption(msg)
                    new_cap = apply_filters(cap, self.filters)

                    if cap == new_cap:
                        continue

                    if await safe_edit_message(self.app, chat_id, mid, new_cap):
                        applied += 1

                except FloodWait as fw:
                    await asyncio.sleep(fw.value or fw.seconds or 10)
                except Exception as e:
                    dlog("tgprofilters_apply_edit error:", e)

            await m.reply_text(f"✅ Edited original messages: {applied}", quote=True)

        # ================= FINAL: COPY → EDIT (NO DOWNLOAD / NO FALLBACK) =================
        @self.app.on_message(filters.command("tgprofilters_apply_cp") & owner_only)
        async def tgprofilters_apply_cp_cmd(c, m: Message):
            chat_id, ids = _parse_link_or_pending(m)
            if not chat_id or not ids:
                await m.reply_text("No link/range provided.", quote=True)

            dest = load_destination(self.dest_channel)
            reposted = 0

            for mid in ids:
                try:
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        continue

                    cap = extract_src_caption(msg)
                    new_cap = apply_filters(cap, self.filters)

                    # COPY ONLY (preserves thumbnail)
                    new_msg = await msg.copy(dest)

                    # EDIT AFTER COPY (apply filters)
                    if new_cap != cap:
                        await safe_edit_message(self.app, dest, new_msg.id, new_cap)

                    reposted += 1
                    await asyncio.sleep(random.uniform(1.5, 2.5))

                except FloodWait as fw:
                    await asyncio.sleep(fw.value or fw.seconds or 10)
                except Exception as e:
                    # No fallback by design
                    dlog("tgprofilters_apply_cp copy-skip:", e)

            await m.reply_text(f"✅ Reposted (copy-paste) messages: {reposted}", quote=True)



        # ================= CLEAN FINAL: EDIT ORIGINAL POSTS ONLY =================
        @self.app.on_message(filters.command("tgprofilters_apply_edit") & owner_only)
        async def tgprofilters_apply_edit_cmd(c, m: Message):
            chat_id, ids = _parse_link_or_pending(m)
            if not chat_id or not ids:
                await m.reply_text("No link/range provided.", quote=True)

            applied = 0
            for mid in ids:
                try:
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        continue

                    cap = extract_src_caption(msg)
                    new_cap = apply_filters(cap, self.filters)

                    if cap == new_cap:
                        continue

                    if await safe_edit_message(self.app, chat_id, mid, new_cap):
                        applied += 1

                except FloodWait as fw:
                    await asyncio.sleep(fw.value or fw.seconds or 10)
                except Exception as e:
                    dlog("tgprofilters_apply_edit error:", e)

            await m.reply_text(f"✅ Edited original messages: {applied}", quote=True)

        # ================= CLEAN FINAL: COPY → EDIT ONLY (NO DOWNLOAD) =================
        @self.app.on_message(filters.command("tgprofilters_apply_cp") & owner_only)
        async def tgprofilters_apply_cp_cmd(c, m: Message):
            chat_id, ids = _parse_link_or_pending(m)
            if not chat_id or not ids:
                await m.reply_text("No link/range provided.", quote=True)

            dest = load_destination(self.dest_channel)
            reposted = 0

            for mid in ids:
                try:
                    msg = await self.fetch_message(chat_id, mid)
                    if not msg:
                        continue

                    cap = extract_src_caption(msg)
                    new_cap = apply_filters(cap, self.filters)

                    new_msg = await msg.copy(dest)

                    if new_cap != cap:
                        await safe_edit_message(self.app, dest, new_msg.id, new_cap)

                    reposted += 1
                    await asyncio.sleep(random.uniform(1.5, 2.5))

                except FloodWait as fw:
                    await asyncio.sleep(fw.value or fw.seconds or 10)
                except Exception as e:
                    dlog("tgprofilters_apply_cp skip:", e)

            await m.reply_text(f"✅ Reposted (copy-paste) messages: {reposted}", quote=True)
