"""
Standalone Text Watermark Bot
- Adds text watermark to videos
- Auto-compression for large files
- Works in groups and private chats
"""

import os
import asyncio
import json
import logging
from typing import Optional
from flask import Flask
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait

# ---------- CONFIG ----------
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PORT = int(os.environ.get("PORT", 10000))
DEBUG = os.environ.get("DEBUG", "0") == "1"

# Text watermark settings
DEFAULT_WATERMARK_TEXT = os.environ.get("WATERMARK_TEXT", "@YourChannel")
WATERMARK_STATE_FILE = "watermark_state.json"
MAX_VIDEO_SIZE_MB = 2000

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("text_watermark_bot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# ---------- FLASK APP ----------
app = Flask(__name__)

@app.route("/")
def home():
    return "Text Watermark Bot Running"

@app.route("/health")
def health():
    return "OK"

# ---------- WATERMARK STATE ----------
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

# ---------- FFMPEG FUNCTIONS ----------
async def add_text_watermark(input_path: str, watermark_text: str, output_path: str = None, font_size: int = 30) -> Optional[str]:
    """Add text watermark to video using ffmpeg drawtext"""
    try:
        if not output_path:
            base, ext = os.path.splitext(input_path)
            output_path = f"{base}_watermarked{ext}"
        
        # Simpler command that definitely works
        cmd = [
            "ffmpeg",
            "-y",
            "-i", input_path,
            "-vf", f"drawtext=text='{watermark_text}':fontcolor=white:fontsize={font_size}:x=10:y=10",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-crf", "28",
            "-c:a", "copy",
            output_path
        ]
        
        logger.info(f"🎨 Running ffmpeg...")
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            logger.error(f"❌ FFmpeg error: {stderr.decode()[:500]}")
            return None
        
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            logger.info(f"✅ Watermark added")
            return output_path
        else:
            logger.error("❌ Output file missing")
            return None
            
    except Exception as e:
        logger.error(f"❌ Exception: {e}")
        return None

async def generate_video_thumbnail(video_path: str, second: int = 10) -> Optional[str]:
    """Generate thumbnail from video"""
    try:
        thumb_path = video_path.rsplit(".", 1)[0] + "_thumb.jpg"
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(second),
            "-i", video_path,
            "-frames:v", "1",
            "-q:v", "3",
            thumb_path
        ]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()
        
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            return thumb_path
    except Exception as e:
        logger.error(f"Thumbnail failed: {e}")
    return None

async def compress_video(input_path: str, target_size_mb: int = 50) -> Optional[str]:
    """Compress video"""
    try:
        duration_cmd = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_path
        ]
        proc = await asyncio.create_subprocess_exec(
            *duration_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        duration = float(stdout.decode().strip())
        
        if duration <= 0:
            return None
        
        target_bitrate = (target_size_mb * 8192) / duration
        output_path = input_path.rsplit(".", 1)[0] + "_compressed.mp4"
        
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-c:v", "libx264",
            "-b:v", f"{target_bitrate}k",
            "-preset", "fast",
            "-c:a", "aac",
            "-b:a", "128k",
            output_path
        ]
        
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()
        
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return output_path
    except Exception as e:
        logger.error(f"Compression failed: {e}")
    return None

# ---------- BOT CLASS ----------
class TextWatermarkBot:
    def __init__(self):
        self.app = Client(
            "text_watermark_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN
        )
        self.watermark_enabled = load_watermark_state()
        self.watermark_text = DEFAULT_WATERMARK_TEXT
        self.downloads_dir = "downloads"
        os.makedirs(self.downloads_dir, exist_ok=True)
    
    async def check_ffmpeg(self):
        """Check if ffmpeg is available"""
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                version_line = stdout.decode().split('\n')[0]
                logger.info(f"✅ FFmpeg found: {version_line}")
                return True
            else:
                logger.error("❌ FFmpeg not found!")
                return False
        except FileNotFoundError:
            logger.error("❌ FFmpeg NOT INSTALLED!")
            return False
        except Exception as e:
            logger.error(f"❌ FFmpeg check failed: {e}")
            return False
    
    async def download_media(self, message: Message) -> Optional[str]:
        """Download video from message"""
        try:
            video = message.video or message.document
            if not video:
                return None
            
            file_path = os.path.join(self.downloads_dir, f"video_{message.id}.mp4")
            status_msg = await message.reply("📥 Downloading video...")
            await message.download(file_name=file_path)
            await status_msg.delete()
            
            if os.path.exists(file_path):
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"Downloaded: {file_path} ({size_mb:.1f}MB)")
                return file_path
        except Exception as e:
            logger.error(f"Download failed: {e}")
            await message.reply(f"❌ Download failed: {str(e)[:100]}")
        return None
    
    async def process_video(self, message: Message, video_path: str):
        """Process video: compress if needed, add text watermark, send back"""
        try:
            status_msg = await message.reply("🎬 Processing video...")
            file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
            logger.info(f"File size: {file_size_mb}MB")
            
            current_path = video_path
            if file_size_mb > 500:
                logger.info("Compressing...")
                await status_msg.edit_text(f"📦 Compressing {file_size_mb:.1f}MB video...")
                compressed = await compress_video(video_path, target_size_mb=200)
                if compressed:
                    current_path = compressed
                    logger.info("Compressed successfully")
                else:
                    logger.warning("Compression failed")
            
            final_path = current_path
            if self.watermark_enabled and self.watermark_text:
                logger.info(f"Adding watermark: '{self.watermark_text}'")
                await status_msg.edit_text(f"✍️ Adding text watermark...")
                watermarked = await add_text_watermark(current_path, self.watermark_text)
                if watermarked:
                    final_path = watermarked
                    logger.info("Watermark added successfully")
                    await status_msg.edit_text("✅ Text watermark added")
                else:
                    logger.error("Watermark failed")
                    await status_msg.edit_text("⚠️ Watermark failed, sending original")
            
            logger.info("Generating thumbnail...")
            await status_msg.edit_text("📸 Generating thumbnail...")
            thumbnail = await generate_video_thumbnail(final_path)
            
            logger.info("Uploading video...")
            await status_msg.edit_text("📤 Uploading video...")
            
            caption = f"""✅ Video processed
📏 Size: {os.path.getsize(final_path) / (1024 * 1024):.1f}MB
✍️ Watermark: {'ON' if self.watermark_enabled else 'OFF'}
📝 Text: {self.watermark_text}

Commands:
/setwatermark <text> - Change watermark text
/watermark - Toggle ON/OFF"""
            
            await message.reply_video(
                video=final_path,
                caption=caption,
                thumb=thumbnail,
                supports_streaming=True
            )
            
            await status_msg.delete()
            
            # Cleanup
            for path in [video_path, current_path, final_path, thumbnail]:
                if path and path != video_path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except:
                        pass
                    
        except FloodWait as e:
            logger.warning(f"Flood wait: {e.value}s")
            await asyncio.sleep(e.value)
            await message.reply("⏳ Rate limited, please try again")
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            await message.reply(f"❌ Processing failed: {str(e)[:200]}")
    
    def register_handlers(self):
        """Register bot commands"""
        
        @self.app.on_message(filters.command("start"))
        async def start_cmd(client, message: Message):
            await message.reply(
                "✍️ Text Watermark Bot\n\n"
                "Send me a video and I'll add a text watermark!\n\n"
                "Commands:\n"
                "/setwatermark <text> - Set watermark text\n"
                "/watermark - Toggle watermark ON/OFF\n"
                "/status - Check bot status\n\n"
                "Example:\n"
                "/setwatermark @MyChannel"
            )
        
        @self.app.on_message(filters.command("status"))
        async def status_cmd(client, message: Message):
            status = "ON" if self.watermark_enabled else "OFF"
            await message.reply(
                f"Bot Status\n\n"
                f"Watermark: {status}\n"
                f"Text: {self.watermark_text}\n"
                f"Send a video to process!"
            )
        
        @self.app.on_message(filters.command("setwatermark"))
        async def set_watermark_text(client, message: Message):
            if len(message.command) < 2:
                await message.reply("Please provide watermark text!\nExample: /setwatermark @MyChannel")
                return
            
            new_text = ' '.join(message.command[1:])
            self.watermark_text = new_text
            await message.reply(f"Watermark text set to: {self.watermark_text}\n\nSend a video to apply it!")
        
        @self.app.on_message(filters.command("watermark"))
        async def toggle_watermark(client, message: Message):
            self.watermark_enabled = not self.watermark_enabled
            save_watermark_state(self.watermark_enabled)
            status = "ON" if self.watermark_enabled else "OFF"
            await message.reply(f"Text watermark turned {status}")
        
        @self.app.on_message(filters.video | filters.document)
        async def handle_video(client, message: Message):
            is_video = message.video or (message.document and message.document.mime_type and message.document.mime_type.startswith("video/"))
            
            if not is_video:
                await message.reply("Please send a video file")
                return
            
            file_size_mb = (message.video or message.document).file_size / (1024 * 1024)
            if file_size_mb > MAX_VIDEO_SIZE_MB:
                await message.reply(f"Video too large ({file_size_mb:.1f}MB). Max size: {MAX_VIDEO_SIZE_MB}MB")
                return
            
            if not self.watermark_text:
                await message.reply("No watermark text set!\nUse /setwatermark YourText first")
                return
            
            processing_msg = await message.reply("Starting video processing...")
            video_path = await self.download_media(message)
            if not video_path:
                await processing_msg.edit_text("Failed to download video")
                return
            
            await self.process_video(message, video_path)
        
        logger.info("Handlers registered")
    
    async def start(self):
        """Start the bot"""
        await self.app.start()
        await self.check_ffmpeg()
        me = await self.app.get_me()
        logger.info(f"Bot started as: {me.first_name} (@{me.username})")
        logger.info(f"Watermark text: '{self.watermark_text}'")
        logger.info(f"Watermark state: {'ON' if self.watermark_enabled else 'OFF'}")
        self.register_handlers()
    
    async def stop(self):
        """Stop the bot"""
        await self.app.stop()
        logger.info("Bot stopped")

# ---------- MAIN ----------
async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        logger.error("Missing required environment variables: API_ID, API_HASH, BOT_TOKEN")
        return
    
    bot = TextWatermarkBot()
    await bot.start()
    
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