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
MAX_VIDEO_SIZE_MB = 2000  # Max video size to process (2GB)

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("text_watermark_bot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# ---------- FLASK APP FOR HEALTH CHECKS ----------
app = Flask(__name__)

@app.route("/")
def home():
    return "Text Watermark Bot Running"

@app.route("/health")
def health():
    return "OK"

# ---------- WATERMARK STATE MANAGEMENT ----------
def load_watermark_state() -> bool:
    """Load watermark enabled/disabled state"""
    try:
        if os.path.exists(WATERMARK_STATE_FILE):
            with open(WATERMARK_STATE_FILE, "r") as f:
                return bool(json.load(f).get("enabled", True))
    except Exception:
        pass
    return True

def save_watermark_state(enabled: bool):
    """Save watermark state"""
    try:
        with open(WATERMARK_STATE_FILE, "w") as f:
            json.dump({"enabled": enabled}, f)
    except Exception:
        pass

# ---------- TEXT WATERMARK FUNCTION ----------
async def add_text_watermark(input_path: str, watermark_text: str, output_path: str = None, font_size: int = 30) -> Optional[str]:
    """
    Add text watermark to video using ffmpeg drawtext
    Returns path to watermarked video or None if failed
    """
    try:
        if not output_path:
            base, ext = os.path.splitext(input_path)
            output_path = f"{base}_watermarked{ext}"
        
        # Get video dimensions to position watermark
        probe_cmd = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-of', 'csv=p=0',
            input_path
        ]
        
        proc = await asyncio.create_subprocess_exec(
            *probe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        width, height = map(int, stdout.decode().strip().split(','))
        
        # Position: bottom-right with 20px padding
        x_pos = width - 20
        y_pos = height - 20
        
        # FFmpeg drawtext filter
        # Using white text with black shadow for visibility
        drawtext_filter = (
            f"drawtext=text='{watermark_text}':"
            f"fontcolor=white:"
            f"fontsize={font_size}:"
            f"x={x_pos}-text_w:"
            f"y={y_pos}-text_h:"
            f"shadowx=2:shadowy=2:shadowcolor=black"
        )
        
        cmd = [
            "ffmpeg",
            "-y",
            "-loglevel", "error",
            "-i", input_path,
            "-vf", drawtext_filter,
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-c:a", "copy",
            output_path
        ]
        
        if DEBUG:
            logger.info(f"🎨 ffmpeg command: {' '.join(cmd)}")
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=300  # 5 minutes timeout
            )
        except asyncio.TimeoutError:
            logger.error("❌ ffmpeg timeout")
            proc.kill()
            return None
        
        if stderr and DEBUG:
            logger.error(f"ffmpeg stderr: {stderr.decode()[:500]}")
        
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            logger.info(f"✅ Text watermark added: {output_path}")
            return output_path
        else:
            logger.error("❌ Output file missing or empty")
            return None
            
    except Exception as e:
        logger.error(f"❌ Text watermark exception: {e}")
        return None

async def generate_video_thumbnail(video_path: str, second: int = 10) -> Optional[str]:
    """Generate thumbnail from video at specified second"""
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
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await proc.communicate()
        
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            return thumb_path
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {e}")
    return None

async def compress_video(input_path: str, target_size_mb: int = 50) -> Optional[str]:
    """Compress video to target size using ffmpeg"""
    try:
        # Get video duration
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
        
        # Calculate target bitrate (kbps)
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

# ---------- BOT HANDLERS ----------
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
    
    async def download_media(self, message: Message) -> Optional[str]:
        """Download video from message"""
        try:
            video = message.video or message.document
            if not video:
                return None
            
            # Generate safe filename
            file_name = f"video_{message.id}.mp4"
            file_path = os.path.join(self.downloads_dir, file_name)
            
            # Download with progress
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
        logger.info(f"📊 File size: {file_size_mb}MB")
        
        # Step 1: Compress if too large (over 500MB) - SKIP for small files
        current_path = video_path
        if file_size_mb > 500:
            logger.info("📦 File >500MB, compressing...")
            await status_msg.edit_text(f"📦 Compressing {file_size_mb:.1f}MB video...")
            compressed = await compress_video(video_path, target_size_mb=200)
            if compressed:
                current_path = compressed
                logger.info(f"✅ Compressed to: {current_path}")
            else:
                logger.warning("⚠️ Compression failed")
        else:
            logger.info(f"✅ File size {file_size_mb}MB - no compression needed")
        
        # Step 2: Add text watermark if enabled
        final_path = current_path
        if self.watermark_enabled and self.watermark_text:
            logger.info(f"✍️ Adding watermark: '{self.watermark_text}'")
            await status_msg.edit_text(f"✍️ Adding text watermark: '{self.watermark_text}'")
            
            # Test if ffmpeg works
            logger.info("🔧 Calling add_text_watermark function...")
            watermarked = await add_text_watermark(current_path, self.watermark_text, font_size=30)
            logger.info(f"🔧 Watermark result: {watermarked}")
            
            if watermarked:
                final_path = watermarked
                logger.info("✅ Watermark added successfully")
                await status_msg.edit_text("✅ Text watermark added")
            else:
                logger.error("❌ Watermark failed")
                await status_msg.edit_text("⚠️ Watermark failed, sending original")
        else:
            logger.info("⏭️ Watermark disabled or no text")
        
        # Step 3: Generate thumbnail
        logger.info("📸 Generating thumbnail...")
        await status_msg.edit_text("📸 Generating thumbnail...")
        thumbnail = await generate_video_thumbnail(final_path)
        
        # Step 4: Send watermarked video
        logger.info("📤 Uploading video...")
        await status_msg.edit_text("📤 Uploading video...")
        
        caption = (
            f"✅ Video processed\n"
            f"📏 Size: {os.path.getsize(final_path) / (1024 * 1024):.1f}MB\n"
            f"✍️ Watermark: {'ON' if self.watermark_enabled else 'OFF'}\n"
            f"📝 Text: {self.watermark_text}\n\n"
            f"Commands:\n"
            f"/setwatermark <text> - Change watermark text\n"
            f"/watermark - Toggle ON/OFF"
        )
        
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
                    logger.info(f"🗑️ Deleted: {path}")
                except:
                    pass
                    
    except FloodWait as e:
        logger.warning(f"Flood wait: {e.value}s")
        await asyncio.sleep(e.value)
        await message.reply("⏳ Rate limited, please try again in a few seconds")
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}", exc_info=True)
        await message.reply(f"❌ Processing failed: {str(e)[:200]}")
    
    def register_handlers(self):
        """Register bot commands"""
        
        @self.app.on_message(filters.command("start"))
        async def start_cmd(client, message: Message):
            await message.reply(
                "✍️ *Text Watermark Bot*\n\n"
                "Send me a video and I'll add a text watermark!\n\n"
                "*Commands:*\n"
                "/setwatermark `<text>` - Set watermark text\n"
                "/watermark - Toggle watermark ON/OFF\n"
                "/status - Check bot status\n"
                "/help - Show this help\n\n"
                "*Features:*\n"
                "• Custom text watermark\n"
                "• Auto-compression for large videos\n"
                "• Works in groups and private chats\n\n"
                "*Example:*\n"
                "`/setwatermark @MyChannel`",
            )
        
        @self.app.on_message(filters.command("help"))
        async def help_cmd(client, message: Message):
            await start_cmd(client, message)
        
        @self.app.on_message(filters.command("status"))
        async def status_cmd(client, message: Message):
            status = "✅ ON" if self.watermark_enabled else "❌ OFF"
            
            await message.reply(
                f"📊 *Bot Status*\n\n"
                f"Watermark: {status}\n"
                f"Text: `{self.watermark_text}`\n"
                f"Max File Size: {MAX_VIDEO_SIZE_MB}MB\n\n"
                f"Send a video to process!",
            )
        
        @self.app.on_message(filters.command("setwatermark"))
        async def set_watermark_text(client, message: Message):
            if len(message.command) < 2:
                await message.reply(
                    "❌ Please provide watermark text!\n"
                    "Example: `/setwatermark @MyChannel`",
                )
                return
            
            new_text = ' '.join(message.command[1:])
            self.watermark_text = new_text
            await message.reply(
                f"✅ Watermark text set to:\n"
                f"`{self.watermark_text}`\n\n"
                f"Send a video to apply it!",
            )
        
        @self.app.on_message(filters.command("watermark"))
        async def toggle_watermark(client, message: Message):
            self.watermark_enabled = not self.watermark_enabled
            save_watermark_state(self.watermark_enabled)
            
            status = "ON ✅" if self.watermark_enabled else "OFF ❌"
            await message.reply(f"✍️ Text watermark turned {status}")
        
        @self.app.on_message(filters.video | filters.document)
        async def handle_video(client, message: Message):
            # Check if it's a video
            is_video = message.video or (message.document and message.document.mime_type and message.document.mime_type.startswith("video/"))
            
            if not is_video:
                await message.reply("❌ Please send a video file")
                return
            
            # Check file size
            file_size_mb = (message.video or message.document).file_size / (1024 * 1024)
            if file_size_mb > MAX_VIDEO_SIZE_MB:
                await message.reply(f"❌ Video too large ({file_size_mb:.1f}MB). Max size: {MAX_VIDEO_SIZE_MB}MB")
                return
            
            # Check if watermark text is set
            if not self.watermark_text:
                await message.reply(
                    "❌ No watermark text set!\n"
                    "Use `/setwatermark YourText` first",
                )
                return
            
            # Send processing message
            processing_msg = await message.reply("📥 Starting video processing...")
            
            # Download video
            video_path = await self.download_media(message)
            if not video_path:
                await processing_msg.edit_text("❌ Failed to download video")
                return
            
            # Process video
            await self.process_video(message, video_path)
        
        logger.info("✅ Handlers registered")
    
    async def start(self):
        """Start the bot"""
        await self.app.start()
        me = await self.app.get_me()
        logger.info(f"✅ Bot started as: {me.first_name} (@{me.username})")
        logger.info(f"✍️ Watermark text: '{self.watermark_text}'")
        logger.info(f"🎨 Watermark state: {'ON' if self.watermark_enabled else 'OFF'}")
        
        self.register_handlers()
    
    async def stop(self):
        """Stop the bot"""
        await self.app.stop()
        logger.info("Bot stopped")

# ---------- MAIN ----------
async def main():
    # Check required environment variables
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        logger.error("❌ Missing required environment variables:")
        logger.error("API_ID, API_HASH, BOT_TOKEN must be set")
        return
    
    bot = TextWatermarkBot()
    await bot.start()
    
    # Start Flask health check server
    asyncio.create_task(asyncio.to_thread(app.run, "0.0.0.0", PORT))
    
    try:
        await asyncio.Future()  # Run forever
    except asyncio.CancelledError:
        pass
    finally:
        await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted, exiting")