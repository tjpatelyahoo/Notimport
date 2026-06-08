"""
Batch Text Watermark Bot
- Working watermark from wm2.py
- Collect multiple videos, then /done to start
- Process one by one
- /cancel to stop
"""

import os
import asyncio
import json
import logging
from typing import Optional, List, Dict
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
logger = logging.getLogger("batch_watermark_bot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# ---------- FLASK APP ----------
app = Flask(__name__)

@app.route("/")
def home():
    return "Batch Watermark Bot Running"

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

# ---------- FFMPEG FUNCTIONS (WORKING FROM wm2.py) ----------
async def add_text_watermark(input_path: str, watermark_text: str, output_path: str = None, font_size: int = 30) -> Optional[str]:
    """Add text watermark to video using ffmpeg drawtext"""
    try:
        if not output_path:
            base, ext = os.path.splitext(input_path)
            output_path = f"{base}_watermarked{ext}"
        
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
class BatchWatermarkBot:
    def __init__(self):
        self.app = Client(
            "batch_watermark_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN
        )
        self.watermark_enabled = load_watermark_state()
        self.watermark_text = DEFAULT_WATERMARK_TEXT
        self.downloads_dir = "downloads"
        os.makedirs(self.downloads_dir, exist_ok=True)
        
        # Batch processing variables
        self.collecting = False
        self.pending_videos: List[Dict] = []
        self.collect_msg = None
        self.is_processing = False
        self.cancel_flag = False
        self.current_chat_id = None
    
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
    
    async def download_media(self, message: Message, video_data: Dict) -> Optional[str]:
        """Download video from message"""
        try:
            file_path = os.path.join(self.downloads_dir, f"video_{message.id}.mp4")
            await message.download(file_name=file_path)
            
            if os.path.exists(file_path):
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"Downloaded: {file_path} ({size_mb:.1f}MB)")
                return file_path
        except Exception as e:
            logger.error(f"Download failed: {e}")
        return None
    
    async def process_single_video(self, message: Message, video_path: str, index: int, total: int, status_msg: Message) -> bool:
        """Process a single video with watermark"""
        try:
            file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
            
            # Compress if needed
            current_path = video_path
            
            # Add watermark
            await status_msg.edit_text(f"✍️ Adding watermark to video {index}/{total}...")
            watermarked = await add_text_watermark(current_path, self.watermark_text)
            
            if not watermarked:
                await message.reply(f"❌ Failed to add watermark to video {index}/{total}")
                return False
            
            # Generate thumbnail
            await status_msg.edit_text(f"📸 Generating thumbnail for video {index}/{total}...")
            thumbnail = await generate_video_thumbnail(watermarked)
            
            # Send video
            await status_msg.edit_text(f"📤 Uploading video {index}/{total}...")
            
            caption = f"""✅ Video {index}/{total} Processed

✍️ Watermark: {self.watermark_text}
📏 Size: {os.path.getsize(watermarked) / (1024 * 1024):.1f}MB

Watermark: {'ON' if self.watermark_enabled else 'OFF'}"""
            
            await message.reply_video(
                video=watermarked,
                caption=caption,
                thumb=thumbnail,
                supports_streaming=True
            )
            
            # Cleanup
            for path in [video_path, current_path, watermarked, thumbnail]:
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except:
                        pass
            
            return True
            
        except Exception as e:
            logger.error(f"Process failed: {e}")
            await message.reply(f"❌ Error processing video {index}/{total}: {str(e)[:100]}")
            return False
    
    async def process_batch(self, message: Message):
        """Process all pending videos one by one"""
        total = len(self.pending_videos)
        success_count = 0
        
        self.is_processing = True
        self.cancel_flag = False
        self.current_chat_id = message.chat.id
        
        # Send status message
        status_msg = await message.reply(
            f"🎬 Starting batch processing\n"
            f"📊 Total videos: {total}\n"
            f"✍️ Watermark: {self.watermark_text}\n\n"
            f"🔄 Processing one by one...\n"
            f"Use /cancel to stop"
        )
        
        for idx, video_data in enumerate(self.pending_videos, 1):
            # Check for cancel
            if self.cancel_flag:
                await status_msg.edit_text(
                    f"🛑 Batch cancelled!\n"
                    f"✅ Completed: {success_count}/{total}\n"
                    f"❌ Remaining: {total - success_count}"
                )
                break
            
            # Download video
            await status_msg.edit_text(f"📥 Downloading video {idx}/{total}...")
            video_msg = video_data['message']
            video_path = await self.download_media(video_msg, video_data)
            
            if not video_path:
                await message.reply(f"❌ Failed to download video {idx}/{total}")
                continue
            
            # Process video
            success = await self.process_single_video(message, video_path, idx, total, status_msg)
            if success:
                success_count += 1
            
            # Delay between videos
            await asyncio.sleep(2)
        
        # Final status
        if not self.cancel_flag:
            await status_msg.edit_text(
                f"✅ Batch completed!\n"
                f"📊 Success: {success_count}/{total}\n"
                f"✍️ Watermark: {self.watermark_text}\n\n"
                f"Send more videos to process!"
            )
        else:
            await status_msg.edit_text(
                f"🛑 Batch stopped!\n"
                f"✅ Completed: {success_count}/{total}"
            )
        
        # Reset
        self.is_processing = False
        self.pending_videos = []
        self.collecting = False
        self.current_chat_id = None
    
    def register_handlers(self):
        """Register bot commands"""
        
        @self.app.on_message(filters.command("start"))
        async def start_cmd(client, message: Message):
            await message.reply(
                f"✍️ *Batch Watermark Bot*\n\n"
                f"Current watermark: `{self.watermark_text}`\n\n"
                f"*How to use:*\n"
                f"1️⃣ Send multiple videos\n"
                f"2️⃣ Type `/done` when ready\n"
                f"3️⃣ Bot processes them one by one\n"
                f"4️⃣ Use `/cancel` to stop\n\n"
                f"*Commands:*\n"
                f"/setwatermark `<text>` - Change watermark\n"
                f"/watermark - Toggle ON/OFF\n"
                f"/status - Check status\n"
                f"/done - Start processing\n"
                f"/cancel - Stop current batch\n"
                f"/clear - Clear pending videos"
            )
        
        @self.app.on_message(filters.command("status"))
        async def status_cmd(client, message: Message):
            status = "ON" if self.watermark_enabled else "OFF"
            
            if self.collecting:
                await message.reply(
                    f"📥 Collecting videos...\n"
                    f"Pending: {len(self.pending_videos)} videos\n"
                    f"Watermark: {status}\n"
                    f"Text: `{self.watermark_text}`\n\n"
                    f"Type `/done` to start processing"
                )
            elif self.is_processing:
                await message.reply(
                    f"🎬 Processing batch...\n"
                    f"Watermark: {status}\n"
                    f"Use `/cancel` to stop"
                )
            else:
                await message.reply(
                    f"✅ Bot idle\n"
                    f"Watermark: {status}\n"
                    f"Text: `{self.watermark_text}`\n\n"
                    f"Send videos to start!"
                )
        
        @self.app.on_message(filters.command("setwatermark"))
        async def set_watermark_text(client, message: Message):
            if len(message.command) < 2:
                await message.reply("Usage: `/setwatermark YourText`")
                return
            
            new_text = ' '.join(message.command[1:])
            self.watermark_text = new_text
            await message.reply(f"✅ Watermark set to: `{self.watermark_text}`")
        
        @self.app.on_message(filters.command("watermark"))
        async def toggle_watermark(client, message: Message):
            self.watermark_enabled = not self.watermark_enabled
            save_watermark_state(self.watermark_enabled)
            status = "ON" if self.watermark_enabled else "OFF"
            await message.reply(f"✍️ Watermark turned {status}")
        
        @self.app.on_message(filters.command("done"))
        async def done_cmd(client, message: Message):
            if not self.collecting or len(self.pending_videos) == 0:
                await message.reply("No pending videos. Send some videos first!")
                return
            
            if self.collect_msg:
                await self.collect_msg.delete()
            
            await message.reply(f"✅ Starting batch with {len(self.pending_videos)} videos...")
            await self.process_batch(message)
        
        @self.app.on_message(filters.command("cancel"))
        async def cancel_cmd(client, message: Message):
            if self.is_processing and message.chat.id == self.current_chat_id:
                self.cancel_flag = True
                await message.reply("🛑 Cancelling current batch...")
            elif self.collecting:
                self.pending_videos = []
                self.collecting = False
                if self.collect_msg:
                    await self.collect_msg.delete()
                await message.reply("✅ Cleared pending videos")
            else:
                await message.reply("ℹ️ No active batch to cancel")
        
        @self.app.on_message(filters.command("clear"))
        async def clear_cmd(client, message: Message):
            if self.collecting:
                self.pending_videos = []
                await message.reply(f"✅ Cleared {len(self.pending_videos)} pending videos")
            else:
                await message.reply("ℹ️ No pending videos")
        
        @self.app.on_message(filters.video | filters.document)
        async def handle_video(client, message: Message):
            # Check if it's a video
            is_video = message.video or (message.document and message.document.mime_type and message.document.mime_type.startswith("video/"))
            
            if not is_video:
                await message.reply("Please send video files only")
                return
            
            # Check file size
            file_size_mb = (message.video or message.document).file_size / (1024 * 1024)
            if file_size_mb > MAX_VIDEO_SIZE_MB:
                await message.reply(f"Video too large ({file_size_mb:.1f}MB). Max: {MAX_VIDEO_SIZE_MB}MB")
                return
            
            # If already processing, reject
            if self.is_processing:
                await message.reply("⚠️ Bot is busy processing a batch. Please try again later.")
                return
            
            # Start collecting if not already
            if not self.collecting:
                self.collecting = True
                self.pending_videos = []
                self.collect_msg = await message.reply(
                    f"📥 Collecting videos...\n"
                    f"Send more videos or type `/done` to start processing\n"
                    f"Type `/clear` to clear all\n\n"
                    f"Video 1 received ✅"
                )
            
            # Add to pending list
            self.pending_videos.append({
                'message': message,
                'message_id': message.id,
                'file_size': file_size_mb
            })
            
            # Update collection message
            await self.collect_msg.edit_text(
                f"📥 Collecting videos...\n"
                f"Received: {len(self.pending_videos)} video(s)\n"
                f"Type `/done` to start processing\n"
                f"Type `/clear` to clear all"
            )
        
        logger.info("✅ Handlers registered")
    
    async def start(self):
        """Start the bot"""
        await self.app.start()
        await self.check_ffmpeg()
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
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        logger.error("Missing required environment variables: API_ID, API_HASH, BOT_TOKEN")
        return
    
    bot = BatchWatermarkBot()
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