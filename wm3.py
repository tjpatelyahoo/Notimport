"""
Batch Text Watermark Bot
- Watermark text from environment variable
- Process multiple videos one by one
- Cancel command to stop processing
- Auto-cleanup after each video
"""

import os
import asyncio
import json
import logging
from typing import Optional, List
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

# Watermark text from environment variable
WATERMARK_TEXT = os.environ.get("WATERMARK_TEXT", "@YourChannel")
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

# ---------- FFMPEG FUNCTIONS ----------
async def add_text_watermark(input_path: str, output_path: str = None, font_size: int = 30) -> Optional[str]:
    """Add text watermark to video"""
    try:
        if not output_path:
            base, ext = os.path.splitext(input_path)
            output_path = f"{base}_watermarked{ext}"
        
        # Escape special characters
        safe_text = WATERMARK_TEXT.replace("'", "'\\''").replace(":", "\\:").replace("@", "\\@")
        
        # Simpler command that works on Render
        cmd = [
            "ffmpeg",
            "-y",
            "-i", input_path,
            "-vf", f"drawtext=text='{safe_text}':fontcolor=white:fontsize={font_size}:x=10:y=10",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-crf", "28",
            "-c:a", "copy",
            output_path
        ]
        
        logger.info(f"Running ffmpeg on {input_path}")
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
            
            if proc.returncode != 0:
                error_msg = stderr.decode()[:500]
                logger.error(f"FFmpeg error: {error_msg}")
                return None
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                logger.info(f"Watermark added successfully")
                return output_path
            else:
                logger.error("Output file missing")
                return None
                
        except asyncio.TimeoutError:
            logger.error("FFmpeg timeout - killing process")
            proc.kill()
            await proc.wait()
            return None
            
    except Exception as e:
        logger.error(f"Exception in watermark: {e}")
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
        self.downloads_dir = "downloads"
        os.makedirs(self.downloads_dir, exist_ok=True)
        
        # Batch processing variables
        self.is_processing = False
        self.cancel_flag = False
        self.processing_queue = []
        self.status_msg = None
        self.current_chat_id = None
    
    async def safe_edit(self, text: str):
        """Safely edit status message"""
        try:
            if self.status_msg:
                await self.status_msg.edit_text(text)
        except Exception as e:
            logger.error(f"Failed to edit message: {e}")
    
    async def download_media(self, message: Message) -> Optional[str]:
        """Download video from message"""
        try:
            video = message.video or message.document
            if not video:
                return None
            
            file_path = os.path.join(self.downloads_dir, f"video_{message.id}.mp4")
            await message.download(file_name=file_path)
            
            if os.path.exists(file_path):
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"Downloaded: {file_path} ({size_mb:.1f}MB)")
                return file_path
        except Exception as e:
            logger.error(f"Download failed: {e}")
        return None
    
    async def process_single_video(self, message: Message, video_path: str, index: int, total: int) -> bool:
        """Process a single video with watermark"""
        try:
            # Update status
            await self.safe_edit(
                f"🎬 Processing video {index}/{total}\n"
                f"📥 File: {os.path.basename(video_path)}\n"
                f"📏 Size: {os.path.getsize(video_path) / (1024 * 1024):.1f}MB\n\n"
                f"✍️ Watermark: {WATERMARK_TEXT}\n\n"
                f"🔄 Adding watermark..."
            )
            
            # Add watermark
            watermarked_path = await add_text_watermark(video_path)
            
            if not watermarked_path:
                await message.reply(f"❌ Failed to process video {index}/{total}")
                return False
            
            # Generate thumbnail
            await self.safe_edit(f"🎬 Processing video {index}/{total}\n📸 Generating thumbnail...")
            thumbnail = await generate_video_thumbnail(watermarked_path)
            
            # Send watermarked video
            await self.safe_edit(f"🎬 Processing video {index}/{total}\n📤 Uploading to Telegram...")
            
            caption = f"""✅ Video {index}/{total} Processed

✍️ Watermark: {WATERMARK_TEXT}
📏 Size: {os.path.getsize(watermarked_path) / (1024 * 1024):.1f}MB

Send more videos to continue!
Use /cancel to stop processing."""
            
            await message.reply_video(
                video=watermarked_path,
                caption=caption,
                thumb=thumbnail,
                supports_streaming=True
            )
            
            # Cleanup files immediately
            for path in [video_path, watermarked_path, thumbnail]:
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                        logger.info(f"Deleted: {path}")
                    except:
                        pass
            
            return True
            
        except FloodWait as e:
            logger.warning(f"Flood wait: {e.value}s")
            await asyncio.sleep(e.value)
            return await self.process_single_video(message, video_path, index, total)
        except Exception as e:
            logger.error(f"Process failed: {e}")
            await message.reply(f"❌ Error processing video {index}/{total}: {str(e)[:100]}")
            return False
    
    async def process_batch(self, message: Message, videos: List[Message]):
        """Process multiple videos one by one"""
        total = len(videos)
        success_count = 0
        
        self.is_processing = True
        self.cancel_flag = False
        self.current_chat_id = message.chat.id
        
        # Send initial status
        self.status_msg = await message.reply(
            f"🎬 Starting batch processing\n"
            f"📊 Total videos: {total}\n"
            f"✍️ Watermark: {WATERMARK_TEXT}\n\n"
            f"🔄 Processing will continue one by one...\n"
            f"Use /cancel to stop"
        )
        
        for idx, video_msg in enumerate(videos, 1):
            # Check if cancelled
            if self.cancel_flag:
                await self.safe_edit(
                    f"🛑 Batch processing cancelled!\n"
                    f"✅ Completed: {success_count}/{total}\n"
                    f"❌ Remaining: {total - success_count}"
                )
                break
            
            # Download video
            await self.safe_edit(f"🎬 Processing video {idx}/{total}\n📥 Downloading video {idx}...")
            
            video_path = await self.download_media(video_msg)
            if not video_path:
                await message.reply(f"❌ Failed to download video {idx}/{total}")
                continue
            
            # Process video
            success = await self.process_single_video(message, video_path, idx, total)
            if success:
                success_count += 1
            
            # Small delay between videos to avoid rate limits
            await asyncio.sleep(2)
        
        # Final status
        if not self.cancel_flag:
            await self.safe_edit(
                f"✅ Batch processing completed!\n"
                f"📊 Success: {success_count}/{total}\n"
                f"✍️ Watermark: {WATERMARK_TEXT}\n\n"
                f"Send more videos to process!"
            )
        
        self.is_processing = False
        self.status_msg = None
        self.current_chat_id = None
    
    def register_handlers(self):
        """Register bot commands"""
        
        @self.app.on_message(filters.command("start"))
        async def start_cmd(client, message: Message):
            await message.reply(
                f"🎬 *Batch Watermark Bot*\n\n"
                f"✍️ Fixed Watermark: `{WATERMARK_TEXT}`\n\n"
                "Send me **multiple videos** and I'll:\n"
                "✅ Add watermark to each\n"
                "✅ Process them one by one\n"
                "✅ Auto-delete after sending\n"
                "✅ Stop anytime with /cancel\n\n"
                "📌 *Commands:*\n"
                "/cancel - Stop current batch\n"
                "/status - Check processing status\n"
                "/watermark - Show current watermark\n\n"
                "Just send videos (one or many)!"
            )
        
        @self.app.on_message(filters.command("watermark"))
        async def watermark_cmd(client, message: Message):
            await message.reply(f"✍️ Current watermark: `{WATERMARK_TEXT}`")
        
        @self.app.on_message(filters.command("status"))
        async def status_cmd(client, message: Message):
            if self.is_processing:
                await message.reply(
                    f"🎬 Batch processing is ACTIVE\n"
                    f"Use /cancel to stop"
                )
            else:
                await message.reply(
                    f"✅ Bot is IDLE\n"
                    f"Send videos to start processing!"
                )
        
        @self.app.on_message(filters.command("cancel"))
        async def cancel_cmd(client, message: Message):
            if self.is_processing and message.chat.id == self.current_chat_id:
                self.cancel_flag = True
                await message.reply(
                    "🛑 Cancel signal sent!\n"
                    "Current video will finish, then batch will stop."
                )
            else:
                await message.reply("ℹ️ No active batch to cancel.")
        
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
            
            # If already processing from same chat, add to queue
            if self.is_processing and message.chat.id == self.current_chat_id:
                self.processing_queue.append(message)
                await message.reply(
                    f"⏳ Added to queue (position: {len(self.processing_queue)})\n"
                    f"Will process after current batch completes."
                )
                return
            
            # If processing from different chat, don't interfere
            if self.is_processing:
                await message.reply("⚠️ Bot is busy processing another batch. Please try again later.")
                return
            
            # Start new batch
            videos = [message]
            
            # Ask if user wants to add more
            collect_msg = await message.reply(
                "🎬 Starting batch processing!\n\n"
                "Send more videos within 10 seconds, or wait to start automatically.\n"
                "Use /cancel to stop processing."
            )
            
            # Wait for more videos
            await asyncio.sleep(10)
            await collect_msg.delete()
            
            # Start processing
            await self.process_batch(message, videos)
            
            # Process queued items after batch completes
            if self.processing_queue:
                next_batch = self.processing_queue.copy()
                self.processing_queue.clear()
                await self.process_batch(next_batch[0], next_batch)
        
        logger.info("✅ Handlers registered")
    
    async def start(self):
        """Start the bot"""
        await self.app.start()
        me = await self.app.get_me()
        logger.info(f"✅ Bot started as: {me.first_name} (@{me.username})")
        logger.info(f"✍️ Watermark: {WATERMARK_TEXT}")
        logger.info(f"📁 Downloads dir: {self.downloads_dir}")
        self.register_handlers()
    
    async def stop(self):
        """Stop the bot"""
        await self.app.stop()
        logger.info("Bot stopped")

# ---------- MAIN ----------
async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        logger.error("Missing required environment variables")
        return
    
    if not WATERMARK_TEXT:
        logger.error("WATERMARK_TEXT environment variable not set!")
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
        logger.info("Interrupted")