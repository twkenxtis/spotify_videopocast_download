import json
import os
import asyncio
import aiohttp
import logging
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MP4Parser:
    """MP4 parser using mp4dump for DRM detection and codec extraction"""
    
    def __init__(self, file_path):
        self.path = Path(file_path)
        self.is_webm = self.path.suffix.lower() in ('.webm', '.mkv')
    
    async def _run_mp4dump(self):
        """Execute mp4dump and capture output"""
        try:
            proc = await asyncio.create_subprocess_exec(
                'mp4dump',
                str(self.path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            return stdout.decode('utf-8', errors='ignore').lower()
        except Exception as e:
            logging.debug(f"mp4dump failed: {e}")
            return ""
    
    async def detect_drm(self):
        """Check for encryption markers"""
        if self.is_webm:
            with open(self.path, 'rb') as fp:
                content = fp.read(min(50000, os.path.getsize(self.path)))
                if b'\x50\x35' in content and b'encrypted' in content.lower():
                    logging.warning(f"\033[35mDRM detected: {self.path.name}\033[0m")
                    return True
            return False
        
        output = await self._run_mp4dump()
        drm_markers = ['sinf', 'schm', 'schi', 'tenc', 'pssh', 'encrypted']
        
        has_drm = any(marker in output for marker in drm_markers)
        if has_drm:
            logging.warning(f"\033[35mDRM detected: {self.path.name}\033[0m")
        return has_drm
    
    async def get_codec_info(self):
        """Extract codec info from mp4dump output"""
        codecs = {'video': None, 'audio': None, 'width': None, 'height': None}
        
        if self.is_webm:
            with open(self.path, 'rb') as fp:
                content = fp.read(min(100000, os.path.getsize(self.path)))
            
            if b'V_VP9' in content:
                codecs['video'] = 'VP9'
            elif b'V_AV1' in content:
                codecs['video'] = 'AV1'
            
            if b'A_OPUS' in content or b'Opus' in content:
                codecs['audio'] = 'Opus'
            elif b'A_VORBIS' in content:
                codecs['audio'] = 'Vorbis'
            
            return codecs
        
        output = await self._run_mp4dump()
        lines = output.split('\n')
        
        for i, line in enumerate(lines):
            if 'avc1' in line or 'avc3' in line:
                codecs['video'] = 'H264'
            elif 'hev1' in line or 'hvc1' in line:
                codecs['video'] = 'H265'
            elif 'vp09' in line:
                codecs['video'] = 'VP9'
            elif 'av01' in line:
                codecs['video'] = 'AV1'
            
            if 'mp4a' in line:
                for j in range(i, min(i+10, len(lines))):
                    if 'objecttypeindication' in lines[j] or 'audio object type' in lines[j]:
                        if '0x40' in lines[j] or 'mpeg-4 audio' in lines[j]:
                            codecs['audio'] = 'AAC'
                        elif '0x6b' in lines[j] or 'mpeg-1 audio' in lines[j]:
                            codecs['audio'] = 'MP3'
                        break
                if not codecs['audio']:
                    codecs['audio'] = 'AAC'
            elif 'opus' in line:
                codecs['audio'] = 'Opus'
            elif 'ac-3' in line:
                codecs['audio'] = 'AC3'
            
            if 'width' in line and '=' in line:
                try:
                    width = int(line.split('=')[1].strip())
                    if 100 < width < 10000:
                        codecs['width'] = width
                except:
                    pass
            if 'height' in line and '=' in line:
                try:
                    height = int(line.split('=')[1].strip())
                    if 100 < height < 10000:
                        codecs['height'] = height
                except:
                    pass
        
        return codecs


class StreamDownloader:
    """Async downloader for segments"""
    
    def __init__(self, session, base_urls, output_dir):
        self.session = session
        self.base_urls = base_urls
        self.output_dir = Path(output_dir)
        
    async def download(self, url, filename, max_retries=3):
        """Download with retry"""
        filepath = self.output_dir / filename
        
        if filepath.exists() and filepath.stat().st_size > 0:
            return True
            
        for attempt in range(max_retries):
            for base in self.base_urls:
                try:
                    async with self.session.get(base + url, timeout=7) as resp:
                        if resp.status == 200:
                            filepath.parent.mkdir(parents=True, exist_ok=True)
                            with open(filepath, 'wb') as f:
                                async for chunk in resp.content.iter_chunked(8192):
                                    f.write(chunk)
                            if filepath.stat().st_size > 0:
                                return True
                except Exception as e:
                    if attempt == max_retries - 1:
                        logging.error(f"Failed to download {filename}: {e}")
        return False


class MediaMerger:
    """Merge init + segments and handle decryption"""
    
    def __init__(self, base_dir):
        self.base_dir = Path(base_dir)
        
    async def merge(self, media_type, ext):
        """Concat init + segments"""
        media_dir = self.base_dir / media_type
        init_file = next(media_dir.glob(f"init*.{ext}"), None)
        segments = sorted([f for f in media_dir.glob(f"*.{ext}") 
                          if f.name != (init_file.name if init_file else '')],
                         key=lambda x: int(x.stem) if x.stem.isdigit() else 0)
        
        if not segments:
            return False, False
            
        output = self.base_dir / f"{media_type}.{ext}"
        with open(output, 'wb') as out:
            if init_file:
                out.write(init_file.read_bytes())
            for seg in segments:
                out.write(seg.read_bytes())
        
        parser = MP4Parser(output)
        has_drm = await parser.detect_drm()
        
        return True, has_drm
    
    async def decrypt_if_needed(self, input_file, key_id, key, stream_type):
        """Use shaka-packager for decryption"""
        output_file = input_file.parent / f"{input_file.stem}_dec{input_file.suffix}"
        
        cmd = [
            "packager",
            f"input={input_file},stream={stream_type},output={output_file}",
            "--enable_raw_key_decryption",
            f"--keys=key_id={key_id}:key={key}"
        ]
        
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        
        if proc.returncode == 0:
            logging.info(f"Decrypted: {output_file.name}")
            return output_file
        return None


class SpotifyDownloader:
    """Main orchestrator"""
    
    def __init__(self, json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            self.data = json.load(f)
        self.output_dir = Path(f"{datetime.now().strftime('%y%m%d_%H%M%S')}_spotify_{Path(json_path).stem}")
        self.shared_key = None
        self.mp4_had_drm = False  # Track if MP4 had DRM
        
    def get_best_profiles(self, fmt):
        """Select highest bitrate profiles"""
        profiles = self.data['contents'][0]['profiles']
        
        video = max((p for p in profiles if p.get('file_type') == fmt and 'video_bitrate' in p),
                   key=lambda x: x.get('video_bitrate', 0), default=None)
        audio = max((p for p in profiles if p.get('file_type') == fmt and 'audio_bitrate' in p 
                    and 'video_bitrate' not in p),
                   key=lambda x: x['audio_bitrate'], default=None)
        
        if video and audio:
            logging.info(f"[{fmt.upper()}] Video: {video['video_width']}x{video['video_height']} "
                        f"{video['video_codec']} {video['video_bitrate']/1000:.0f}kbps | "
                        f"Audio: {audio['audio_codec']} {audio['audio_bitrate']/1000:.0f}kbps")
        return video, audio
    
    async def run(self):
        """Main workflow"""
        segment_count = (self.data['end_time_millis'] - self.data['start_time_millis']) // \
                    (self.data['contents'][0]['segment_length'] * 1000) + 1
        
        async with aiohttp.ClientSession() as session:
            for fmt in ['mp4', 'webm']:
                video_prof, audio_prof = self.get_best_profiles(fmt)
                if not video_prof or not audio_prof:
                    continue
                
                # Download
                downloader = StreamDownloader(session, self.data['base_urls'], 
                                            self.output_dir / fmt)
                
                init_template = unquote(self.data['initialization_template'])
                seg_template = unquote(self.data['segment_template'])
                
                tasks = []
                for typ, prof in [('video', video_prof), ('audio', audio_prof)]:
                    init_url = init_template.replace('{{profile_id}}', str(prof['id'])) \
                                        .replace('{{file_type}}', fmt)
                    tasks.append(downloader.download(init_url, f"{typ}/init.{fmt}"))
                    
                    for i in range(segment_count):
                        ts = i * 4
                        seg_url = seg_template.replace('{{profile_id}}', str(prof['id'])) \
                                            .replace('{{segment_timestamp}}', str(ts)) \
                                            .replace('{{file_type}}', fmt)
                        tasks.append(downloader.download(seg_url, f"{typ}/{ts}.{fmt}"))
                
                await asyncio.gather(*tasks)
                
                # Merge
                merger = MediaMerger(self.output_dir / fmt)
                v_ok, v_drm = await merger.merge('video', fmt)
                a_ok, a_drm = await merger.merge('audio', fmt)
                
                # Mux
                if v_ok and a_ok:
                    video_file = self.output_dir / fmt / f"video.{fmt}"
                    audio_file = self.output_dir / fmt / f"audio.{fmt}"
                    
                    # Determine if decryption needed
                    # For WebM: if MP4 had DRM, force decrypt both tracks
                    if fmt == 'webm' and self.mp4_had_drm:
                        needs_video_decrypt = True
                        needs_audio_decrypt = True
                    else:
                        needs_video_decrypt = v_drm
                        needs_audio_decrypt = a_drm
                    
                    # Handle DRM
                    if needs_video_decrypt or needs_audio_decrypt:
                        if not self.shared_key:
                            self.shared_key = input("Enter decryption key (kid:key): ").strip()
                        
                        key_id, key = self.shared_key.split(':')
                        
                        if needs_video_decrypt:
                            decrypted = await merger.decrypt_if_needed(video_file, key_id, key, 'video')
                            if decrypted:
                                video_file = decrypted
                        
                        if needs_audio_decrypt:
                            decrypted = await merger.decrypt_if_needed(audio_file, key_id, key, 'audio')
                            if decrypted:
                                audio_file = decrypted
                    
                    # Track if MP4 had DRM for WebM processing
                    if fmt == 'mp4' and (v_drm or a_drm):
                        self.mp4_had_drm = True
                    
                    # Get codec info from merged video file
                    parser = MP4Parser(video_file)
                    info = await parser.get_codec_info()
                    
                    # Fallback to profile data if detection failed
                    if not info['video']:
                        codec = video_prof.get('video_codec', 'avc1').lower()
                        info['video'] = 'H265' if 'hev' in codec or 'hvc' in codec else \
                                    'VP9' if 'vp9' in codec else \
                                    'AV1' if 'av01' in codec else 'H264'
                    
                    if not info['audio']:
                        codec = audio_prof.get('audio_codec', 'mp4a').lower()
                        info['audio'] = 'Opus' if 'opus' in codec else \
                                    'Vorbis' if 'vorbis' in codec else 'AAC'
                    
                    if not info['height']:
                        info['height'] = video_prof.get('video_height', 1080)
                    
                    # Generate filename
                    output = self.output_dir / \
                            f"spotify_video.{info['height']}p.Spotify.WEB-DL.{info['audio']}2.0.{info['video']}.{fmt}"
                    
                    # FFmpeg mux
                    cmd = ['ffmpeg', '-y', '-i', str(video_file), '-i', str(audio_file),
                        '-c', 'copy', str(output)]
                    proc = await asyncio.create_subprocess_exec(*cmd, 
                                                                stdout=asyncio.subprocess.PIPE,
                                                                stderr=asyncio.subprocess.PIPE)
                    await proc.communicate()
                    
                    if proc.returncode == 0:
                        logging.info(f"Completed: {output}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <json_file>")
        sys.exit(1)
    
    asyncio.run(SpotifyDownloader(sys.argv[1]).run())
