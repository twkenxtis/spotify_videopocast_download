import json
import os
import asyncio
import aiohttp
import logging
import sys
import re
import time
import subprocess
from urllib.parse import unquote
from pathlib import Path


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


if len(sys.argv) < 2:
    print("Usage: python spotify_download.py <json_file>")
    sys.exit(1)


if not os.path.exists(sys.argv[1]):
    logging.error(f"輸入文件未找到: {sys.argv[1]}")
    sys.exit(1)


try:
    with open(sys.argv[1], 'r', encoding='utf-8') as f:
        data = json.load(f)
except (FileNotFoundError, json.JSONDecodeError) as e:
    logging.error(f"加載JSON失敗: {e}")
    sys.exit(1)


drminfo = len(data['contents'][0]['encryption_infos'])
if drminfo > 0:
    supported_formats = ['mp4', 'ts']
elif drminfo == 0:
    supported_formats = ['ts', 'mp4']


def select_best_video_profile(video_profiles, file_type):
    """Select the best video profile for a specific file type, sorted by bitrate or max_bitrate."""
    video_profiles_filtered = [p for p in video_profiles if p.get('file_type') == file_type]
    if video_profiles_filtered:
        sorted_profiles = sorted(
            video_profiles_filtered,
            key=lambda x: x.get('bitrate', x.get('max_bitrate', 0)),
            reverse=True
        )
        return sorted_profiles[0]
    return None


def select_best_audio_profile(audio_profiles, file_type):
    """Select the best audio profile for a specific file type, sorted by bitrate."""
    audio_profiles_filtered = [p for p in audio_profiles if p.get('file_type') == file_type]
    if audio_profiles_filtered:
        sorted_profiles = sorted(
            audio_profiles_filtered,
            key=lambda x: x.get('bitrate', x.get('max_bitrate', 0)),
            reverse=True
        )
        return sorted_profiles[0]
    return None


async def download_with_retry(session, url, filename, output_dir, base_urls, max_retries=3):
    """Download a file with retry mechanism."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / filename


    if file_path.exists() and os.path.getsize(file_path) > 0:
        logging.info(f"File already exists, non-empty, and valid: {file_path}")
        return True


    for attempt in range(max_retries):
        for base in base_urls:
            full_url = base + url
            try:
                async with session.get(full_url, timeout=7) as response:
                    if response.status == 200:
                        with open(file_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(1024)
                                if not chunk:
                                    break
                                f.write(chunk)


                        if os.path.getsize(file_path) > 0:
                            #logging.info(f"Downloaded and validated: {file_path} (size: {os.path.getsize(file_path)} bytes)")
                            return True
                        else:
                            logging.warning(f"Downloaded file is invalid or empty: {file_path}")
                            file_path.unlink(missing_ok=True)
                            continue
            except Exception as e:
                logging.warning(f"Attempt {attempt+1} failed for {full_url}: {e}")


    logging.error(f"Could not download {filename} after {max_retries} attempts")
    return False


async def download_subtitles(session, output_dir, subtitle_template, language_codes, base_urls):
    """Download subtitles in all available languages."""
    output_dir = Path(output_dir) / "subtitles"
    output_dir.mkdir(parents=True, exist_ok=True)


    tasks = []
    for lang in language_codes:
        sub_url = subtitle_template.replace("{{language_code}}", lang)
        sub_filename = f"subtitle_{lang}.vtt"
        tasks.append(
            download_with_retry(session, sub_url, sub_filename, output_dir, base_urls)
        )


    results = await asyncio.gather(*tasks)
    return all(results)


async def download_spritemap(session, output_dir, spritemap_template, spritemap_id, base_urls):
    """Download spritemap image."""
    output_dir = Path(output_dir) / "spritemaps"
    output_dir.mkdir(parents=True, exist_ok=True)


    spritemap_url = spritemap_template.replace("{{spritemap_id}}", str(spritemap_id))
    spritemap_filename = f"spritemap_{spritemap_id}.jpg"


    return await download_with_retry(session, spritemap_url, spritemap_filename, output_dir, base_urls)


class Downloader:
    """Handles downloading of initialization segments, subtitles, spritemaps, and media segments."""


    def __init__(self, data, session, output_base_dir):
        self.data = data
        self.session = session
        self.base_urls = data.get('base_urls', [])
        self.subtitle_base_urls = data.get('subtitle_base_urls', [])
        self.spritemap_base_urls = data.get('spritemap_base_urls', [])
        self.output_base_dir = Path(output_base_dir)


    async def download_initialization_segments(self, video_profile, audio_profile, format_name):
        """Download video and audio initialization segments."""
        video_ext = video_profile['file_type']
        audio_ext = audio_profile['file_type']


        init_video_url = unquote(self.data['initialization_template']).replace(
            "{{profile_id}}", str(video_profile['id'])
        ).replace("{{file_type}}", video_ext)
        
        init_audio_url = unquote(self.data['initialization_template']).replace(
            "{{profile_id}}", str(audio_profile['id'])
        ).replace("{{file_type}}", audio_ext)


        video_dir = self.output_base_dir / format_name / "video"
        audio_dir = self.output_base_dir / format_name / "audio"


        await asyncio.gather(
            download_with_retry(self.session, init_video_url, f"init_video.{video_ext}", video_dir, self.base_urls),
            download_with_retry(self.session, init_audio_url, f"init_audio.{audio_ext}", audio_dir, self.base_urls)
        )


    async def download_subtitles(self):
        """Download subtitles for all available languages."""
        if 'subtitle_template' not in self.data or 'subtitle_language_codes' not in self.data:
            return {}


        subtitle_success = await download_subtitles(
            self.session,
            self.output_base_dir,
            self.data['subtitle_template'],
            self.data['subtitle_language_codes'],
            self.subtitle_base_urls
        )


        if subtitle_success:
            return {lang: self.output_base_dir / "subtitles" / f"subtitle_{lang}.vtt"
                    for lang in self.data['subtitle_language_codes']}
        return {}


    async def download_spritemap(self):
        """Download spritemap if available."""
        if 'spritemap_template' in self.data and 'spritemaps' in self.data and self.data['spritemaps']:
            await download_spritemap(
                self.session,
                self.output_base_dir,
                self.data['spritemap_template'],
                self.data['spritemaps'][0]['id'],
                self.spritemap_base_urls
            )


    async def download_segments(self, video_profile, audio_profile, segment_count, format_name):
        """Download video and audio segments in batches."""
        segment_template = unquote(self.data['segment_template'])
        video_ext = video_profile['file_type']
        audio_ext = audio_profile['file_type']


        video_dir = self.output_base_dir / format_name / "video"
        audio_dir = self.output_base_dir / format_name / "audio"


        batch_size = 40
        for start_index in range(0, segment_count, batch_size):
            end_index = min(start_index + batch_size, segment_count)
            tasks = []


            for i in range(start_index, end_index):
                segment_timestamp = i * 4


                video_segment_url = segment_template.replace(
                    "{{profile_id}}", str(video_profile['id'])
                ).replace("{{segment_timestamp}}", str(segment_timestamp)).replace(
                    "{{file_type}}", video_ext
                )


                audio_segment_url = segment_template.replace(
                    "{{profile_id}}", str(audio_profile['id'])
                ).replace("{{segment_timestamp}}", str(segment_timestamp)).replace(
                    "{{file_type}}", audio_ext
                )


                video_filename = f"{segment_timestamp}.{video_ext}"
                audio_filename = f"{segment_timestamp}.{audio_ext}"


                tasks.extend([
                    download_with_retry(self.session, video_segment_url, video_filename, video_dir, self.base_urls),
                    download_with_retry(self.session, audio_segment_url, audio_filename, audio_dir, self.base_urls)
                ])


            await asyncio.gather(*tasks, return_exceptions=True)


class SpotifyDownloader:
    """Orchestrates the Spotify video downloading workflow."""


    def __init__(self, data):
        self.data = data
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        self.output_base_dir = Path(f"spotify_video_{timestamp}")
        self.mp4_has_drm = False  # DRM flag
        self.shared_decryption_key = None  # 共享的解密金鑰


    def select_profiles_for_format(self, file_type):
        """Select best video and audio profiles for a specific format."""
        # 修改：從 contents[0] 中分別讀取 video_profiles 和 audio_profiles
        video_profiles = self.data['contents'][0]['video_profiles']
        audio_profiles = self.data['contents'][0]['audio_profiles']

        video_profile = select_best_video_profile(video_profiles, file_type)
        audio_profile = select_best_audio_profile(audio_profiles, file_type)


        if not video_profile or not audio_profile:
            return None, None


        # Video Profile 日誌
        logging.info(
            f"\033[95m[{file_type.upper()}]\033[0m Selected video profile: "
            f"\033[96m{video_profile.get('width', 'N/A')}x{video_profile.get('height', 'N/A')}\033[0m, "
            f"codec: \033[96m{video_profile.get('codec', 'N/A')}\033[0m, "
            f"bitrate: \033[93m{int(video_profile.get('bitrate', video_profile.get('max_bitrate', 0))) / 1000:.2f} kbps\033[0m"
        )


        # Audio Profile 日誌
        logging.info(
            f"\033[95m[{file_type.upper()}]\033[0m Selected audio profile: "
            f"codec: \033[92m{audio_profile.get('codec', 'N/A')}\033[0m, "
            f"bitrate: \033[93m{int(audio_profile.get('bitrate', 0)) / 1000:.2f} kbps\033[0m"
        )


        return video_profile, audio_profile


    def calculate_segment_count(self):
        """Calculate the number of segments based on duration and segment length."""
        total_duration = self.data['end_time_millis'] - self.data['start_time_millis']
        segment_length_ms = self.data['contents'][0]['segment_length'] * 1000
        segment_count = (total_duration // segment_length_ms) + (1 if total_duration % segment_length_ms else 0)


        logging.info(
            f"{"\033[36m"}Total duration: {total_duration} ms{"\033[0m"} | "
            f"{"\033[33m"}Segment length: {segment_length_ms} ms{"\033[0m"} | "
            f"{"\033[32m"}Segment count: {segment_count}{"\033[0m"}"
        )
        return segment_count


    async def run(self):
        """Run the download workflow for both MP4 and WebM."""
        segment_count = self.calculate_segment_count()


        # 固定順序：先下載 MP4，再下載 WebM
        formats_to_download = [
            ('mp4', 'MP4'),
            ('webm', 'WebM')
        ]


        async with aiohttp.ClientSession() as session:
            downloader = Downloader(self.data, session, self.output_base_dir)


            # Download subtitles and spritemap once (shared)
            subtitle_files = await downloader.download_subtitles()
            await downloader.download_spritemap()


            # Download each format
            for idx, (file_type, format_name) in enumerate(formats_to_download):
                logging.info(f"開始下載 {format_name} 格式")


                video_profile, audio_profile = self.select_profiles_for_format(file_type)


                if not video_profile or not audio_profile:
                    logging.warning(f"無法找到 {format_name} 格式的有效 profile，跳過")
                    continue


                await downloader.download_initialization_segments(video_profile, audio_profile, format_name)
                await downloader.download_segments(video_profile, audio_profile, segment_count, format_name)


                # Merge fragments
                merger = MP4FragmentMerger(self.output_base_dir / format_name)


                # 如果是第一個格式 (MP4)，正常檢測 DRM
                # 如果是第二個格式 (WebM)，且 MP4 有 DRM，則強制視為有 DRM
                if idx == 0:  # MP4
                    video_ok, audio_ok, video_has_drm, audio_has_drm = await merger.merge_all(
                        video_profile['file_type'],
                        audio_profile['file_type']
                    )
                    # 設置 DRM flag
                    if video_has_drm or audio_has_drm:
                        self.mp4_has_drm = True
                else:  # WebM
                    video_ok, audio_ok, _, _ = await merger.merge_all(
                        video_profile['file_type'],
                        audio_profile['file_type'],
                        force_drm=self.mp4_has_drm  # 強制使用 MP4 的 DRM 狀態
                    )
                    video_has_drm = self.mp4_has_drm
                    audio_has_drm = self.mp4_has_drm


                if video_ok and audio_ok:
                    muxer = SubtitleMuxer(self.output_base_dir, self.shared_decryption_key)
                    video_file = self.output_base_dir / format_name / f"video.{video_profile['file_type']}"
                    audio_file = self.output_base_dir / format_name / f"audio.{audio_profile['file_type']}"


                    if file_type == 'mp4':
                        output_file = self.output_base_dir / "spotify_video.720p.Spotify.WEB-DL.H264.AAC.mp4"
                    else:
                        output_file = self.output_base_dir / "spotify_video.720p.Spotify.WEB-DL.VP9.Opus.mkv"


                    mux_success = await muxer.mux_files(
                        video_file, 
                        audio_file, 
                        output_file,
                        video_has_drm,
                        audio_has_drm
                    )


                    # 保存第一次輸入的解密金鑰供後續使用
                    if mux_success and muxer.last_used_key:
                        self.shared_decryption_key = muxer.last_used_key


                    if mux_success:
                        logging.info(f"\033[94m✓ {format_name} 格式處理完成:\033[0m {output_file}")
                    else:
                        logging.warning(f"✗ {format_name} 格式合併失敗")
                else:
                    logging.warning(f"✗ {format_name} 格式片段合併失敗")


class DRMDetector:
    """使用系統中的 mp4dump 檢測 MP4 檔案中的 DRM 保護"""


    async def _run_mp4dump(self, file_path: Path):
        """執行 mp4dump 並分析輸出"""
        proc = await asyncio.create_subprocess_exec(
            "mp4dump",
            str(file_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )


        stdout, stderr = await proc.communicate()
        output = stdout.decode().lower()


        drm_markers = [
            "sinf", "schm", "schi", "encrypted", "drm",
            "pssh", "fairplay", "widevine", "playready"
        ]


        has_drm = any(marker in output for marker in drm_markers)
        return has_drm, output


    async def check_file(self, file_path: Path) -> bool:
        """檢查單個檔案是否有 DRM"""
        if not file_path.exists():
            logging.error(f"檔案不存在: {file_path}")
            return False


        try:
            has_drm, _ = await self._run_mp4dump(file_path)
            if has_drm:
                logging.warning(f"\033[35m檢測到 DRM 保護: {file_path}\033[0m")
            else:
                pass
            return has_drm
        except FileNotFoundError:
            logging.error("未找到 mp4dump 命令，請確保 Bento4 工具已安裝並在 PATH 中")
            return False
        except Exception as e:
            logging.error(f"檢測 DRM 時出錯 ({file_path}): {str(e)}")
            return False


class SubtitleMuxer:
    """處理字幕識別與混流，包含 DRM 解密處理（使用 Shaka Packager）"""


    def __init__(self, base_dir: Path, shared_key=None):
        self.subs_dir = base_dir / "subtitles"
        self.base_dir = base_dir
        self.shared_key = shared_key
        self.last_used_key = None


    def find_subtitles(self):
        sub_path = self.subs_dir / "subtitle_en.vtt"
        if sub_path.exists():
            return {"en": sub_path}
        return {}


    def parse_key_input(self, user_input: str):
        """
        解析使用者輸入的金鑰格式
        支援格式：
        1. key_id=xxx:key=yyy
        2. xxx:yyy (自動識別為 key_id:key)
        """
        user_input = user_input.strip()


        # 格式 1: key_id=xxx:key=yyy
        if "key_id=" in user_input and ":key=" in user_input:
            parts = user_input.split(":key=")
            key_id = parts[0].replace("key_id=", "").strip()
            key = parts[1].strip()
            return key_id, key


        # 格式 2: xxx:yyy
        elif ":" in user_input and "=" not in user_input:
            parts = user_input.split(":")
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()


        # 無法解析
        raise ValueError("金鑰格式錯誤，請使用 'key_id=xxx:key=yyy' 或 'xxx:yyy' 格式")


    async def decrypt_with_packager(self, input_file: Path, output_file: Path, key_id: str, key: str, stream_type: str):
        """
        使用 Shaka Packager 解密檔案
        stream_type: 'video' 或 'audio'
        """
        cmd = [
            "packager",
            f"input={str(input_file)},stream={stream_type},output={str(output_file)}",
            "--enable_raw_key_decryption",
            f"--keys=key_id={key_id}:key={key}"
        ]


        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )


        stdout, stderr = await proc.communicate()


        if proc.returncode != 0:
            error_msg = stderr.decode().strip()
            logging.error(f"Packager 解密失敗: {error_msg}")
            return False


        logging.info(f"\033[38;5;244m成功解密:\033[0m \033[37m{output_file}\033[0m")
        return True


    async def mux_files(self, video: Path, audio: Path, output: Path, video_has_drm: bool = False, audio_has_drm: bool = False) -> bool:
        video_final = video
        audio_final = audio


        temp_video = video.parent / "video_decrypted.mp4"
        temp_audio = audio.parent / "audio_decrypted.mp4"


        try:
            # 如果檢測到 DRM 或強制視為有 DRM
            if video_has_drm or audio_has_drm:
                # 如果已經有共享的金鑰，直接使用
                if self.shared_key:
                    key_id, key = self.parse_key_input(self.shared_key)
                else:
                    # 提示使用者輸入金鑰
                    print("\033[90m=\033[0m" * 40)
                    user_key = input("檢測到 DRM 保護，請輸入解密金鑰 (格式: key_id=xxx:key=yyy 或 xxx:yyy): ")
                    print("\033[90m=\033[0m" * 40)
                    try:
                        key_id, key = self.parse_key_input(user_key)
                        self.last_used_key = user_key
                    except ValueError as e:
                        logging.error(str(e))
                        return False


                # 解密影像
                if video_has_drm:
                    success = await self.decrypt_with_packager(
                        video, temp_video, key_id, key, "video"
                    )
                    if not success:
                        logging.error("影像解密失敗")
                        return False
                    video_final = temp_video


                # 解密聲音
                if audio_has_drm:
                    success = await self.decrypt_with_packager(
                        audio, temp_audio, key_id, key, "audio"
                    )
                    if not success:
                        logging.error("聲音解密失敗")
                        return False
                    audio_final = temp_audio


            # 混流
            subtitles = self.find_subtitles()
            sub_path = subtitles.get("en")
            ext = Path(output).suffix.lower()


            cmd = [
                "ffmpeg",
                "-y",
                "-i", str(video_final),
                "-i", str(audio_final),
            ]


            if sub_path:
                cmd += ["-i", str(sub_path)]


            cmd += ["-map", "0:v:0", "-map", "1:a:0"]


            if sub_path:
                cmd += ["-map", "2:s:0"]


            cmd += ["-c:v", "copy", "-c:a", "copy"]


            if sub_path:
                if ext in (".mp4", ".m4v", ".mov"):
                    cmd += ["-c:s", "mov_text"]
                elif ext in (".mkv", ".webm"):
                    cmd += ["-c:s", "webvtt"]
                else:
                    cmd += ["-c:s", "webvtt"]


            if sub_path:
                cmd += [
                    "-metadata:s:s:0", "language=en",
                    "-metadata:s:s:0", "title=Spotify auto generate",
                ]


            cmd += [str(output)]


            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )


            _, err = await proc.communicate()


            if proc.returncode == 0:
                return True
            else:
                logging.error(f"混流失敗: {err.decode().strip()}")
                return False


        except Exception as e:
            logging.error(f"混流或解密過程中發生錯誤: {str(e)}")
            return False


        finally:
            # 清理臨時解密檔案
            for temp_file in [temp_video, temp_audio]:
                if temp_file.exists():
                    try:
                        temp_file.unlink()
                    except Exception:
                        pass


class MP4FragmentMerger:
    def __init__(self, base_dir, timeout=60, retries=2):
        self.base_dir = Path(base_dir)
        self.temp_dir = self.base_dir / "tmp_frag_merge"
        self.temp_dir.mkdir(exist_ok=True, parents=True)
        self.timeout = timeout
        self.retries = retries
        self.drm_detector = DRMDetector()


    async def _run_cmd(self, cmd, cwd=None):
        for attempt in range(1, self.retries + 1):
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )


            try:
                _, err = await asyncio.wait_for(proc.communicate(), timeout=self.timeout)
                if proc.returncode == 0:
                    return
                raise RuntimeError(err.decode().strip())
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                err_msg = "Command timed out"
            except Exception as e:
                err_msg = str(e)


            if attempt < self.retries:
                logging.warning(
                    "Attempt %d/%d failed: %s – retrying",
                    attempt, self.retries, err_msg
                )
                await asyncio.sleep(1)
            else:
                raise RuntimeError(f"Command failed after {self.retries} attempts: {err_msg}")


    async def merge_type(self, media_type: str, ext: str) -> bool:
        media_dir = self.base_dir / media_type
        init_file = next(media_dir.glob(f"init_*.{ext}"), None)
        segments = sorted(
            [f for f in media_dir.glob(f"*.{ext}") if not init_file or f.name != init_file.name],
            key=lambda f: int(f.stem) if f.stem.isdigit() else f.name
        )


        if not segments:
            logging.error("找不到任何片段: %s/*.%s", media_dir, ext)
            return False


        temp_full = self.temp_dir / f"{media_type}_full.{ext}"
        final_output = self.base_dir / f"{media_type}.{ext}"
        list_file = self.temp_dir / f"{media_type}_list.txt"


        with open(list_file, 'w', encoding='utf-8') as f:
            if init_file and init_file.exists():
                f.write(f'"{init_file}"\n')
            for seg in segments:
                f.write(f'"{seg}"\n')


        try:
            with open(temp_full, 'wb') as outfile:
                if init_file and init_file.exists():
                    outfile.write(init_file.read_bytes())
                else:
                    logging.warning(f"No init file found for {media_type}, proceeding with segments only")


                for seg in segments:
                    outfile.write(seg.read_bytes())


            if final_output.exists():
                final_output.unlink()


            temp_full.rename(final_output)
            return True


        except Exception as e:
            logging.error("合併檔案時發生錯誤: %s", str(e))
            return False


    async def merge_all(self, video_ext: str, audio_ext: str, force_drm: bool = False):
        """
        合併所有片段並檢測 DRM
        force_drm: 如果為 True，則強制視為有 DRM (用於 WebM 格式)
        """
        try:
            video_ok = await self.merge_type("video", video_ext)
            audio_ok = await self.merge_type("audio", audio_ext)


            video_has_drm = False
            audio_has_drm = False


            if force_drm:
                # 強制視為有 DRM
                video_has_drm = True
                audio_has_drm = True
            else:
                # 正常檢測 DRM
                if video_ok:
                    video_file = self.base_dir / f"video.{video_ext}"
                    video_has_drm = await self.drm_detector.check_file(video_file)


                if audio_ok:
                    audio_file = self.base_dir / f"audio.{audio_ext}"
                    audio_has_drm = await self.drm_detector.check_file(audio_file)


            return video_ok, audio_ok, video_has_drm, audio_has_drm


        finally:
            for tmp in self.temp_dir.iterdir():
                try:
                    tmp.unlink()
                except Exception:
                    pass


async def main():
    """Main entry point for the script"""
    spotify_downloader = SpotifyDownloader(data)
    await spotify_downloader.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
