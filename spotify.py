import re
import os
import sys
import json
import shutil
import asyncio
import aiohttp
import logging
import time
import subprocess
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode
from functools import wraps
from aiohttp import ClientSession, ClientTimeout


# 配置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# Configuration parameters
MAX_VIDEO_PROFILE_TEST = 36  # Max video track profile ID to test
MAX_AUDIO_PROFILE_TEST = 36  # Max audio profile ID to test
SEGMENT_STEP = 4    # Segment index step, Spotify limit is 4
TIMEOUT_SECONDS = 7   # Download timeout duration (seconds)
RETRY_ATTEMPTS = 1    # Retry attempts for a single download
LIVE_RETRY_LIMIT = 1  # Retry limit for judging end of live stream
REQUEST_INTERVAL = 0 # Request interval (seconds)

default_video_profile = 0
default_audio_profile = 7

# Download retry decorator
def retry_on_failure(retries=RETRY_ATTEMPTS):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries - 1:
                        logger.error(f"Function {func.__name__} failed after {retries} retries: {str(e)}")
                        raise
                    logger.warning(f"Function {func.__name__} attempt {attempt + 1} failed: {str(e)}")
        return wrapper
    return decorator

class SpotifyLiveDownloader:
    """Spotify 直播影片下載器類"""

    def __init__(self, input_url):
        self.input_url = input_url
        self.parsed = urlparse(input_url)
        self.query_params = parse_qs(self.parsed.query)
        self.base_token = self.query_params['token'][0]
        self.token_ak = self.query_params['token_ak'][0]
        self.base_url = re.sub(r'/profiles/.*$', '', input_url.split('?')[0])
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.working_dir = os.path.join("spotify", self.timestamp)
        self.headers = {
            "Accept-Encoding": "identity",
            "User-Agent": "Spotify/8.6.10 Linux; Android 15; SM-X710",
            "Accept-Language": "ko-KR;q=1.00, en-KR;q=0.50, *;q=0.01",
            'Connection': 'keep-alive',
            'Referer': 'https://open.spotify.com/',
            'Origin': 'https://open.spotify.com',
        }

    def setup_directories(self):
        """設置工作目錄"""
        os.makedirs(self.working_dir, exist_ok=True)
        logger.info(f"\033[92mThe working directory has been created: {self.working_dir}\033[0m")

    def build_url(self, profile_id, segment_type, segment_index=None):
        """構建完整下載 URL"""
        params = {"token": self.base_token, "token_ak": self.token_ak}
        
        if segment_type == "init":
            path = f"{self.base_url}/profiles/{profile_id}/inits/mp4"
        else:
            path = f"{self.base_url}/profiles/{profile_id}/{segment_index}.mp4"

        return f"{path}?{urlencode(params)}"

    @retry_on_failure()
    async def download_file(self, session: ClientSession, url: str, filename: str):
        """異步下載文件"""
        
        try:
            async with session.get(url, timeout=ClientTimeout(total=TIMEOUT_SECONDS)) as response:
                status_code = response.status
                
                if status_code == 404:
                    return False
                if status_code == 403:
                    logger.info(f"Attempting to download: {url}")
                    logger.info(f"HTTP status code: {status_code} - URL: {url}")
                    logger.error(f"Access denied (403): {url}")
                    return False
                if status_code >= 500:
                    logger.info(f"Attempting to download: {url}")
                    logger.info(f"HTTP status code: {status_code} - URL: {url}")
                    logger.error(f"Server error ({status_code}): {url}")
                    return False
                if status_code == 200:
                    content = await response.read()
                    with open(filename, 'wb') as f:
                        f.write(content)
                    return True
                else:
                    logger.warning(f"Unhandled status code ({status_code}): {url}")
                return False
        except asyncio.TimeoutError:
            logger.error(f"Download timeout: {url}")
            return False


    async def download_init(self, session: ClientSession, profile_id: int, output_dir: str, track_type: str):
        """下載並返回初始化文件資訊（音訊檢測到有效結果立即返回）"""
        init_url = self.build_url(profile_id, "init")
        init_file = os.path.join(output_dir, f"init_{profile_id}_{track_type}.mp4")

        if not await self.download_file(session, init_url, init_file):
            return None

        cmd = ['mp4info', init_file]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = result.stdout

        if track_type == "video":
            try:
                width = int(re.search(r'Width:\s+(\d+)', output).group(1))
                height = int(re.search(r'Height:\s+(\d+)', output).group(1))
                return {
                    'file': init_file,
                    'width': width,
                    'height': height,
                    'profile': profile_id,
                    'type': track_type
                }
            except AttributeError as e:
                if "object has no attribute 'group'" in str(e):
                    pass
                else:
                    logging.error(f"Unable to parse video initialization file: {init_file} - error: {str(e)}")
        else:
            codec_match = re.search(r'Coding:\s+(\w+)', output)
            codec = codec_match.group(1) if codec_match else None
            
            channels_match = re.search(r'Channels:\s+(\d+)', output)
            sample_rate_match = re.search(r'Sample Rate:\s+(\d+)', output)
            
            if not all([codec, channels_match, sample_rate_match]):
                return None
                
            channels = int(channels_match.group(1))
            sample_rate = int(sample_rate_match.group(1))
            
            return {
                'file': init_file,
                'codec': codec,
                'channels': str(channels),
                'sample_rate': str(sample_rate),
                'profile': profile_id,
                'type': track_type
            }

        
    async def find_best_profile(self, max_profile, track_type="video"):
        """異步檢測最佳 profile"""
        temp_dir = os.path.join(self.working_dir, f"temp_init_{track_type}")
        os.makedirs(temp_dir, exist_ok=True)

        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = [self.download_init(session, profile_id, temp_dir, track_type)
                    for profile_id in range(0, max_profile + 1)]
            results = await asyncio.gather(*tasks)
            valid_profiles = [r for r in results if r is not None]

            if not valid_profiles:
                logger.warning(f"Warning: No available {track_type} profile，Use default values")
                if track_type == "video":
                    # 手動指定預設視訊 profile
                    default_profile = default_video_profile
                    logger.info(f"\033[31mUse default video track profile {default_profile}\033[0m")
                    
                    # 下載預設 profile 的 init 文件
                    init_url = self.build_url(default_profile, "init")
                    init_file = os.path.join(temp_dir, f"init_{default_profile}_{track_type}.mp4")
                    if not await self.download_file(session, init_url, init_file):
                        logger.error("Unable to download default video initialization file")
                        return None
                    
                    return default_profile
                else:
                    # 手動指定預設音訊 profile
                    default_profile = default_audio_profile
                    logger.info(f"\033[31mUse default audio track profile {default_profile}\033[0m")
                    
                    # 下載預設 profile 的 init 文件
                    init_url = self.build_url(default_profile, "init")
                    init_file = os.path.join(temp_dir, f"init_{default_profile}_{track_type}.mp4")
                    if not await self.download_file(session, init_url, init_file):
                        logger.error("Unable to download default audio initialization file")
                        return None
                    
                    return default_profile

            if track_type == "video":
                best_profile = max(valid_profiles, key=lambda x: x['width'] * x['height'])
            else:
                best_profile = max(valid_profiles, key=lambda x: int(x['channels']))

            # 保留最佳 profile 的 init 文件
            best_init = os.path.join(temp_dir, f"init_{best_profile['profile']}_{track_type}.mp4")
            for item in os.listdir(temp_dir):
                if item != os.path.basename(best_init):
                    os.remove(os.path.join(temp_dir, item))

            return best_profile['profile']

    async def download_single_segment(self, session: ClientSession, profile_id: int, track_type: str, segment_index: int, output_dir: str):
        """下載單個分段（簡化文件名格式）"""
        segment_url = self.build_url(profile_id, "segment", segment_index)
        segment_file = os.path.join(output_dir, f"{segment_index}.mp4")
        
        try:
            if await self.download_file(session, segment_url, segment_file):
                return True
            else:
                logger.warning(f"{track_type} track section {segment_index} Download failed")
                return False
        except Exception as e:
            logger.error(f"{track_type} track section {segment_index} Download error: {str(e)}")
            return False

    @staticmethod
    def merge_bin_files(input_dir: str, output_file: str):
        """合併二進位分段文件（適應新的文件名格式）"""
        # 獲取所有.mp4文件並按數字排序（排除init文件）
        bin_files = [f for f in os.listdir(input_dir) if f.endswith('.mp4') and not f.startswith('init.')]
        
        # 按文件名數字部分排序
        try:
            bin_files.sort(key=lambda x: int(x.split('.')[0]))
        except ValueError:
            logger.error("Segmented file name format error, cannot be converted to numbers")
            return False

        if not bin_files:
            logger.error(f"Error: Segmentation file not found at {input_dir}")
            return False

        try:
            total_size = 0
            init_file = os.path.join(input_dir, [f for f in os.listdir(input_dir) if f.startswith('init.')][0])
            
            with open(output_file, 'wb') as outfile:
                # 寫入初始化段
                with open(init_file, 'rb') as infile:
                    data = infile.read()
                    outfile.write(data)
                    total_size += len(data)
                
                # 寫入所有分段
                for bin_file in bin_files:
                    file_path = os.path.join(input_dir, bin_file)
                    with open(file_path, 'rb') as infile:
                        data = infile.read()
                        outfile.write(data)
                        total_size += len(data)
            
            logger.info(f"Successful merger {len(bin_files)} each segment to {output_file} (Total size: {total_size/1024/1024:.2f} MB)")
            return True
        except Exception as e:
            logger.error(f"Merge file failed: {output_file} - error: {str(e)}")
            return False
        
    @staticmethod
    def mux_audio_video(video_file: str, audio_file: str, output_file: str):
        """混流音影片"""
        try:
            logger.info(f"Start mux audio and video: {video_file} + {audio_file} -> {output_file}")
            
            # 先檢查文件大小
            video_size = os.path.getsize(video_file) / 1024 / 1024
            audio_size = os.path.getsize(audio_file) / 1024 / 1024
            logger.info(f"\033[90m Video file size: {video_size:.2f} MB, Audio file size: {audio_size:.2f} MB \033[0m")
            
            cmd = [
                'ffmpeg',
                '-i', video_file,
                '-i', audio_file,
                '-c', 'copy',
                '-movflags', 'faststart',
                '-y',
                output_file
            ]
            
            # 捕獲 ffmpeg 輸出以便日誌記錄
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Mixed flow failure (return code {result.returncode}): {result.stderr}")
                return False
                
            output_size = os.path.getsize(output_file) / 1024 / 1024
            logger.info(f"\033[95m mux successful! Output file size: {output_size:.2f} MB \033[0m")
            return True
        except Exception as e:
            logger.error(f"Mixed audio video failure: {str(e)}")
            return False

    @staticmethod
    def verify_video(file_path: str):
        """驗證影片文件完整性"""
        try:
            logger.info(f"Start verifying video files: {file_path}")
            cmd = ['ffprobe', '-v', 'error', '-show_format', '-show_streams', file_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # 分析輸出以獲取更多信息
            duration_match = re.search(r"duration=(\d+\.\d+)", result.stdout)
            if duration_match:
                logger.info(f"Film length: {float(duration_match.group(1)):.2f} s")
            
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Movie file verification failed (return code {e.returncode}): {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"Video file verification failed: {file_path} - error: {str(e)}")
            return False

    async def process_downloads(self):
        """處理直播下載流程（音訊與影像交替下載）"""
        self.setup_directories()

        # 查找最佳影片和音訊 profile
        video_profile = await self.find_best_profile(MAX_VIDEO_PROFILE_TEST, "video")
        audio_profile = await self.find_best_profile(MAX_AUDIO_PROFILE_TEST, "audio")
        
        if not video_profile or not audio_profile:
            logger.error("Unable to find the best profile")
            video_profile = default_video_profile
            audio_profile = default_audio_profile
            logger.error(f"Hard-coded selection video_profile: {video_profile}, audio_profile: {audio_profile}")

        video_dir = os.path.join(self.working_dir, "video")
        audio_dir = os.path.join(self.working_dir, "audio")
        os.makedirs(video_dir, exist_ok=True)
        os.makedirs(audio_dir, exist_ok=True)

        # 移動初始化文件到正確位置
        video_init = os.path.join(self.working_dir, f"temp_init_video/init_{video_profile}_video.mp4")
        audio_init = os.path.join(self.working_dir, f"temp_init_audio/init_{audio_profile}_audio.mp4")
        
        shutil.move(video_init, os.path.join(video_dir, "init.video.mp4"))
        shutil.move(audio_init, os.path.join(audio_dir, "init.audio.mp4"))
        
        # 清理臨時目錄
        shutil.rmtree(os.path.join(self.working_dir, "temp_init_video"))
        shutil.rmtree(os.path.join(self.working_dir, "temp_init_audio"))

        # 交替下載音訊和影像分段
        async with aiohttp.ClientSession(headers=self.headers) as session:
            segment_index = 0
            video_failure_count = 0
            audio_failure_count = 0
            video_downloaded_count = 0
            audio_downloaded_count = 0

            while (video_failure_count < LIVE_RETRY_LIMIT and 
                audio_failure_count < LIVE_RETRY_LIMIT):
                # 下載影像分段
                if video_failure_count < LIVE_RETRY_LIMIT:
                    if await self.download_single_segment(session, video_profile, "video", segment_index, video_dir):
                        video_failure_count = 0
                        video_downloaded_count += 1
                    else:
                        video_failure_count += 1
                        logger.info(f"Image track segmentation {segment_index} fail {video_failure_count}/{LIVE_RETRY_LIMIT}")

                # 下載音訊分段
                if audio_failure_count < LIVE_RETRY_LIMIT:
                    if await self.download_single_segment(session, audio_profile, "audio", segment_index, audio_dir):
                        audio_failure_count = 0
                        audio_downloaded_count += 1
                    else:
                        audio_failure_count += 1
                        logger.info(f"Audio track segmentation {segment_index} fail {audio_failure_count}/{LIVE_RETRY_LIMIT}")

                segment_index += SEGMENT_STEP
                await asyncio.sleep(REQUEST_INTERVAL)

            logger.info(f"\033[93mLive stream download complete: Number of video segments {video_downloaded_count}, Number of audio segments {audio_downloaded_count}\033[0m")
            logger.info(f"Number of failed images: {video_failure_count}, Number of audio failures: {audio_failure_count}")

            if video_failure_count >= LIVE_RETRY_LIMIT or audio_failure_count >= LIVE_RETRY_LIMIT:
                logger.info("\033[90mLive broadcast ended, consecutive failures reached limit\033[0m")


            # 檢查下載的分段數是否合理
            if video_downloaded_count > 0 and audio_downloaded_count > 0:
                await self.process_output(video_dir, audio_dir)
            else:
                logger.error("Not enough segments downloaded to continue processing")


    def check_encryption(self, file_path: str) -> bool:
        """使用mp4info檢查文件是否加密"""
        try:
            cmd = ['mp4info', file_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # 檢查輸出中是否包含[ENCRYPTED]標記
            if '[ENCRYPTED]' in result.stdout:
                logger.warning(f"Encrypted content detected: {file_path}")
                return True
            return False
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to execute mp4info: {e.stderr}")
            return False  # 假設檢查失敗時不加密
        except Exception as e:
            logger.error(f"An error occurred while checking the encryption status: {str(e)}")
            return False
    
    
    async def process_output(self, video_dir: str, audio_dir: str):
        """處理直播輸出文件"""
        video_raw = os.path.join(self.working_dir, "video_raw.mp4")
        audio_raw = os.path.join(self.working_dir, "audio_raw.mp4")
        final_output = os.path.join(self.working_dir, "spotify_video.mp4")

        # 合併分段文件
        if not self.merge_bin_files(video_dir, video_raw):
            logger.error("Error: Video segment merging failed")
            return
        
        if not self.merge_bin_files(audio_dir, audio_raw):
            logger.error("Error: Audio segment merging failed")
            return

        # 檢查加密狀態
        video_encrypted = self.check_encryption(video_raw)
        audio_encrypted = self.check_encryption(audio_raw)
        
        if video_encrypted or audio_encrypted:
            logger.error("\033[36mError: Encrypted content detected, cannot mix streams\033[0m")
            logger.info(f"Video tracks are stored in: {video_raw}")
            logger.info(f"Audio tracks stored in: {audio_raw}")
            return
        
        # 執行混流
        if self.mux_audio_video(video_raw, audio_raw, final_output):
            if self.verify_video(final_output):
                logger.info(f"\033[93m Processing complete, final live stream video saved to:\033[0m {final_output}")
                # self.cleanup(video_dir, audio_dir, video_raw, audio_raw)
            else:
                logger.warning("Warning: Final live stream file verification failed.")
        else:
            logger.error("Error: Live audio and video mux failed.")
            
    def cleanup(self, video_dir: str, audio_dir: str, video_raw: str, audio_raw: str):
        """清理臨時文件"""
        try:
            shutil.rmtree(video_dir)
            shutil.rmtree(audio_dir)
            os.remove(video_raw)
            os.remove(audio_raw)
            logger.info("Temporary files have been cleaned up.")
        except Exception as e:
            logger.error(f"Failed to clean up temporary files: {str(e)}")

def get_remaining_time(url_string: str) -> timedelta | None:
    match = re.search(r'exp%3D(\d+)', url_string)
    
    if not match:
        return None
    
    timestamp = int(match.group(1))
    expiry_dt = datetime.fromtimestamp(timestamp)
    return expiry_dt - datetime.now()

def extract_urls_from_file(path):
    """從檔案中提取 URL"""
    urls = []
    try:
        if path.endswith('.json'):
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 假設 JSON 是列表格式或特定欄位，這裡以列表為例
                if isinstance(data, list):
                    urls = [str(item) for item in data if str(item).startswith("https://")]
                elif isinstance(data, dict):
                    # 如果是 dict，試著抓取所有 values 中符合 URL 的
                    urls = [v for v in data.values() if isinstance(v, str) and v.startswith("https://")]
        
        elif path.endswith('.txt'):
            with open(path, 'r', encoding='utf-8') as f:
                # 逐行讀取並去空格，過濾出 https:// 開頭的行
                urls = [line.strip() for line in f if line.strip().startswith("https://")]
                
    except Exception as e:
        logger.error(f"讀取檔案 {path} 時出錯: {e}")
    
    return urls

async def main():
    """主函數 - 支援 URL 與 檔案輸入"""
    if len(sys.argv) < 2:
        logger.error("使用方式: python3 spotify.py <url_or_file_path> ...")
        sys.exit(1)

    all_urls = []
    
    # 遍歷所有命令列參數
    for arg in sys.argv[1:]:
        if arg.startswith("https://"):
            all_urls.append(arg)
        elif os.path.isfile(arg):
            # 如果是檔案路徑，解析內容
            logger.info(f"檢測到檔案輸入: {arg}")
            found_urls = extract_urls_from_file(arg)
            all_urls.extend(found_urls)
        else:
            logger.warning(f"無效的輸入參數，已忽略: {arg}")

    if not all_urls:
        logger.error("未找到任何有效的 URL。")
        sys.exit(1)

    # 同步迴圈處理
    for index, url in enumerate(all_urls, start=1):
        logger.info(f"--- 處理中 ({index}/{len(all_urls)}) ---")
        logger.info(f"URL: {url}")
        
        try:
            remaining_time = get_remaining_time(url)
            logger.info(f"\033[96mToken time: {remaining_time if remaining_time else 'Unknown'} \033[0m")
            
            downloader = SpotifyLiveDownloader(url)
            await downloader.process_downloads()
            
        except Exception as e:
            logger.error(f"處理失敗: {url} | 錯誤: {e}")
            continue

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("User interruption")
        sys.exit(0)