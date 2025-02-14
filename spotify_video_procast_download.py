import json
import os
import aiohttp
import asyncio
from urllib.parse import unquote
import logging

# 設置日誌記錄
logging.basicConfig(level=logging.INFO)

# 1. 讀取本地 JSON 文件
with open('response.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 2. 選擇最佳畫質和特定格式的音質配置


def select_best_video_profile(profiles):
    video_profiles = [
        p for p in profiles if p['mime_type'].startswith('video/')]
    sorted_profiles = sorted(video_profiles,
                             key=lambda x: (
                                 x.get('video_height', 0), x['max_bitrate']),
                             reverse=True)
    return sorted_profiles[0] if video_profiles else None


def select_specific_audio_profile(profiles):
    audio_profiles = [p for p in profiles if p['file_type'] == 'ts']
    sorted_profiles = sorted(audio_profiles,
                             key=lambda x: x.get('audio_bitrate', 0),
                             reverse=True)
    return sorted_profiles[0] if audio_profiles else None


best_video_profile = select_best_video_profile(data['contents'][0]['profiles'])
specific_audio_profile = select_specific_audio_profile(
    data['contents'][0]['profiles'])
logging.info(
    f"Selected video profile: {best_video_profile['video_width']}x{best_video_profile['video_height']}")
logging.info(
    f"Selected audio profile: {specific_audio_profile['audio_bitrate']} bps")

# 3. 生成初始化分片 URL
init_video_url_template = unquote(data['initialization_template'])
init_video_url = init_video_url_template.replace("{{profile_id}}", str(best_video_profile['id'])) \
                                        .replace("{{file_type}}", best_video_profile['file_type'])

init_audio_url_template = unquote(data['initialization_template'])
init_audio_url = init_audio_url_template.replace("{{profile_id}}", str(specific_audio_profile['id'])) \
                                        .replace("{{file_type}}", specific_audio_profile['file_type'])

# 4. 生成分片 URL 模板
segment_template = unquote(data['segment_template'])
total_duration = data['end_time_millis'] - data['start_time_millis']
# 將 segment_length 轉換為毫秒
segment_length_ms = data['contents'][0]['segment_length'] * 1000

# 計算 segment_count，如果有餘數則加 1
segment_count = total_duration // segment_length_ms
if total_duration % segment_length_ms != 0:
    segment_count += 1

logging.info(f"Total duration: {total_duration} ms")
logging.info(f"Segment length: {segment_length_ms} ms")
logging.info(f"Segment count: {segment_count}")

# 5. 創建下載目錄
output_video_dir = "spotify_video/video"
output_audio_dir = "spotify_video/audio"
os.makedirs(output_video_dir, exist_ok=True)
os.makedirs(output_audio_dir, exist_ok=True)

# 6. 檢查是否所有分片都存在
all_video_segments_exist = all(
    os.path.exists(os.path.join(output_video_dir, f"segment_{i * 4:04d}.ts"))
    for i in range(segment_count)
)
all_audio_segments_exist = all(
    os.path.exists(os.path.join(output_audio_dir, f"segment_{i * 4:04d}.ts"))
    for i in range(segment_count)
)

if all_video_segments_exist and all_audio_segments_exist:
    logging.info(
        "All video and audio segments already exist. Skipping download.")
else:
    # 7. 異步下載文件
    async def download_file(session, url, filename, output_dir):
        for base in data['base_urls']:
            full_url = base + url
            try:
                async with session.get(full_url) as response:
                    if response.status == 200:
                        logging.info(f"Downloading {full_url}")
                        with open(os.path.join(output_dir, filename), 'wb') as f:
                            while True:
                                chunk = await response.content.read(1024)
                                if not chunk:
                                    break
                                f.write(chunk)
                        return True
            except Exception as e:
                logging.error(f"Error downloading {full_url}: {str(e)}")
        return False

    logging.info("Downloading initialization segments...")

    async def download_init_segments():
        async with aiohttp.ClientSession() as session:
            await download_file(session, init_video_url, "init_video.ts", output_video_dir)
            await download_file(session, init_audio_url, "init_audio.ts", output_audio_dir)

    asyncio.run(download_init_segments())

    # 8. 異步下載所有分片
    async def download_segments(start_index, end_index):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(start_index, end_index):
                segment_timestamp = i * 4 * 1000  # 每次遞增4秒（乘以1000轉換為毫秒）
                video_segment_url = segment_template.replace("{{profile_id}}", str(best_video_profile['id'])) \
                                                    .replace("{{segment_timestamp}}", str(i * 4)) \
                                                    .replace("{{file_type}}", best_video_profile['file_type'])
                audio_segment_url = segment_template.replace("{{profile_id}}", str(specific_audio_profile['id'])) \
                                                    .replace("{{segment_timestamp}}", str(i * 4)) \
                                                    .replace("{{file_type}}", specific_audio_profile['file_type'])
                video_filename = f"segment_{i * 4:04d}.ts"  # 使用4秒的倍數作為文件名
                audio_filename = f"segment_{i * 4:04d}.ts"  # 使用4秒的倍數作為文件名
                logging.info(
                    f"Generated video segment URL: {video_segment_url}")
                logging.info(
                    f"Generated audio segment URL: {audio_segment_url}")
                logging.info(
                    f"Downloading {video_filename} ({i+1}/{segment_count})")
                logging.info(
                    f"Downloading {audio_filename} ({i+1}/{segment_count})")
                tasks.append(download_file(
                    session, video_segment_url, video_filename, output_video_dir))
                tasks.append(download_file(
                    session, audio_segment_url, audio_filename, output_audio_dir))
            await asyncio.gather(*tasks)

    # 設定同時進行的請求數量
    batch_size = 40  # 根據需求調整批次大小

    async def main():
        for i in range(0, segment_count, batch_size):
            await download_segments(i, min(i + batch_size, segment_count))

    asyncio.run(main())

# 9. 檢查分片數量是否正確
downloaded_video_segments = len([name for name in os.listdir(
    output_video_dir) if name.startswith("segment_") and name.endswith(".ts")])
downloaded_audio_segments = len([name for name in os.listdir(
    output_audio_dir) if name.startswith("segment_") and name.endswith(".ts")])
logging.info(
    f"\nExpected video segments: {segment_count}, Downloaded video segments: {downloaded_video_segments}")
logging.info(
    f"\nExpected audio segments: {segment_count}, Downloaded audio segments: {downloaded_audio_segments}")

if downloaded_video_segments == segment_count and downloaded_audio_segments == segment_count:
    logging.info("\nDownload completed. Merging segments using FFmpeg...")

    # 10. 使用FFmpeg進行混流，確保路徑正確
    with open("video_file_list.txt", "w") as video_file, open("audio_file_list.txt", "w") as audio_file:
        for i in range(segment_count):
            video_file.write(
                f"file '{output_video_dir}/segment_{i * 4:04d}.ts'\n")
            audio_file.write(
                f"file '{output_audio_dir}/segment_{i * 4:04d}.ts'\n")

    os.system(f"ffmpeg -f concat -safe 0 -i video_file_list.txt -c copy video.ts")
    os.system(f"ffmpeg -f concat -safe 0 -i audio_file_list.txt -c copy audio.ts")
    os.system(
        "ffmpeg -i video.ts -i audio.ts -c copy spotify_video/spotify_video.mp4")

    os.remove("video_file_list.txt")
    os.remove("audio_file_list.txt")
    os.remove("video.ts")
    os.remove("audio.ts")

    logging.info(
        "\nMerged video and audio saved as 'spotify_video.mp4' in the parent directory.")
else:
    logging.error("\nDownload incomplete. Some segments are missing.")
