import yt_dlp
import pandas as pd
import langid
from tqdm import tqdm
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# Custom logger to silence ffmpeg warnings
class QuietLogger:
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): print(msg)

channel_url = "https://www.youtube.com/@MrBeast/videos"
print("Starting search for videos...")

# Get video IDs (no view filter)
flat_opts = {
    'extract_flat': True,
    'quiet': True,
    'skip_download': True,
    'no_warnings': True,
    'logger': QuietLogger()
}

with yt_dlp.YoutubeDL(flat_opts) as ydl:
    flat_info = ydl.extract_info(channel_url, download=False)
    flat_videos = flat_info.get('entries', [])
    video_ids = [video['id'] for video in flat_videos if 'id' in video]

print(f"Found {len(video_ids)} videos.\n")

# ---- Worker function ----
def process_video(vid_id):
    full_opts = {
        'extract_flat': False,
        'quiet': True,
        'skip_download': True,
        'no_warnings': True,
        'logger': QuietLogger()
    }
    try:
        with yt_dlp.YoutubeDL(full_opts) as ydl:
            video_url = f"https://www.youtube.com/watch?v={vid_id}"
            info = ydl.extract_info(video_url, download=False)
            year = info.get('upload_date', '')[:4] if info.get('upload_date') else "Unknown"
            comment_count = info.get('comment_count') or 0
            has_comments = comment_count > 0

            # Simulated sleep per video
            time.sleep(random.uniform(1.5, 3.5))

            return {
                'video_id': vid_id,
                'video_title': info.get('title'),
                'views': info.get('view_count'),
                'year': year,
                'link': video_url,
                'comments': has_comments,
                'comment_count': comment_count
            }
    except Exception as e:
        print(f"Error processing {video_url}: {e}")
        return None

# ---- Parallelization ----
final_videos = []
max_workers = 10  # You can try 5 or 10 depending on your machine

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(process_video, vid_id) for vid_id in video_ids]
    for future in tqdm(as_completed(futures), total=len(video_ids), desc="Processing commercials"):
        result = future.result()
        if result:
            final_videos.append(result)

# ---- Save results ----
df = pd.DataFrame(final_videos)
df.sort_values(by=['year', 'views', 'comment_count'], ascending=[True, False, False], inplace=True)

# Language detection
df['language_detected'] = df['video_title'].astype(str).apply(lambda x: langid.classify(x)[0])

# Save CSV
output_path = "data/raw/videos_mrbeast.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)
if os.path.isfile(output_path):
    os.remove(output_path)
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"\nFull Videos download completed.")


# ---- Summary ----
print(
    "\n"
    + "=" * 60 + "\n"
    "SUMMARY REPORT\n"
    + "=" * 60 + "\n"
    f"Output file           : videos_mrbeast.csv\n"
    f"Output directory      : {output_path}\n"
    f"Total videos processed: {len(df):,}\n"
    f"Languages detected    : {df['language_detected'].nunique()}\n"
    f"Total comments        : {df['comment_count'].sum():,}\n"
    f"Year range            : {df['year'].min()} – {df['year'].max()}\n"
    + "=" * 60
)
