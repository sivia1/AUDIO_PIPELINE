import os
import uuid
import subprocess
import glob
import json
import time
import asyncio
import yaml
import sys
from deepgram import DeepgramClient
from dotenv import load_dotenv

# -- Configuration and Paths --
CHUNKS_BASE_DIR = 'data/audio_chunks'
OUTPUT_BASE_DIR = 'data/metadata'
SEGMENT_SECONDS = 10
OVERLAP_SECONDS = 3
CHOP_SECONDS = SEGMENT_SECONDS - OVERLAP_SECONDS
SAMPLE_RATE_HZ = 16000 
CONCURRENT_DELAY_SECONDS = 3  # Delay between launching each download task

load_dotenv()

# --- Core Functions ---

def get_video_id(url):
    """
    Extracts the YouTube video unique ID 
    """
    try:
        from urllib.parse import urlparse, parse_qs
        parsed_url = urlparse(url)
        # for standard URLs like youtube.com/watch?v=XXXXX
        if parsed_url.hostname in ['www.youtube.com', 'youtube.com']:
            qs = parse_qs(parsed_url.query)
            if 'v' in qs:
                return qs['v'][0]
        # for short URL (e.g., youtu.be/ID)
        elif parsed_url.netloc in ('youtu.be', 'www.youtu.be'):
            video_id = parsed_url.path.lstrip('/')
            video_id = video_id.split('?')[0]
            return video_id
        # Fallback 
        else:
            path_parts = parsed_url.path.strip('/').split('/')
            return path_parts[-1] if path_parts and path_parts[-1] else f"video_{int(time.time())}"
    except:
        return f"video_{int(time.time())}"
    
def load_urls_from_yaml(filepath):
    """
    Loads a list of YouTube URLs from a YAML file.
    """
    print(f"Reading URLs from {filepath}...")
    with open(filepath, 'r') as f:
        data = yaml.safe_load(f)
        if isinstance(data, dict):
            urls = data.get('urls') or data.get('youtube_links') or data.get('links')
            if isinstance(urls, list):
                return [url for url in urls if isinstance(url, str)]
            raise ValueError("YAML file structure invalid. Expected list under key 'urls' or similar.")
        elif isinstance(data, list):
            return [url for url in data if isinstance(url, str)]
        else:
            raise ValueError("YAML content is neither a dictionary nor a list.")

async def download_audio(video_id, youtube_url, output_path):
    """
    Downloads audio using yt-dlp with a cookies.txt file to avoid IP blocking and converts it to a single WAV file at 16kHz.
    """
    print(f"[{video_id}] Downloading Raw Audio from {youtube_url}....")

    # Check if cookies file exists
    cookies_path = 'cookies.txt'
    has_cookies = os.path.exists(cookies_path)
    
    if not has_cookies:
        print(f"⚠️ Warning: '{cookies_path}' not found. YouTube might block this request.")

    def sync_download():
        command = [
            'yt-dlp',
            '--cookies-from-browser', 'chrome',  # or 'firefox', 'edge', 'brave'
            '-x',
            '--audio-format', 'wav',
            '--postprocessor-args', f'ffmpeg:-ar {SAMPLE_RATE_HZ}',
            '-o', output_path,
            youtube_url
        ]

        # Only add the cookies flag if the file exists
        if has_cookies:
            command.insert(1, '--cookies')
            command.insert(2, cookies_path)
            
        # Add a user agent to look like a real browser
        command.extend(['--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'])

        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result
    
    try:
        await asyncio.to_thread(sync_download)
        print(f"[{video_id}] Raw audio downloaded to: {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"[{video_id}] ❌ Download failed!")
        print(f"STDERR: {e.stderr[-500:]}")
        raise
    
# async def download_audio(video_id, youtube_url, output_path):
#     """
#     Downloads audio and converts it to a single WAV file at 16kHz.
#     """
#     print(f"[{video_id}] Downloading Raw Audio from {youtube_url}....")
#     def sync_download():
#         command = [
#             'yt-dlp', 
#             '-x', 
#             '--audio-format', 'wav', 
#             '--postprocessor-args', f"-ar {SAMPLE_RATE_HZ}",
#             #'--ppa', 'FixupMuteFragments',
#             '-o', output_path,
#             youtube_url
#         ]
#         return subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     await asyncio.to_thread(sync_download)
#     print(f"[{video_id}] Raw audio downloaded to: {output_path}")

#NEW Overlap function for splitting audio

async def get_duration_async(input_audio_path):
    """Uses ffprobe to asynchronously determine the audio file's duration."""
    # ffprobe is a blocking process, so we run it in a thread.
    def sync_get_duration():
        cmd = [
            'ffprobe', '-v', 'error', '-show_entries', 
            'format=duration', '-of', 
            'default=noprint_wrappers=1:nokey=1', input_audio_path
        ]
        # Run subprocess without 'shell=True' for better safety
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        return float(result.stdout.strip())

    return await asyncio.to_thread(sync_get_duration)


async def extract_chunk_async(video_id, input_audio_path, start_time, segment_seconds, chunk_index):
    """Asynchronously extracts a single chunk using ffmpeg -ss and -t."""
    
    chunks_dir = os.path.join(CHUNKS_BASE_DIR, video_id)
    output_path = os.path.join(chunks_dir, f'chunk_{chunk_index:03d}.wav')

    def sync_extract():
        # -ss: seek to the start time
        # -t: sets the duration of the output file
        cmd = [
            'ffmpeg', '-ss', str(start_time), '-i', input_audio_path,
            '-t', str(segment_seconds), '-c', 'copy', '-y', output_path
        ]
        return subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    await asyncio.to_thread(sync_extract)
    # print(f"[{video_id}] Extracted chunk {chunk_index:03d}") # Optional detailed logging


async def split_audio(input_audio_path, video_id, segment_seconds, overlap_seconds):
    """
    Splits the WAV into 10-second chunks using ffmpeg.
    """
    print(f"[{video_id}] Splitting Audio Chunks (Overlap: {overlap_seconds}s)...")

    # Create the required folder structure
    chunks_dir = os.path.join(CHUNKS_BASE_DIR, video_id)
    os.makedirs(chunks_dir, exist_ok=True)
    duration = await get_duration_async(input_audio_path)


    # Calculate the interval
    chop_seconds = segment_seconds - overlap_seconds
    start_times = []
    
    # 2. Calculate all segment start times
    t = 0.0
    while t < duration:
        start_times.append(t)
        t += chop_seconds
        
    print(f"[{video_id}] Calculated {len(start_times)} overlapping segments over {duration:.2f}s.")

    # 3. Create and run all extraction tasks concurrently
    extraction_tasks = []
    for i, start in enumerate(start_times):
        # Create a task for each chunk
        task = extract_chunk_async(video_id, input_audio_path, start, segment_seconds, i)
        extraction_tasks.append(task)
        
    # Run all chunk extractions concurrently (parallel ffmpeg processes!)
    await asyncio.gather(*extraction_tasks)
    
    print(f"[{video_id}] Audio split into {len(start_times)} chunks in: {chunks_dir}")
    
    return chunks_dir

# Transcribe chunks concurrently
async def transcribe_chunk_sync(chunk_path, client, options):
    """Transcribes a single chunk synchronously in a thread."""
    # Define the blocking action to run in the thread
    def blocking_transcribe():
        with open(chunk_path, 'rb') as audio_file:
            return client.listen.v1.media.transcribe_file(
                request=audio_file.read(),
                **options
            )
            
    # Run the blocking action in a separate thread
    response = await asyncio.to_thread(blocking_transcribe)
    
    transcript_text = response.results.channels[0].alternatives[0].transcript
    chunk_name = os.path.basename(chunk_path)
    # Corrected chunk_id parsing to be safe
    try:
        chunk_id = chunk_name.split('_')[-1].replace('.wav', '')
    except IndexError:
        chunk_id = "000"
        
    return {'chunk_id': chunk_id, 'text': transcript_text}


async def transcribe_chunks(video_id, chunks_dir, deepgram_api_key):
    """Transcribes all chunks for a video concurrently."""
    print(f"[{video_id}] Transcribing audio chunks...")

    client = DeepgramClient(api_key=deepgram_api_key)
    chunk_files = sorted(glob.glob(os.path.join(chunks_dir, '*.wav')))
    
    options = {
        "model": "nova-3",
        "language": "en-IN",
        "punctuate": True
    }
    
    # Create a list of async tasks for all chunks
    transcription_tasks = [
        transcribe_chunk_sync(chunk_path, client, options)
        for chunk_path in chunk_files
    ]

    # Run all chunk transcription tasks concurrently
    transcriptions = await asyncio.gather(*transcription_tasks)

    print(f"[{video_id}] Transcription complete.")
    return transcriptions


def create_metadata_jsonl(video_id, transcriptions, base_output_dir, segment_seconds):
    """
    Creates the final JSONL metadata file for a single video.
    """

    output_jsonl = os.path.join(base_output_dir, f'metadata_{video_id}.jsonl')
    print(f"[{video_id}] Creating Metadata to {output_jsonl}....")
    # audio_path relative to data/audio_chunks/ folder
    relative_audio_base = os.path.join(CHUNKS_BASE_DIR, video_id) 

    with open(output_jsonl, 'w') as out_file:
        for item in transcriptions:
            chunk_id = item['chunk_id']
            i = int(chunk_id) 
            
            start_time = i * segment_seconds
            end_time = start_time + segment_seconds
            
            audio_path_relative = os.path.join(relative_audio_base, f'chunk_{chunk_id}.wav') 
            
            row = {
                'video_id': video_id,
                'chunk_id': chunk_id,
                'start': start_time,
                'end': end_time,
                'audio_path': audio_path_relative,
                'text': item['text']
            }
            # one JSON object per line
            out_file.write(json.dumps(row) + '\n')

    print(f"[{video_id}] Metadata saved")

# -- Concurrency Logic --
async def process_single_url_async(video_url, deepgram_key):
    video_id = get_video_id(video_url)
    print(f"\n--- Starting processing for video: {video_id} ({video_url}) ---")

    data_dir = os.path.dirname(CHUNKS_BASE_DIR)
    raw_audio_path = os.path.join(data_dir, f'raw_audio_temp_{video_id}.wav')

    try:
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

        await download_audio(video_id, video_url, raw_audio_path)

        chunks_dir = await split_audio(raw_audio_path, video_id, SEGMENT_SECONDS, OVERLAP_SECONDS)

        # Clean up the raw temp file
        os.remove(raw_audio_path)

        transcriptions = await transcribe_chunks(video_id, chunks_dir, deepgram_key)

        create_metadata_jsonl(video_id, transcriptions, OUTPUT_BASE_DIR, SEGMENT_SECONDS)

        print(f"\n--- Finished processing video: {video_id} ---")
        return f"SUCCESS: {video_id} ({video_url})"

    except Exception as e:
        error_msg = f"ERROR processing video {video_id} ({video_url}: {type(e).__name__}: {e}"
        print(f" {error_msg}", file=sys.stderr)
        raise Exception(error_msg)

    finally:
        if os.path.exists(raw_audio_path):
            os.remove(raw_audio_path)
            print(f"Cleaned up temp file for {video_id}")

# -- Orchestration --
async def main_async():
    """Main execution flow for the pipeline."""

    load_dotenv()  # Load environment variables from .env file

    print("\n--- Pipeline Execution Initiated---\n")
    
    
    # Get Deepgram key from .env
    deepgram_key = os.environ.get("DEEPGRAM_API_KEY")

        
    if not deepgram_key:
        print("Error: DEEPGRAM_API_KEY environment variable not set.")
        sys.exit(1)

    if len(sys.argv) < 2:
        print("Usage: python pipeline_script.py <YouTube_URL_or_YAML_file>")
        sys.exit(1)
    # --- ---
    input_arg = sys.argv[1]
    urls = []

    # Check if input_arg is a file (YAML) or URL
    if os.path.isfile(input_arg):
        try:
            urls = load_urls_from_yaml(input_arg)
            if not urls:
                print("No URLs found in YAML file.")
                sys.exit(1)
        except Exception as e:
            print(f"Error loading YAML file: {e}")
            sys.exit(1)
    else:
        urls = [input_arg]

    tasks = []
    print(f"\nScheduling {len(urls)} video tasks with a {CONCURRENT_DELAY_SECONDS}s launch delay between each...")
    for i, url in enumerate(urls):
        tasks.append(process_single_url_async(url, deepgram_key))
        await asyncio.sleep(CONCURRENT_DELAY_SECONDS)  # Rate limit between launches

    print(f"\n--- Launching {len(tasks)} Concurrent Video Processing Tasks ---\n")

    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful_count = 0
    failure_count = 0

    print("\nBatch Processing Results:")
    for res in results:
        if isinstance(res, str) and res.startswith("SUCCESS"):
            print(res)
            successful_count += 1
        # The task failed, and the exception object was returned by asyncio.gather
        elif isinstance(res, Exception):
            print(f" FAILED: {res}", file=sys.stderr)
            failure_count += 1

    print(f"\nPipeline execution complete. Successes: {successful_count}, Failures: {failure_count}.")

if __name__ == '__main__':
    # Ensure the base output directories are ready
    os.makedirs(CHUNKS_BASE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    try:
        subprocess.run(['yt-dlp', '--version'], check=True, stdout=subprocess.DEVNULL)
        subprocess.run(['ffmpeg', '-version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as e:
        print(f"ERROR: Missing external dependency: {e.filename} not found. Please install yt-dlp and ffmpeg.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: External dependency check failed. {e}", file=sys.stderr)
        sys.exit(1)

    asyncio.run(main_async())