yt-dlp --> Downloads the raw audio stream from the YouTube URL.
ffmpeg --> Splits the large downloaded audio file into small, 10-second chunks for transcription.

macOS installation -->

yt-dlp = brew install yt-dlp
ffmpeg = brew install ffmpeg

Run as "python3 pipeline_script.py yaml_file_path"

run command with --lang hindi (which sets lang_code to 'hi'), it automatically uses nova-2 for hindi_urls.

run command with --lang english (which sets lang_code to 'en-IN'), it uses nova-3 for english_urls
