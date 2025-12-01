# Audio Pipeline

## Project Overview

This pipeline automates the extraction, processing, and transcription of audio streams. It downloads raw audio from YouTube, splits it into manageable chunks using FFmpeg, and transcribes it using language-specific Nova models.

## Prerequisites

### System Requirements (macOS)

Ensure the following system tools are installed via Homebrew:

- **yt-dlp**: For downloading raw audio streams.
  `brew install yt-dlp`
- **ffmpeg**: For audio splitting and processing.
  `brew install ffmpeg`

### Python Dependencies

Install the required Python packages:
`pip install -r requirements.txt`

## Usage

Run the pipeline using the following command structure:

```bash
python3 pipeline_script.py <yaml_file_path> --lang <language>
```
