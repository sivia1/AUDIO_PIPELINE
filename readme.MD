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

## 🔧 Key Technical Challenges & Solutions

### 1. Handling Audio Variability & Normalization
**The Challenge:** Raw audio inputs came in various formats (mp3, wav, flac) with inconsistent sample rates and volume levels. Direct ingestion led to model convergence issues due to data variance.
**The Solution:**
- Implemented a preprocessing layer using **FFmpeg** to standardize all inputs to 16kHz mono WAV files (industry standard for speech models).
- Added an RMS-based volume normalization step to ensure consistent audio energy levels across the dataset.

### 2. Intelligent Segmentation (Chunking)
**The Challenge:** Long audio files cannot be fed directly into most transformer models due to token limits. Randomly slicing audio cuts off words and destroys semantic context.
**The Solution:**
- Developed a silence-based chunking algorithm using `librosa`.
- The system analyzes decibel thresholds to detect natural pauses in speech and slices the audio there, preserving sentence integrity for better training outcomes.
