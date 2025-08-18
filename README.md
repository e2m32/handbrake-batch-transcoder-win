# Video Transcoding Script

A powerful, multi-threaded video transcoding tool built with Python and HandBrakeCLI. This script automatically processes video files in a directory structure, transcoding them to efficient formats while providing real-time progress tracking and intelligent pause/resume functionality.

## Quick start

```bash
# Basic
python transcode_videos.py /path/to/video/directory

# Choose worker count
python transcode_videos.py /path/to/video/directory 8

# Reduce console noise or enable debug
python transcode_videos.py /path/to/video/directory --quiet
python transcode_videos.py /path/to/video/directory --verbose

# Windows path example
python transcode_videos.py "C:\\Videos\\Movies" 4 --quiet
```

## Features

### 🎥 Smart Video Processing
- **Automatic codec detection** - Skips already efficient codecs (H.265, HEVC, AV1)
- **Bitrate analysis** - Avoids transcoding files that would likely become larger
- **Resolution filtering** - Configurable minimum resolution thresholds
- **File size validation** - Prevents transcoding when output would be larger than input

### 🚀 Multi-threaded Performance
- **Configurable worker threads** - Process multiple videos simultaneously
- **Real-time progress bars** - Live HandBrake progress parsing for accurate completion estimates
- **Ordered worker display** - Workers always shown in numerical order (Worker_1, Worker_2, etc.)
- **Thread-safe logging** - Comprehensive CSV logging of all operations

### ⏸️ Interactive Control
- **Keyboard interrupt support** - Ctrl+C to pause/resume workers
- **Process suspension** - Actual HandBrake process suspension on Windows for true pause functionality
- **Graceful shutdown options** - Choose immediate shutdown or wait for current jobs to complete
- **Resume from interruption** - Automatically skips already processed files

### 📊 Comprehensive Logging
- **CSV progress tracking** - Detailed logs of all processed files
- **Status categorization** - Success, failed, skipped (various reasons)
- **Size comparison** - Before/after file sizes and compression ratios
- **Intelligent skip detection** - Separate tracking for different skip reasons

### 🔧 Cross-platform Support
- **Windows process control** - Full process suspension/resume support
- **Unix fallback** - Graceful degradation on non-Windows systems
- **Path handling** - Robust file path management across platforms

## Prerequisites

### Required Software
- **Python 3.6+** - Core runtime
- **HandBrakeCLI** - Video transcoding engine
  - Download from: https://handbrake.fr/downloads2.php
  - Must be accessible via `HandBrakeCLI` command in PATH
- **FFprobe** (part of FFmpeg) - Video analysis
  - Download from: https://ffmpeg.org/download.html
  - Must be accessible via `ffprobe` command in PATH

### Python Dependencies
All dependencies are part of Python standard library:
- `threading` - Multi-threading support
- `subprocess` - External process management
- `csv` - Logging functionality
- `signal` - Keyboard interrupt handling
- `os`, `sys`, `shutil` - File system operations
- `time`, `json` - Utilities

### Windows-specific (Optional)
- **ctypes** - For process suspension/resume functionality
- Requires Windows kernel32.dll access

## Installation

1. **Clone or download** this repository
2. **Install HandBrakeCLI** and ensure it's in your system PATH
3. **Install FFmpeg** and ensure `ffprobe` is in your system PATH
4. **Configure the script** by editing the configuration section

## Configuration

Edit the configuration variables at the top of `transcode_videos.py`:

```python
# === Configuration ===
MAX_WORKERS = 4               # Number of concurrent transcoding workers
CREATE_BACKUPS = False        # Create backup copies of original files
BACKUP_SUBDIR = "backups"     # Backup directory name
SHOW_PROGRESS = True          # Enable real-time progress bars

# HandBrake settings
PRESET = "Fast 1080p30 Subs"           # HandBrake encoding preset (by name)
PRESET_JSON = "fast1080p30subs.json"   # Preset file path (imported at runtime)
```

## Usage

### Basic Usage
```bash
python transcode_videos.py /path/to/video/directory
```

### With Custom Worker Count
```bash
python transcode_videos.py /path/to/video/directory 4
```

### With Flags (quiet/verbose)
```bash
python transcode_videos.py /path/to/video/directory --quiet
python transcode_videos.py /path/to/video/directory 8 --verbose
```

### Interactive Controls

During execution, use **Ctrl+C** to access the pause menu:

```
⏸ WORKERS PAUSED - Choose an option:
(R) Resume workers
(Q) Quit immediately - stop all workers now
(S) Graceful shutdown - finish current jobs then stop
Choice: 
```

- **R** - Resume all workers and continue processing
- **Q** - Immediately terminate all workers and exit
- **S** - Allow current jobs to finish, then gracefully exit

#### UI options
- Console clear around menu: toggle via `MENU_CLEAR_CONSOLE` (default: true)
- Pre-menu settle delay: `MENU_SETTLE_MS` (default: 250 ms)
- Quiet mode: add `--quiet` or `-q` to reduce console noise
- Verbose mode: add `--verbose` or `-v` for extra debug output

## Output

### Progress Display
```
[Worker_1 ] video1.mp4                        [████████████░░░░░░░░░░░░░]  52.3% Transcoding
[Worker_2 ] video2.avi                        [██████░░░░░░░░░░░░░░░░░░░]  24.7% Transcoding
[Worker_3 ] video3.mkv                        [███████████████░░░░░░░░░]  65.1% Transcoding
```

### Processing Summary
```
Processing complete!
Successful: 145
Skipped (low resolution): 23
Skipped (likely larger): 8
Skipped (larger after transcode): 5
Failed: 2
Total: 183
```

### CSV Log Format
The `transcode_log.csv` file contains detailed processing information:

| Column | Description |
|--------|-------------|
| filepath | Full path to the processed file |
| status | Processing result (success, failed, skipped_*) |
| timestamp | ISO timestamp of completion |
| before_size_mb | Original file size in MB |
| after_size_mb | Transcoded file size in MB |
| compression_ratio | Size ratio (after/before) |

## Skip Conditions

The script intelligently skips files in several scenarios:

### Resolution-based Skips
- `skipped_low_res_WIDTHxHEIGHT` - Video resolution below configured minimums

### Codec-based Skips  
- `skipped_likely_larger_low_bitrate_X_Mbps_for_WIDTHxHEIGHT` - Already efficient encoding
- Files already using H.265, HEVC, or AV1 codecs

### Size-based Skips
- `skipped_larger_size_X.XXX` - Transcoded file would be larger than original

## Troubleshooting

### Common Issues

**"HandBrakeCLI not found"**
- Ensure HandBrakeCLI is installed and in your system PATH
- Test with: `HandBrakeCLI --version`

**"ffprobe not found"**
- Ensure FFmpeg is installed and ffprobe is in your system PATH
- Test with: `ffprobe -version`

**Workers get stuck after pause/resume**
- Check for HandBrake process suspension issues
- Review debug output for specific worker problems
- Consider restarting the script if workers remain unresponsive

**High memory usage**
- Reduce `MAX_WORKERS` if experiencing memory pressure
- HandBrake processes can be memory-intensive for large files

### Debug Information

For troubleshooting, disable progress bars to see detailed output:
```python
SHOW_PROGRESS = False
```

This reveals:
- Detailed HandBrake command execution
- Process suspension/resume messages
- Worker state transitions
- Error details and return codes

## Version History

### v0.5.1 (Current)
- Adds optional console clear/redraw around the pause menu for a crisper UI
- Makes the pre-menu settle delay configurable via `MENU_SETTLE_MS`
- Adds `--quiet`/`--verbose` flags to control console noise

### v0.5.0
- Reliable Ctrl+C pause menu on Windows via console control handler
- HandBrakeCLI launched in a new process group so Ctrl+C doesn't kill it
- Optional Windows process suspension during pause for true pauses
- Interrupted jobs are logged as "interrupted" (they retry), not "failed"

### v0.4.0
- **Fixed** cancelled jobs being logged as 'failed' in CSV
- **Improved** worker display ordering (always numerical)
- **Enhanced** pause/resume reliability with better state validation
- **Added** comprehensive debugging and error handling

### v0.3.1
- **Added** graceful shutdown vs immediate shutdown options
- **Improved** worker thread naming (Worker_1, Worker_2, etc.)
- **Fixed** progress bar alignment issues
- **Enhanced** signal handling for better Ctrl+C behavior

### v0.2.x
- **Added** real-time HandBrake progress parsing
- **Implemented** cross-platform compatibility
- **Added** keyboard interrupt pause/resume functionality

### v0.1.x
- Initial release with basic transcoding functionality
- Multi-threaded processing
- CSV logging

## Contributing

Feel free to submit issues, feature requests, or pull requests. When contributing:

1. **Test thoroughly** with various video formats and scenarios
2. **Maintain cross-platform compatibility** 
3. **Update documentation** for any new features
4. **Follow existing code style** and commenting patterns

## License

This project is provided as-is for personal and educational use. Please ensure you have appropriate rights to transcode any video files you process.

## Acknowledgments

- **HandBrake team** - For the excellent HandBrakeCLI transcoding engine
- **FFmpeg team** - For the powerful ffprobe video analysis tool
- **Python community** - For the robust standard library that makes this possible
