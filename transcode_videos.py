import os
import subprocess
import shutil
import tempfile
from datetime import datetime
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import time
import sys

# === Version ===
VERSION = "0.3.1"

# === Config ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PRESET = "Fast 1080p30 Subs"
PRESET_JSON = os.path.join(SCRIPT_DIR, "fast1080p30subs.json")
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv"}
LOG_FILE = os.path.join(SCRIPT_DIR, "transcode_log.csv")
MAX_WORKERS = 4  # Number of concurrent transcode operations
CREATE_BACKUPS = False  # Whether to create backups of original files
BACKUP_SUBDIR = "backups"  # Subdirectory name for backups (relative to processed directory)
VERBOSE_HANDBRAKE = False  # Whether to use verbose output in HandBrake
SHOW_PROGRESS = True  # Whether to show progress bars for each worker

# Thread lock for log file access
log_lock = threading.Lock()

# Global progress tracking
progress_data = {}
progress_lock = threading.Lock()

def update_progress(thread_id, filename, progress, status="Processing"):
    """Update progress for a specific thread"""
    if not SHOW_PROGRESS:
        return
    
    with progress_lock:
        progress_data[thread_id] = {
            'filename': os.path.basename(filename),
            'progress': progress,
            'status': status
        }
        display_progress()

def display_progress():
    """Display progress bars for all active threads"""
    if not SHOW_PROGRESS or not progress_data:
        return
    
    # Clear previous lines
    sys.stdout.write('\033[2K\r')  # Clear current line
    for _ in range(len(progress_data)):
        sys.stdout.write('\033[A\033[2K')  # Move up and clear line
    
    # Display progress for each thread
    for thread_id, data in progress_data.items():
        filename = data['filename'][:40] + "..." if len(data['filename']) > 40 else data['filename']
        progress = data['progress']
        status = data['status']
        
        # Create progress bar
        bar_length = 30
        filled_length = int(bar_length * progress / 100)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        print(f"[{thread_id}] {filename:<45} [{bar}] {progress:3.0f}% {status}")
    
    sys.stdout.flush()

def clear_progress(thread_id):
    """Remove progress tracking for a completed thread"""
    if not SHOW_PROGRESS:
        return
    
    with progress_lock:
        if thread_id in progress_data:
            del progress_data[thread_id]

# === Load already processed files ===
def load_processed_files():
    processed = {}
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed[row["filepath"]] = row["status"]
    return processed

# === Append result to log ===
def log_result(filepath, status, before_size=None, after_size=None):
    with log_lock:  # Thread-safe logging
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["filepath", "status", "timestamp", "before_size_mb", "after_size_mb", "compression_ratio"])
            
            # Calculate compression ratio if both sizes are available
            compression_ratio = ""
            if before_size is not None and after_size is not None and before_size > 0:
                compression_ratio = f"{(after_size / before_size):.3f}"
            
            # Convert sizes to MB
            before_mb = f"{before_size / (1024*1024):.2f}" if before_size is not None else ""
            after_mb = f"{after_size / (1024*1024):.2f}" if after_size is not None else ""
            
            writer.writerow([filepath, status, datetime.now().isoformat(), before_mb, after_mb, compression_ratio])

# === Check file type ===
def is_video_file(filename):
    return os.path.splitext(filename)[1].lower() in VIDEO_EXTENSIONS

# === Get video resolution ===
def get_video_resolution(filepath):
    """Get video resolution using ffprobe. Returns (width, height) or None if failed."""
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-select_streams", "v:0",
            filepath
        ]
        
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        if result.returncode != 0:
            return None
            
        import json
        data = json.loads(result.stdout)
        
        if 'streams' in data and len(data['streams']) > 0:
            stream = data['streams'][0]
            width = stream.get('width')
            height = stream.get('height')
            
            if width and height:
                return (width, height)
        
        return None
    except Exception as e:
        print(f"Error getting video resolution for {filepath}: {e}")
        return None

def get_video_info(filepath):
    """Get comprehensive video information using ffprobe. Returns dict with codec, bitrate, etc."""
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_format",
            "-select_streams", "v:0",
            filepath
        ]
        
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        if result.returncode != 0:
            return None
            
        import json
        data = json.loads(result.stdout)
        
        info = {}
        
        # Get video stream info
        if 'streams' in data and len(data['streams']) > 0:
            stream = data['streams'][0]
            info['codec'] = stream.get('codec_name', '').lower()
            info['width'] = stream.get('width')
            info['height'] = stream.get('height')
            info['bit_rate'] = stream.get('bit_rate')
            info['duration'] = stream.get('duration')
            
        # Get format info (for overall bitrate if stream bitrate unavailable)
        if 'format' in data:
            format_info = data['format']
            info['format_bit_rate'] = format_info.get('bit_rate')
            info['format_duration'] = format_info.get('duration')
            info['format_size'] = format_info.get('size')
            
        return info
        
    except Exception as e:
        print(f"Error getting video info for {filepath}: {e}")
        return None

def should_skip_likely_larger(filepath):
    """Check if transcoding will likely result in a larger file based on codec and bitrate analysis"""
    info = get_video_info(filepath)
    
    if not info:
        return False, "unknown video info"
    
    codec = info.get('codec', '')
    width = info.get('width', 0)
    height = info.get('height', 0)
    
    # Get bitrate (try stream bitrate first, then format bitrate)
    bit_rate = info.get('bit_rate')
    if not bit_rate:
        bit_rate = info.get('format_bit_rate')
    
    # Convert bitrate to number if it's a string
    try:
        if bit_rate:
            bit_rate = int(bit_rate)
    except (ValueError, TypeError):
        bit_rate = None
    
    # Skip if already using modern, efficient codecs
    efficient_codecs = ['h265', 'hevc', 'x265', 'av1']
    if any(eff_codec in codec for eff_codec in efficient_codecs):
        return True, f"already efficient codec ({codec})"
    
    # If we have bitrate information, check if it's already low
    if bit_rate and width and height:
        # Calculate pixels per second for bitrate efficiency
        pixels = width * height
        
        # Rough bitrate thresholds (bits per pixel per second)
        # These are conservative estimates for when our preset might create larger files
        if pixels >= 1920 * 1080:  # 1080p or higher
            # For 1080p, if bitrate is already below ~3000 kbps, our preset might make it larger
            if bit_rate < 3000000:  # 3 Mbps in bits per second
                return True, f"low bitrate ({bit_rate/1000000:.1f} Mbps for {width}x{height})"
        elif pixels >= 1280 * 720:  # 720p
            # For 720p, if bitrate is already below ~1500 kbps
            if bit_rate < 1500000:  # 1.5 Mbps
                return True, f"low bitrate ({bit_rate/1000000:.1f} Mbps for {width}x{height})"
    
    # Check for very small files that are likely already compressed
    try:
        file_size = os.path.getsize(filepath)
        duration = info.get('duration') or info.get('format_duration')
        
        if duration and file_size:
            duration = float(duration)
            # If file is smaller than 500MB per hour, it's probably already well compressed
            size_per_hour = file_size / (duration / 3600)  # bytes per hour
            if size_per_hour < 500 * 1024 * 1024:  # 500MB per hour
                return True, f"already compact ({size_per_hour/(1024*1024):.0f} MB/hour)"
                
    except (ValueError, TypeError, ZeroDivisionError):
        pass
    
    return False, f"codec: {codec}, likely worth transcoding"

def should_skip_resolution(filepath):
    """Check if video should be skipped due to resolution being less than 1080p"""
    resolution = get_video_resolution(filepath)
    
    if resolution is None:
        # If we can't determine resolution, don't skip (let HandBrake try)
        return False, "unknown resolution"
    
    width, height = resolution
    
    # Check if resolution is less than 1080p (1920x1080)
    # We'll be conservative and check if either dimension is significantly less than 1080p
    if height < 1080 and width < 1920:
        return True, f"{width}x{height}"
    
    return False, f"{width}x{height}"

# === Transcode one file ===
def transcode_file(filepath, root_backup_dir):
    thread_id = threading.current_thread().name
    dirpath, filename = os.path.split(filepath)
    name, ext = os.path.splitext(filename)
    
    # Initialize progress
    update_progress(thread_id, filename, 0, "Starting")
    
    # Check if video resolution is less than 1080p
    update_progress(thread_id, filename, 2, "Checking resolution")
    should_skip, resolution_info = should_skip_resolution(filepath)
    
    if should_skip:
        clear_progress(thread_id)
        original_size = os.path.getsize(filepath)
        log_result(filepath, f"skipped_low_res_{resolution_info}", original_size)
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] SKIP: Low resolution ({resolution_info}): {filepath}")
        return True  # Return True since this is successful processing (just skipped)
    
    # Check if transcoding will likely result in larger file
    update_progress(thread_id, filename, 4, "Analyzing codec")
    should_skip_codec, codec_info = should_skip_likely_larger(filepath)
    
    if should_skip_codec:
        clear_progress(thread_id)
        original_size = os.path.getsize(filepath)
        log_result(filepath, f"skipped_likely_larger_{codec_info.replace(' ', '_').replace('(', '').replace(')', '')}", original_size)
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] SKIP: Likely to be larger ({codec_info}): {filepath}")
        return True  # Return True since this is successful processing (just skipped)
    
    if not SHOW_PROGRESS:
        print(f"[{thread_id}] Resolution: {resolution_info}, Codec: {codec_info} - proceeding with transcode")
    
    # Use a local temp directory instead of network path for better reliability
    import tempfile
    local_temp_dir = tempfile.gettempdir()
    temp_fd, temp_path = tempfile.mkstemp(suffix=ext, dir=local_temp_dir)
    os.close(temp_fd)

    # Get original file size
    original_size = os.path.getsize(filepath)
    update_progress(thread_id, filename, 5, "Preparing")

    cmd = [
        "HandBrakeCLI",
        "--preset-import-file", PRESET_JSON,
        "--preset", PRESET,
        "-i", filepath,
        "-o", temp_path,
        "--all-subtitles",
        "--markers"
    ]
    
    # Add verbose flag if enabled
    if VERBOSE_HANDBRAKE:
        cmd.append("--verbose=1")

    if not SHOW_PROGRESS:
        print(f"[{thread_id}] Transcoding: {filepath} ({original_size / (1024*1024):.2f} MB)")
        print(f"[{thread_id}] Temp file: {temp_path}")
    
    update_progress(thread_id, filename, 10, "Transcoding")
    
    # Check available disk space
    try:
        import shutil
        total, used, free = shutil.disk_usage(local_temp_dir)
        free_gb = free / (1024**3)
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] Free disk space: {free_gb:.2f} GB")
        
        if free_gb < 10:  # Less than 10GB free
            if not SHOW_PROGRESS:
                print(f"[{thread_id}] WARNING: Low disk space ({free_gb:.2f} GB)")
    except Exception as e:
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] Could not check disk space: {e}")
    
    # Start transcoding with progress simulation
    start_time = time.time()
    result = None
    
    # Run HandBrake in a separate thread to allow progress updates
    def run_handbrake():
        nonlocal result
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    import threading as thread_module
    handbrake_thread = thread_module.Thread(target=run_handbrake)
    handbrake_thread.start()
    
    # Simulate progress while HandBrake is running
    progress = 10
    while handbrake_thread.is_alive():
        elapsed = time.time() - start_time
        
        # Estimate progress based on file size and elapsed time
        # Rough estimate: 1 MB/second processing speed
        estimated_time = original_size / (1024 * 1024)  # seconds
        if estimated_time > 0:
            time_progress = min(80, (elapsed / estimated_time) * 70 + 10)  # 10-80%
            progress = max(progress, time_progress)
        else:
            progress = min(80, progress + 1)
        
        update_progress(thread_id, filename, progress, "Transcoding")
        time.sleep(1)
        
        # Prevent indefinite waiting
        if elapsed > 3600:  # 1 hour timeout
            break
    
    handbrake_thread.join()
    
    update_progress(thread_id, filename, 85, "Finishing")

    if result.returncode != 0:
        clear_progress(thread_id)
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] ERROR: {filepath}")
            print(f"[{thread_id}] Return code: {result.returncode}")
            print(f"[{thread_id}] STDOUT: {result.stdout}")
            print(f"[{thread_id}] STDERR: {result.stderr}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        log_result(filepath, "failed", original_size)
        return False

    # Check if temp file was actually created and has content
    if not os.path.exists(temp_path):
        clear_progress(thread_id)
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] ERROR: Temp file was not created: {temp_path}")
        log_result(filepath, "failed", original_size)
        return False
    
    # Get transcoded file size
    transcoded_size = os.path.getsize(temp_path)
    if transcoded_size == 0:
        clear_progress(thread_id)
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] ERROR: Transcoded file is empty: {temp_path}")
        os.remove(temp_path)
        log_result(filepath, "failed", original_size)
        return False
    
    update_progress(thread_id, filename, 90, "Backing up")
    compression_ratio = transcoded_size / original_size if original_size > 0 else 0

    # Check if transcoded file is larger than original
    if transcoded_size >= original_size:
        clear_progress(thread_id)
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] SKIP: Transcoded file is larger ({transcoded_size / (1024*1024):.2f} MB >= {original_size / (1024*1024):.2f} MB): {filepath}")
        
        # Remove the temp file since we're not using it
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        # Log as skipped due to larger size
        log_result(filepath, f"skipped_larger_size_{compression_ratio:.3f}", original_size, transcoded_size)
        return True  # Return True since this is successful processing (just skipped)

    # Backup original (if enabled)
    if CREATE_BACKUPS:
        try:
            # Create relative path structure in backup directory
            rel_path = os.path.relpath(filepath, os.path.dirname(root_backup_dir))
            backup_file_path = os.path.join(root_backup_dir, rel_path)
            backup_file_dir = os.path.dirname(backup_file_path)
            
            os.makedirs(backup_file_dir, exist_ok=True)
            shutil.copy2(filepath, backup_file_path)
            if not SHOW_PROGRESS:
                print(f"[{thread_id}] Backup created: {backup_file_path}")
        except Exception as e:
            if not SHOW_PROGRESS:
                print(f"[{thread_id}] WARNING: Could not create backup: {e}")

    update_progress(thread_id, filename, 95, "Finalizing")
    
    # Replace original
    try:
        shutil.move(temp_path, filepath)
        log_result(filepath, "success", original_size, transcoded_size)
        
        update_progress(thread_id, filename, 100, "Complete")
        time.sleep(0.5)  # Brief pause to show completion
        clear_progress(thread_id)
        
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] SUCCESS: {filepath} ({original_size / (1024*1024):.2f} MB -> {transcoded_size / (1024*1024):.2f} MB, ratio: {compression_ratio:.3f})")
        
        return True
    except Exception as e:
        clear_progress(thread_id)
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] ERROR: Could not move file from {temp_path} to {filepath}: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        log_result(filepath, "failed", original_size)
        return False

# === Worker function for thread pool ===
def process_file_worker(filepath, root_backup_dir):
    """Worker function that processes a single file and handles exceptions"""
    try:
        return transcode_file(filepath, root_backup_dir)
    except Exception as e:
        print(f"ERROR: Exception during {filepath}: {e}")
        # Get original file size for failed operations
        try:
            original_size = os.path.getsize(filepath)
            log_result(filepath, "failed", original_size)
        except:
            log_result(filepath, "failed")
        return False

# === Main process loop ===
def process_directory(root_dir):
    processed = load_processed_files()
    
    # Create root backup directory if backups are enabled
    root_backup_dir = os.path.join(root_dir, BACKUP_SUBDIR)
    if CREATE_BACKUPS:
        os.makedirs(root_backup_dir, exist_ok=True)
    
    # Collect all video files to process
    files_to_process = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Skip backup directories
        if BACKUP_SUBDIR in dirnames:
            dirnames.remove(BACKUP_SUBDIR)  # This prevents os.walk from descending into backup directories
        
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            if is_video_file(full_path):
                if full_path in processed:
                    status = processed[full_path]
                    if status == "success":
                        print(f"SKIP: Already processed: {full_path}")
                        continue
                    elif status.startswith("skipped_low_res_"):
                        print(f"SKIP: Low resolution already checked: {full_path}")
                        continue
                    elif status.startswith("skipped_larger_size_"):
                        print(f"SKIP: Transcoding would increase file size: {full_path}")
                        continue
                    elif status.startswith("skipped_likely_larger_"):
                        print(f"SKIP: Likely to create larger file: {full_path}")
                        continue
                files_to_process.append(full_path)
    
    if not files_to_process:
        print("No video files to process.")
        return
    
    print(f"Found {len(files_to_process)} video files to process using {MAX_WORKERS} threads.")
    if CREATE_BACKUPS:
        print(f"Backups will be stored in: {root_backup_dir}")
    print(f"Transcode Videos Script v{VERSION}")
    print("=" * 50)
    
    # Process files using ThreadPoolExecutor
    successful = 0
    failed = 0
    skipped_resolution = 0
    skipped_larger = 0
    skipped_codec = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(process_file_worker, filepath, root_backup_dir): filepath 
                         for filepath in files_to_process}
        
        # Process completed tasks
        for future in as_completed(future_to_file):
            filepath = future_to_file[future]
            try:
                success = future.result()
                if success:
                    # Check if it was actually transcoded or skipped
                    processed_updated = load_processed_files()
                    if filepath in processed_updated:
                        status = processed_updated[filepath]
                        if status.startswith("skipped_low_res_"):
                            skipped_resolution += 1
                        elif status.startswith("skipped_larger_size_"):
                            skipped_larger += 1
                        elif status.startswith("skipped_likely_larger_"):
                            skipped_codec += 1
                        else:
                            successful += 1
                    else:
                        successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"ERROR: Unexpected exception for {filepath}: {e}")
                failed += 1
    
    print(f"\nProcessing complete!")
    print(f"Successful: {successful}")
    print(f"Skipped (low resolution): {skipped_resolution}")
    print(f"Skipped (likely larger): {skipped_codec}")
    print(f"Skipped (larger after transcode): {skipped_larger}")
    print(f"Failed: {failed}")
    print(f"Total: {successful + skipped_resolution + skipped_codec + skipped_larger + failed}")

# === Entry point ===
if __name__ == "__main__":
    import sys
    
    # Handle help flag
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print(f"Transcode Videos Script v{VERSION}")
        print("Usage: python transcode_videos.py /path/to/videos [max_workers]")
        print()
        print("This script transcodes video files in a directory and subdirectories using HandBrake.")
        print("It automatically skips files that are already processed or would become larger.")
        print()
        print("Arguments:")
        print("  /path/to/videos    Directory containing video files to transcode")
        print("  max_workers        Number of concurrent operations (default: 4)")
        print()
        print("Features:")
        print("  - Skips videos with resolution less than 1080p")
        print("  - Skips transcoding if output would be larger than original")
        print("  - Resumes processing from where it left off")
        print("  - Multi-threaded processing with progress bars")
        print("  - Detailed logging of all operations")
        print()
        print("Configuration:")
        print(f"  - Preset: {PRESET}")
        print(f"  - Preset JSON: {os.path.basename(PRESET_JSON)}")
        print(f"  - Supported formats: {', '.join(sorted(VIDEO_EXTENSIONS))}")
        print(f"  - Log file: {os.path.basename(LOG_FILE)}")
        print(f"  - Create backups: {CREATE_BACKUPS}")
        print(f"  - Backup subdirectory: {BACKUP_SUBDIR}")
        print(f"  - Verbose HandBrake: {VERBOSE_HANDBRAKE}")
        print(f"  - Show progress: {SHOW_PROGRESS}")
        print()
        print("Examples:")
        print("  python transcode_videos.py /media/videos")
        print("  python transcode_videos.py /media/videos 8")
        print("  python transcode_videos.py \"C:\\Videos\\Movies\" 2")
        sys.exit(0)
    
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(f"Transcode Videos Script v{VERSION}")
        print("Usage: python transcode_videos.py /path/to/videos [max_workers]")
        print("  max_workers: Number of concurrent operations (default: 4)")
        print("Use -h or --help for detailed information.")
        sys.exit(1)

    input_dir = sys.argv[1]
    if not os.path.isdir(input_dir):
        print(f"Invalid directory: {input_dir}")
        sys.exit(1)

    # Optional: Allow user to specify number of workers
    if len(sys.argv) == 3:
        try:
            MAX_WORKERS = int(sys.argv[2])
            if MAX_WORKERS < 1:
                print("max_workers must be at least 1")
                sys.exit(1)
        except ValueError:
            print("max_workers must be a valid integer")
            sys.exit(1)

    process_directory(input_dir)
