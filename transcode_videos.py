import os
import subprocess
import shutil
import tempfile
from datetime import datetime
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import queue
import time
import sys
import signal
import ctypes
import platform

# Platform-specific imports
IS_WINDOWS = platform.system() == "Windows"
if IS_WINDOWS:
    try:
        import ctypes
        from ctypes import wintypes
        WINDOWS_PROCESS_CONTROL = True
        import msvcrt  # For Windows keyboard input
    except ImportError:
        WINDOWS_PROCESS_CONTROL = False
        print("Warning: ctypes not available - process suspension disabled")
else:
    WINDOWS_PROCESS_CONTROL = False
    try:
        import select
        import tty
        import termios
        UNIX_KEYBOARD_CONTROL = True
    except ImportError:
        UNIX_KEYBOARD_CONTROL = False

# Global state tracking
# (removed keyboard monitoring variables as they don't work with terminal Ctrl+C)

# === Version ===
VERSION = "0.6.0"

# === Config ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PRESET = "Fast 1080p30 Subs"
PRESET_JSON = os.path.join(SCRIPT_DIR, "fast1080p30subs.json")
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv"}
LOG_FILE = os.path.join(SCRIPT_DIR, "transcode_log.csv")
# Separate log for failures (to keep main log clean of persistent failed rows)
FAILED_LOG_FILE = os.path.join(SCRIPT_DIR, "transcode_failed_log.csv")
MAX_WORKERS = 4  # Number of concurrent transcode operations
CREATE_BACKUPS = False  # Whether to create backups of original files
BACKUP_SUBDIR = "backups"  # Subdirectory name for backups (relative to processed directory)
VERBOSE_HANDBRAKE = False  # Whether to use verbose output in HandBrake
SHOW_PROGRESS = True  # Whether to show progress bars for each worker
VERBOSE = False  # Extra debug output
QUIET = False    # Minimal console noise (still prints final summary)

# Network recovery configuration
NETWORK_CHECK_INTERVAL = 10        # seconds between network availability checks
NETWORK_MAX_WAIT = 5 * 60 * 60     # max seconds to wait (5 hours) before giving up on a file
NETWORK_RETRY_ENABLED = True       # whether to wait for network rather than fail immediately

# Finalization (move/replace) retry configuration
FINAL_MOVE_RETRIES = 5             # number of attempts to move temp file to destination
FINAL_MOVE_RETRY_DELAY = 15        # initial delay (seconds) before first retry
FINAL_MOVE_BACKOFF_FACTOR = 2      # exponential backoff multiplier

# Pause menu UI behavior
MENU_CLEAR_CONSOLE = True   # Clear console before showing the pause menu
MENU_SETTLE_MS = 250        # Delay (ms) before showing menu so worker messages settle

# Thread lock for log file access
log_lock = threading.Lock()

# Global progress tracking
progress_data = {}
progress_lock = threading.Lock()

# Global state for pause/resume functionality
worker_paused = threading.Event()
worker_paused.set()  # Start unpaused
shutdown_requested = threading.Event()  # Immediate shutdown
graceful_shutdown_requested = threading.Event()  # Graceful shutdown (finish current jobs)
pause_requested = threading.Event()  # Signal that pause was requested
menu_thread = None
suppress_progress_display = threading.Event()  # When set, progress UI is muted

def _print_worker_event(message):
    """Print a one-line worker event message without permanently breaking the progress layout.
    Messages are informational; in SHOW_PROGRESS mode they may be overwritten by later redraws.
    """
    if QUIET:
        return
    try:
        # Temporarily suppress progress redraw while printing
        saved = suppress_progress_display.is_set()
        suppress_progress_display.set()
        print(message)
        if not saved:
            suppress_progress_display.clear()
    except Exception:
        pass

# Windows Console Control Handler (reliable Ctrl+C on Windows)
console_ctrl_handler_ref = None  # Keep a reference to prevent GC

def register_windows_ctrl_c_handler():
    """On Windows, register a console control handler to catch Ctrl+C and pause workers."""
    if not IS_WINDOWS:
        return
    kernel32 = ctypes.windll.kernel32

    CTRL_C_EVENT = 0
    CTRL_BREAK_EVENT = 1

    # Prototype: BOOL WINAPI HandlerRoutine(DWORD dwCtrlType)
    HANDLER_ROUTINE = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_ulong)

    def handler(ctrl_type):
        # Only act on Ctrl+C or Ctrl+Break
        if ctrl_type in (CTRL_C_EVENT, CTRL_BREAK_EVENT):
            # Trigger pause without letting Python terminate
            if worker_paused.is_set():
                worker_paused.clear()
            pause_requested.set()
            suppress_progress_display.set()
            # Give a moment for worker "PAUSED" messages to flush
            time.sleep(MENU_SETTLE_MS / 1000.0)
            # Start menu in a thread if not already running
            global menu_thread
            if menu_thread is None or not menu_thread.is_alive():
                menu_thread = threading.Thread(target=show_pause_menu, daemon=True)
                menu_thread.start()
            # Return True to indicate the event was handled (prevents default termination)
            return True
        # Not handled here
        return False

    global console_ctrl_handler_ref
    console_ctrl_handler_ref = HANDLER_ROUTINE(handler)
    kernel32.SetConsoleCtrlHandler(console_ctrl_handler_ref, True)

# Windows process control functions
def suspend_process(pid):
    """Suspend a Windows process by PID"""
    if not WINDOWS_PROCESS_CONTROL:
        return False
    
    try:
        kernel32 = ctypes.windll.kernel32
        PROCESS_SUSPEND_RESUME = 0x0800
        handle = kernel32.OpenProcess(PROCESS_SUSPEND_RESUME, False, pid)
        if handle:
            # Use NtSuspendProcess from ntdll
            ntdll = ctypes.windll.ntdll
            result = ntdll.NtSuspendProcess(handle)
            kernel32.CloseHandle(handle)
            return result == 0  # Success if result is 0
        else:
            print(f"Failed to open process {pid} for suspension")
    except Exception as e:
        print(f"Failed to suspend process {pid}: {e}")
    return False

def resume_process(pid):
    """Resume a Windows process by PID"""
    if not WINDOWS_PROCESS_CONTROL:
        return False
    
    try:
        kernel32 = ctypes.windll.kernel32
        PROCESS_SUSPEND_RESUME = 0x0800
        handle = kernel32.OpenProcess(PROCESS_SUSPEND_RESUME, False, pid)
        if handle:
            # Use NtResumeProcess from ntdll
            ntdll = ctypes.windll.ntdll
            result = ntdll.NtResumeProcess(handle)
            kernel32.CloseHandle(handle)
            return result == 0  # Success if result is 0
        else:
            print(f"Failed to open process {pid} for resume")
    except Exception as e:
        print(f"Failed to resume process {pid}: {e}")
    return False

def signal_handler(signum, frame):
    """Handle Ctrl+C interrupt for pause/resume functionality"""
    if VERBOSE and not QUIET:
        print("\n" + "="*70)
        print("🔸 SIGNAL HANDLER CALLED - PAUSE TRIGGERED")
        print(f"🔸 Signal: {signum}, worker_paused.is_set(): {worker_paused.is_set()}")
        print("="*70)
    
    if worker_paused.is_set():
        if VERBOSE and not QUIET:
            print("🔸 PAUSING ALL WORKERS")
            if WINDOWS_PROCESS_CONTROL:
                print("🔸 HandBrake processes will be suspended")
            else:
                print("🔸 Progress display pausing (process suspension not available)")
            print("🔸 Workers will pause at next checkpoint...")
            print("="*70)
        worker_paused.clear()  # Pause workers
        pause_requested.set()  # Signal that pause was requested
        
        # Start menu thread
        menu_thread = threading.Thread(target=show_pause_menu, daemon=True)
        menu_thread.start()
        if VERBOSE and not QUIET:
            print("🔸 Menu thread started")
    else:
        if VERBOSE and not QUIET:
            print("🔸 Workers are already paused. Use the pause menu to control.")
            print("="*70)

def show_pause_menu():
    """Show the pause menu and handle user input"""
    # Ensure progress is muted while showing menu
    suppress_progress_display.set()
    # Small delay to allow worker PAUSED messages to land
    time.sleep(MENU_SETTLE_MS / 1000.0)

    # Optionally clear the console for a crisp menu
    if MENU_CLEAR_CONSOLE:
        try:
            clear_console()
        except Exception:
            pass
    
    print("\n" + "="*70)
    print("🔹 ALL WORKERS ARE PAUSED")
    print("="*70)
    sys.stdout.flush()
    
    while pause_requested.is_set():
        try:
            # Print prompt on a clean line and flush
            sys.stdout.write("🔹 [R]esume, [Q]uit immediately, or [S]hutdown after current files? ")
            sys.stdout.flush()
            choice = input().lower().strip()
            
            if choice == 'r' or choice == 'resume':
                print("🔸 RESUMING all workers...")
                print("="*70)
                worker_paused.set()  # Resume workers
                pause_requested.clear()  # Clear pause request
                suppress_progress_display.clear()  # Re-enable progress
                
                break
            elif choice == 'q' or choice == 'quit':
                print("🔸 SHUTTING DOWN immediately (terminating current jobs)...")
                print("="*70)
                shutdown_requested.set()  # Immediate shutdown
                worker_paused.set()  # Allow workers to see shutdown signal
                suppress_progress_display.clear()
                pause_requested.clear()  # Clear pause request
                break
            elif choice == 's' or choice == 'shutdown':
                print("🔸 GRACEFUL SHUTDOWN - letting current files finish...")
                print("🔸 No new files will be started. Press Ctrl+C to force immediate shutdown.")
                print("="*70)
                graceful_shutdown_requested.set()  # Graceful shutdown
                worker_paused.set()  # Allow workers to finish current files
                suppress_progress_display.clear()
                pause_requested.clear()  # Clear pause request
                break
            else:
                print("🔸 Invalid choice. Please enter R, Q, or S.")
                
        except (EOFError, KeyboardInterrupt):
            # Handle Ctrl+C during menu input
            print("\n🔸 RESUMING all workers...")
            print("="*70)
            worker_paused.set()
            pause_requested.clear()  # Clear pause request
            break

    # Redraw progress after menu if desired
    if MENU_CLEAR_CONSOLE:
        try:
            clear_console()
        except Exception:
            pass
    if SHOW_PROGRESS and progress_data and not suppress_progress_display.is_set():
        print()
        sys.stdout.flush()
        display_progress()

def clear_console():
    """Clear the terminal screen and move cursor to home position."""
    # ANSI clear screen and home cursor
    sys.stdout.write('\033[2J\033[H')
    sys.stdout.flush()

def wait_if_paused(thread_id, filename):
    """Check if workers should be paused and wait if necessary"""
    # Check for immediate shutdown first
    if shutdown_requested.is_set():
        return None
    
    if not worker_paused.is_set():  # If workers should be paused
        update_progress(thread_id, filename, None, "⏸ PAUSED")
        if not QUIET and not SHOW_PROGRESS:
            print(f"[{thread_id}] ⏸ PAUSED: {os.path.basename(filename)}")
        
        # Check if this is the first worker to pause and print separator
        with progress_lock:
            paused_count = sum(1 for data in progress_data.values() if data['status'] == '⏸ PAUSED')
            if paused_count == 1:  # First worker to pause
                print("\n" + "─"*70)
                print("⏸ ALL WORKERS PAUSED - WAITING FOR RESUME COMMAND")
                print("─"*70)
        
        # Wait for resume with periodic checks to avoid indefinite blocking
        while not worker_paused.is_set() and not shutdown_requested.is_set():
            if worker_paused.wait(timeout=0.1):  # Wait up to 100ms for better responsiveness
                break
            # Add periodic debug info to help track stuck workers
            if int(time.time()) % 10 == 0:  # Every 10 seconds
                if not QUIET and not SHOW_PROGRESS:
                    print(f"[{thread_id}] 🔸 Still waiting for resume signal...")
        
        if shutdown_requested.is_set():
            return None  # Signal cancellation due to shutdown
        
        # Double-check that we're actually resumed before continuing
        if worker_paused.is_set():
            if not QUIET and not SHOW_PROGRESS:
                print(f"[{thread_id}] ▶ RESUMED: {os.path.basename(filename)}")
        else:
            # This shouldn't happen, but let's handle it gracefully
            if not QUIET and not SHOW_PROGRESS:
                print(f"[{thread_id}] ⚠ WARNING: Exited pause loop but worker_paused not set")
    
    # Check for immediate shutdown after resume
    if shutdown_requested.is_set():
        return None
    
    return True

def check_shutdown():
    """Check if shutdown has been requested"""
    return shutdown_requested.is_set()

def should_start_new_job():
    """Check if new jobs should be started (false during graceful shutdown)"""
    return not shutdown_requested.is_set() and not graceful_shutdown_requested.is_set()

def update_progress(thread_id, filename, progress, status="Processing", extra_info=""):
    """Update progress for a specific thread"""
    if not SHOW_PROGRESS or suppress_progress_display.is_set():
        return
    
    with progress_lock:
        progress_data[thread_id] = {
            'filename': os.path.basename(filename),
            'progress': progress if progress is not None else 0,
            'status': status,
            'extra_info': extra_info
        }
        display_progress()

def display_progress():
    """Display progress bars for all active threads"""
    if not SHOW_PROGRESS or not progress_data or suppress_progress_display.is_set():
        return
    
    # Clear previous lines
    sys.stdout.write('\033[2K\r')  # Clear current line
    for _ in range(len(progress_data)):
        sys.stdout.write('\033[A\033[2K')  # Move up and clear line
    
    # Sort threads by worker number for consistent display order
    def get_worker_number(thread_id):
        try:
            if thread_id.startswith("Worker_"):
                return int(thread_id.split("_")[1])
            else:
                return 999  # Put non-Worker threads at the end
        except:
            return 999
    
    sorted_threads = sorted(progress_data.keys(), key=get_worker_number)
    
    # Display progress for each thread in sorted order
    for thread_id in sorted_threads:
        data = progress_data[thread_id]
        filename = data['filename']
        progress = data['progress']
        status = data['status']
        extra_info = data.get('extra_info', '')
        
        # Truncate or pad filename to exactly 35 characters for consistent alignment
        if len(filename) > 35:
            display_filename = filename[:32] + "..."
        else:
            display_filename = filename.ljust(35)  # Left-justify and pad to 35 chars
        
        # Create progress bar
        bar_length = 25
        filled_length = int(bar_length * progress / 100)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        # Format thread ID to be consistent width (pad to 9 chars to handle "Worker_X")
        thread_display = f"[{thread_id}]".ljust(10)
        
        # Format the display line with consistent spacing
        if extra_info:
            print(f"{thread_display} {display_filename} [{bar}] {progress:5.1f}% {status} {extra_info}")
        else:
            print(f"{thread_display} {display_filename} [{bar}] {progress:5.1f}% {status}")
    
    sys.stdout.flush()

def is_network_path(path):
    """Heuristic to detect a UNC network path (\\\\server\\share\\...)."""
    if not path:
        return False
    # UNC path starts with two backslashes
    return path.startswith('\\\\')

def get_unc_root(path):
    """Return the \\server\share root for a UNC path, else None."""
    if not is_network_path(path):
        return None
    parts = path.strip('\\').split('\\')
    if len(parts) >= 2:
        return f"\\\\{parts[0]}\\{parts[1]}"
    return None

def wait_for_network(path, thread_id=None):
    """Wait for network path to become available. Returns True if available, False if timed out."""
    if not NETWORK_RETRY_ENABLED:
        return True
    if not is_network_path(path):
        return True
    root = get_unc_root(path) or path
    start = time.time()
    notified = False
    while True:
        try:
            # Try listing root to wake connection
            os.listdir(root)
            return True
        except Exception as e:
            if time.time() - start > NETWORK_MAX_WAIT:
                if not QUIET:
                    print(f"[{thread_id or 'MAIN'}] NETWORK TIMEOUT: {root} still unavailable after {NETWORK_MAX_WAIT}s: {e}")
                return False
            # Provide periodic feedback
            if not QUIET:
                if not notified:
                    print(f"[{thread_id or 'MAIN'}] Waiting for network share {root} to become available...")
                    notified = True
                elif int(time.time() - start) % 60 == 0:  # every minute
                    print(f"[{thread_id or 'MAIN'}] Still waiting for network share {root} ({int(time.time() - start)}s)...")
            # Update progress status if in a worker
            if thread_id and SHOW_PROGRESS and not suppress_progress_display.is_set():
                update_progress(thread_id, os.path.basename(path), 0, "WaitingNet")
            if shutdown_requested.is_set():
                return False
            time.sleep(NETWORK_CHECK_INTERVAL)

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
    # Decide which log file to use
    target_log = FAILED_LOG_FILE if status == "failed" else LOG_FILE
    with log_lock:  # Thread-safe logging
        with open(target_log, "a", newline="", encoding="utf-8") as f:
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
    
    # Check for shutdown before starting
    if not wait_if_paused(thread_id, filename):
        # Don't log cancelled jobs as failed
        return None  # Return None to indicate cancellation, not failure
    
    # Ensure network path (if any) is available before starting
    if not wait_for_network(filepath, thread_id):
        log_result(filepath, "failed_network_unavailable")
        return False

    # Initialize progress
    update_progress(thread_id, filename, 0, "Starting")
    
    # Check if video resolution is less than 1080p
    update_progress(thread_id, filename, 2, "Checking resolution")
    
    # Check for pause/shutdown
    if not wait_if_paused(thread_id, filename):
        return None  # Return None to indicate cancellation
    
    should_skip, resolution_info = should_skip_resolution(filepath)
    
    if should_skip:
        clear_progress(thread_id)
        original_size = os.path.getsize(filepath)
        log_result(filepath, f"skipped_low_res_{resolution_info}", original_size)
        _print_worker_event(f"[{thread_id}] SKIP low-res {resolution_info}: {filepath}")
        return True  # Return True since this is successful processing (just skipped)
    
    # Check if transcoding will likely result in larger file
    update_progress(thread_id, filename, 4, "Analyzing codec")
    
    # Check for pause/shutdown
    if not wait_if_paused(thread_id, filename):
        return None  # Return None to indicate cancellation
    
    should_skip_codec, codec_info = should_skip_likely_larger(filepath)
    
    if should_skip_codec:
        clear_progress(thread_id)
        original_size = os.path.getsize(filepath)
        log_result(filepath, f"skipped_likely_larger_{codec_info.replace(' ', '_').replace('(', '').replace(')', '')}", original_size)
        _print_worker_event(f"[{thread_id}] SKIP likely larger ({codec_info}): {filepath}")
        return True  # Return True since this is successful processing (just skipped)

    # Check for pause/shutdown before starting expensive transcode
    if not wait_if_paused(thread_id, filename):
        return None  # Return None to indicate cancellation

    if not QUIET and not SHOW_PROGRESS:
        print(f"[{thread_id}] Resolution: {resolution_info}, Codec: {codec_info} - proceeding with transcode")
    
    # Use a local temp directory instead of network path for better reliability
    import tempfile
    local_temp_dir = tempfile.gettempdir()
    temp_fd, temp_path = tempfile.mkstemp(suffix=ext, dir=local_temp_dir)
    os.close(temp_fd)

    # Get original file size (with network retry if needed)
    try:
        original_size = os.path.getsize(filepath)
    except OSError as e:
        # Potential transient network failure; attempt wait once more
        if wait_for_network(filepath, thread_id):
            try:
                original_size = os.path.getsize(filepath)
            except Exception as e2:
                log_result(filepath, "failed_network_stat")
                if not QUIET:
                    print(f"[{thread_id}] NETWORK ERROR: Could not stat file after retry: {e2}")
                return False
        else:
            log_result(filepath, "failed_network_unavailable")
            if not QUIET:
                print(f"[{thread_id}] NETWORK UNAVAILABLE: {filepath}")
            return False
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

    if not QUIET and not SHOW_PROGRESS:
        print(f"[{thread_id}] Transcoding: {filepath} ({original_size / (1024*1024):.2f} MB)")
        print(f"[{thread_id}] Temp file: {temp_path}")
    
    update_progress(thread_id, filename, 10, "Transcoding")
    
    # Check available disk space
    try:
        import shutil
        total, used, free = shutil.disk_usage(local_temp_dir)
        free_gb = free / (1024**3)
        if not QUIET and not SHOW_PROGRESS:
            print(f"[{thread_id}] Free disk space: {free_gb:.2f} GB")
        
        if free_gb < 10:  # Less than 10GB free
            if not QUIET and not SHOW_PROGRESS:
                print(f"[{thread_id}] WARNING: Low disk space ({free_gb:.2f} GB)")
    except Exception as e:
        if not QUIET and not SHOW_PROGRESS:
            print(f"[{thread_id}] Could not check disk space: {e}")
    
    # Start transcoding with progress simulation
    start_time = time.time()
    result = None
    handbrake_process = None
    process_suspended = False
    
    # Run HandBrake and capture real-time progress
    def run_handbrake():
        nonlocal result, handbrake_process
        try:
            # On Windows, create HandBrake in a new process group so Ctrl+C doesn't kill it
            creationflags = 0
            if IS_WINDOWS:
                creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)

            try:
                handbrake_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    creationflags=creationflags
                )
            except OSError as e:
                # Possibly network path vanished; attempt to wait and retry once
                if wait_for_network(filepath, thread_id):
                    handbrake_process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                        universal_newlines=True,
                        creationflags=creationflags
                    )
                else:
                    raise
            
            stdout_lines = []
            
            # Read output line by line to capture progress
            while True:
                try:
                    # Check if the process is still alive before reading
                    if handbrake_process.poll() is not None:
                        # Process has terminated, read any remaining output
                        remaining_output = handbrake_process.stdout.read()
                        if remaining_output:
                            stdout_lines.extend(remaining_output.strip().split('\n'))
                        break
                    
                    output = handbrake_process.stdout.readline()
                    if output == '' and handbrake_process.poll() is not None:
                        break
                    if output:
                        stdout_lines.append(output.strip())
                        
                        # Parse HandBrake progress output
                        # HandBrake outputs progress like: "Encoding: task 1 of 1, 45.67 % (23.45 fps, avg 24.12 fps, ETA 00h15m42s)"
                        if "Encoding:" in output and "%" in output:
                            try:
                                # Extract percentage from output
                                percent_start = output.find("% (")
                                if percent_start > 0:
                                    # Look backwards for the percentage number
                                    percent_text = output[:percent_start]
                                    percent_parts = percent_text.split()
                                    if percent_parts:
                                        progress_percent = float(percent_parts[-1])
                                        
                                        # Extract additional info (fps, ETA)
                                        extra_info = ""
                                        if "fps," in output and "ETA" in output:
                                            try:
                                                # Extract current fps
                                                fps_start = output.find("(") + 1
                                                fps_end = output.find(" fps,")
                                                if fps_start > 0 and fps_end > fps_start:
                                                    fps = output[fps_start:fps_end]
                                                    
                                                # Extract ETA
                                                eta_start = output.find("ETA ") + 4
                                                eta_end = output.find(")", eta_start)
                                                if eta_start > 3 and eta_end > eta_start:
                                                    eta = output[eta_start:eta_end]
                                                    extra_info = f"({fps} fps, ETA {eta})"
                                            except:
                                                pass
                                        
                                        # Update progress with real HandBrake progress
                                        if worker_paused.is_set() and not shutdown_requested.is_set():  # Only update if not paused and not shutting down
                                            update_progress(thread_id, filename, progress_percent, "Transcoding", extra_info)
                            except (ValueError, IndexError):
                                pass  # Ignore parsing errors
                except (OSError, ValueError) as e:
                    # Handle broken pipe or other I/O errors
                    if handbrake_process and handbrake_process.poll() is not None:
                        break  # Process has terminated
                    else:
                        break  # Stop reading on error
            
            # Get final result
            result = handbrake_process.poll()
            result = type('Result', (), {
                'returncode': result,
                'stdout': '\n'.join(stdout_lines),
                'stderr': ''
            })()
            
        except Exception as e:
            # Handle any other exceptions in the HandBrake thread
            result = type('Result', (), {
                'returncode': -1,
                'stdout': '',
                'stderr': f'HandBrake thread error: {str(e)}'
            })()
    
    import threading as thread_module
    handbrake_thread = thread_module.Thread(target=run_handbrake)
    handbrake_thread.start()
    
    # Monitor for pause/resume while HandBrake is running
    last_pause_check = time.time()
    fallback_progress = 10  # Fallback progress for when we can't parse HandBrake output
    was_paused_during_execution = False  # Track if we paused during this file
    
    while handbrake_thread.is_alive():
        try:
            current_time = time.time()

            # Check for pause/shutdown more frequently (every 0.5 seconds instead of 2)
            if current_time - last_pause_check > 0.5:
                if not worker_paused.is_set() and not process_suspended:
                    # Mark that this file was paused during execution
                    was_paused_during_execution = True

                    if not QUIET and not SHOW_PROGRESS:
                        print(f"[{thread_id}] 🔸 PAUSE DETECTED - Suspending HandBrake process...")

                    # Actually suspend the HandBrake process (Windows only)
                    if WINDOWS_PROCESS_CONTROL and handbrake_process and handbrake_process.pid:
                        # Check if process is still alive before suspending
                        if handbrake_process.poll() is None:  # Process is still running
                            if suspend_process(handbrake_process.pid):
                                process_suspended = True
                                if not QUIET and not SHOW_PROGRESS:
                                    print(f"[{thread_id}] 🔸 HandBrake process {handbrake_process.pid} suspended")
                            else:
                                if not QUIET and not SHOW_PROGRESS:
                                    print(f"[{thread_id}] ⚠ WARNING: Failed to suspend HandBrake process {handbrake_process.pid}")
                                was_paused_during_execution = True
                        else:
                            if not QUIET and not SHOW_PROGRESS:
                                print(f"[{thread_id}] ⚠ WARNING: HandBrake process {handbrake_process.pid} already terminated")
                            was_paused_during_execution = True

                    update_progress(thread_id, filename, fallback_progress, "⏸ PAUSED")

                    # Wait for resume with timeout to allow signal handling
                    while not worker_paused.is_set() and not shutdown_requested.is_set():
                        # Use a shorter timeout and add periodic status updates
                        if worker_paused.wait(timeout=0.1):  # Check every 100ms for better responsiveness
                            break
                        # Update progress periodically to show we're still alive
                        if int(time.time()) % 5 == 0:  # Every 5 seconds
                            update_progress(thread_id, filename, fallback_progress, "⏸ PAUSED (waiting)")

                    # Resume the HandBrake process (Windows only)
                    if WINDOWS_PROCESS_CONTROL and process_suspended and handbrake_process and handbrake_process.pid:
                        # Check if process is still alive before resuming
                        if handbrake_process.poll() is None:  # Process is still running
                            if resume_process(handbrake_process.pid):
                                process_suspended = False
                                if not QUIET and not SHOW_PROGRESS:
                                    print(f"[{thread_id}] ▶ HandBrake process {handbrake_process.pid} resumed")
                            else:
                                if not QUIET and not SHOW_PROGRESS:
                                    print(f"[{thread_id}] ⚠ WARNING: Failed to resume HandBrake process {handbrake_process.pid}")
                                # If resume failed, the process might be dead - this should be treated as interruption
                                was_paused_during_execution = True
                        else:
                            if not QUIET and not SHOW_PROGRESS:
                                print(f"[{thread_id}] ⚠ WARNING: HandBrake process {handbrake_process.pid} died while suspended")
                            was_paused_during_execution = True
                            process_suspended = False

                    # Clear paused status and show we're back to work
                    update_progress(thread_id, filename, fallback_progress, "Transcoding")

                    if not QUIET and not SHOW_PROGRESS:
                        print(f"[{thread_id}] ▶ RESUMING - HandBrake process should be active again")

                    if shutdown_requested.is_set():
                        # Terminate HandBrake and cleanup
                        if handbrake_process:
                            if WINDOWS_PROCESS_CONTROL and process_suspended:
                                resume_process(handbrake_process.pid)  # Resume before terminating
                            handbrake_process.terminate()
                            handbrake_process.wait()
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                        clear_progress(thread_id)
                        # Log as interrupted, not failed, so it can be retried
                        log_result(filepath, "interrupted", original_size)
                        return None  # Return None to indicate cancellation, not failure

                last_pause_check = current_time

            # Fallback progress estimation (only used if HandBrake progress parsing fails)
            elapsed = time.time() - start_time
            estimated_time = original_size / (1024 * 1024)  # seconds
            if estimated_time > 0:
                time_progress = min(80, (elapsed / estimated_time) * 70 + 10)  # 10-80%
                fallback_progress = max(fallback_progress, time_progress)
            else:
                fallback_progress = min(80, fallback_progress + 1)

            time.sleep(1)

            # Prevent indefinite waiting
            if elapsed > 3600:  # 1 hour timeout
                break

        except KeyboardInterrupt:
            # Swallow in worker loop; main thread owns the pause menu and orchestration
            continue
    
    handbrake_thread.join()
    
    # Check for shutdown after HandBrake completes
    if shutdown_requested.is_set():
        # Immediate shutdown - cleanup and exit
        if os.path.exists(temp_path):
            os.remove(temp_path)
        clear_progress(thread_id)
        # Log as interrupted, not failed, so it can be retried
        log_result(filepath, "interrupted", original_size)
        return None
    elif graceful_shutdown_requested.is_set():
        # Graceful shutdown - finish this job but don't start new ones
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] 🔸 Graceful shutdown - completing current job: {os.path.basename(filename)}")
        # Continue with normal processing to finish this job
    
    update_progress(thread_id, filename, 85, "Finishing")

    # Check if HandBrake failed, but consider if it was paused during execution
    if result.returncode != 0:
        clear_progress(thread_id)
        if not SHOW_PROGRESS:
            print(f"[{thread_id}] ERROR: {filepath}")
            print(f"[{thread_id}] Return code: {result.returncode}")
            print(f"[{thread_id}] STDOUT: {result.stdout}")
            print(f"[{thread_id}] STDERR: {result.stderr}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        # If the file was paused during execution, treat failure as interruption
        if was_paused_during_execution:
            if not QUIET and not SHOW_PROGRESS:
                print(f"[{thread_id}] INTERRUPTED: File was paused during execution, marking as interrupted for retry")
            log_result(filepath, "interrupted", original_size)
            _print_worker_event(f"[{thread_id}] INTERRUPTED (during transcode): {filepath}")
            return None  # Treat as interruption, not failure
        else:
            log_result(filepath, "failed", original_size)
            _print_worker_event(f"[{thread_id}] FAIL (return code {result.returncode}): {filepath}")
            return False

    # Check if temp file was actually created and has content
    if not os.path.exists(temp_path):
        clear_progress(thread_id)
        if not QUIET and not SHOW_PROGRESS:
            print(f"[{thread_id}] ERROR: Temp file was not created: {temp_path}")
        log_result(filepath, "failed", original_size)
        _print_worker_event(f"[{thread_id}] FAIL (no temp output): {filepath}")
        return False
    
    # Get transcoded file size
    transcoded_size = os.path.getsize(temp_path)
    if transcoded_size == 0:
        clear_progress(thread_id)
        if not QUIET and not SHOW_PROGRESS:
            print(f"[{thread_id}] ERROR: Transcoded file is empty: {temp_path}")
        os.remove(temp_path)
        log_result(filepath, "failed", original_size)
        _print_worker_event(f"[{thread_id}] FAIL (empty output): {filepath}")
        return False
    
    update_progress(thread_id, filename, 90, "Backing up")
    compression_ratio = transcoded_size / original_size if original_size > 0 else 0

    # Check if transcoded file is larger than original
    if transcoded_size >= original_size:
        clear_progress(thread_id)
        if not QUIET and not SHOW_PROGRESS:
            print(f"[{thread_id}] SKIP: Transcoded file is larger ({transcoded_size / (1024*1024):.2f} MB >= {original_size / (1024*1024):.2f} MB): {filepath}")
        
        # Remove the temp file since we're not using it
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        # Log as skipped due to larger size
        log_result(filepath, f"skipped_larger_size_{compression_ratio:.3f}", original_size, transcoded_size)
        _print_worker_event(f"[{thread_id}] SKIP larger result {compression_ratio:.3f}: {filepath}")
        return True  # Return True since this is successful processing (just skipped)

    # Check for pause/shutdown before final operations
    if wait_if_paused(thread_id, filename) is None:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return None

    # Backup original (if enabled)
    if CREATE_BACKUPS:
        try:
            # Create relative path structure in backup directory
            rel_path = os.path.relpath(filepath, os.path.dirname(root_backup_dir))
            backup_file_path = os.path.join(root_backup_dir, rel_path)
            backup_file_dir = os.path.dirname(backup_file_path)
            
            os.makedirs(backup_file_dir, exist_ok=True)
            shutil.copy2(filepath, backup_file_path)
            if not QUIET and not SHOW_PROGRESS:
                print(f"[{thread_id}] Backup created: {backup_file_path}")
        except Exception as e:
            if not QUIET and not SHOW_PROGRESS:
                print(f"[{thread_id}] WARNING: Could not create backup: {e}")

    update_progress(thread_id, filename, 95, "Finalizing")
    
    # Replace original
    move_attempt = 0
    delay = FINAL_MOVE_RETRY_DELAY
    while True:
        try:
            shutil.move(temp_path, filepath)
            log_result(filepath, "success", original_size, transcoded_size)
            update_progress(thread_id, filename, 100, "Complete")
            time.sleep(0.5)
            clear_progress(thread_id)
            if not QUIET and not SHOW_PROGRESS:
                print(f"[{thread_id}] SUCCESS: {filepath} ({original_size / (1024*1024):.2f} MB -> {transcoded_size / (1024*1024):.2f} MB, ratio: {compression_ratio:.3f})")
            _print_worker_event(f"[{thread_id}] OK {compression_ratio:.3f} {os.path.basename(filepath)}")
            return True
        except Exception as e:
            move_attempt += 1
            if move_attempt > FINAL_MOVE_RETRIES:
                clear_progress(thread_id)
                if not QUIET and not SHOW_PROGRESS:
                    print(f"[{thread_id}] ERROR: Final move failed after {FINAL_MOVE_RETRIES} retries: {e}")
                # Best effort cleanup: if temp still exists, leave it for manual recovery instead of deleting
                log_result(filepath, "failed", original_size)
                _print_worker_event(f"[{thread_id}] FAIL (final move exhausted retries): {filepath}")
                return False
            # Attempt network wait (UNC only)
            wait_for_network(filepath, thread_id)
            if not QUIET:
                _print_worker_event(f"[{thread_id}] RETRY move {move_attempt}/{FINAL_MOVE_RETRIES} in {delay}s: {os.path.basename(filepath)} -> {e}")
            time.sleep(delay)
            delay *= FINAL_MOVE_BACKOFF_FACTOR

# === Worker function for thread pool ===
def process_file_worker(filepath, root_backup_dir):
    """Worker function that processes a single file and handles exceptions"""
    thread_id = threading.current_thread().name
    try:
        result = transcode_file(filepath, root_backup_dir)
        # Don't log cancelled jobs (None) - only log actual failures (False)
        if result is False:
            # Get original file size for failed operations
            try:
                original_size = os.path.getsize(filepath)
                log_result(filepath, "failed", original_size)
            except:
                log_result(filepath, "failed")
        elif result is None:
            # This was an interrupted/cancelled job, already logged as "interrupted"
            pass
        return result
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully - this should be treated as interruption
        print(f"INTERRUPTED: {thread_id} during {filepath}")

        clear_progress(thread_id)
        try:
            original_size = os.path.getsize(filepath)
            log_result(filepath, "interrupted", original_size)
        except:
            log_result(filepath, "interrupted")
        return None
    except Exception as e:
        print(f"ERROR: Exception in {thread_id} during {filepath}: {e}")
        # Clear any progress display for this worker
        clear_progress(thread_id)
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
                        if not QUIET:
                            print(f"SKIP: Already processed: {full_path}")
                        continue
                    elif status.startswith("skipped_low_res_"):
                        if not QUIET:
                            print(f"SKIP: Low resolution already checked: {full_path}")
                        continue
                    elif status.startswith("skipped_larger_size_"):
                        if not QUIET:
                            print(f"SKIP: Transcoding would increase file size: {full_path}")
                        continue
                    elif status.startswith("skipped_likely_larger_"):
                        if not QUIET:
                            print(f"SKIP: Likely to create larger file: {full_path}")
                        continue
                    elif status == "interrupted":
                        if not QUIET:
                            print(f"RETRY: Previously interrupted: {full_path}")
                        # Allow interrupted files to be retried
                    elif status == "failed":
                        if not QUIET:
                            print(f"RETRY: Previously failed: {full_path}")
                        # Allow failed files to be retried
                files_to_process.append(full_path)
    
    if not files_to_process:
        print("No video files to process.")
        return
    
    if not QUIET:
        print(f"Found {len(files_to_process)} video files to process using {MAX_WORKERS} threads.")
        if CREATE_BACKUPS:
            print(f"Backups will be stored in: {root_backup_dir}")
        print(f"Transcode Videos Script v{VERSION}")
    
    # Platform information
    if not QUIET:
        if WINDOWS_PROCESS_CONTROL:
            print("🔸 Windows process control enabled - HandBrake processes can be paused/resumed")
        elif IS_WINDOWS:
            print("⚠️  Windows detected but process control unavailable - only progress display pausing")
        else:
            print(f"🔸 Running on {platform.system()} - process suspension not available, progress display pausing only")
        print("=" * 50)
    
    # Process files using ThreadPoolExecutor
    successful = 0
    failed = 0
    skipped_resolution = 0
    skipped_larger = 0
    skipped_codec = 0
    
    # Create a custom worker function that sets the thread name
    worker_counter = 0
    worker_name_map = {}  # Map to track worker names for consistency
    
    def named_worker(filepath, root_backup_dir):
        nonlocal worker_counter
        thread = threading.current_thread()
        
        # Assign a consistent worker name if not already assigned
        if thread not in worker_name_map:
            worker_counter += 1
            worker_name_map[thread] = f"Worker_{worker_counter}"
        
        thread.name = worker_name_map[thread]
        return process_file_worker(filepath, root_backup_dir)
    
    # Register Ctrl+C handling
    if IS_WINDOWS:
        # Use Windows console control handler for reliable Ctrl+C pause
        register_windows_ctrl_c_handler()
    else:
        # Non-Windows: standard Python signal handler
        signal.signal(signal.SIGINT, signal_handler)
    
    # Note: On Windows, console control handler is used; on other platforms SIGINT is used.
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_file = {}
            for filepath in files_to_process:
                future = executor.submit(named_worker, filepath, root_backup_dir)
                future_to_file[future] = filepath
        
        # Process completed tasks with Ctrl+C pause support
        completed_jobs = 0
        total_jobs = len(files_to_process)

        pending = set(future_to_file.keys())
        while pending:
            try:
                # Wait briefly for any completed futures, so we can catch Ctrl+C between waits
                done, not_done = concurrent.futures.wait(pending, timeout=0.5, return_when=concurrent.futures.FIRST_COMPLETED)

                for future in done:
                    pending.discard(future)
                    completed_jobs += 1

                    # Check for immediate shutdown before processing results
                    if shutdown_requested.is_set():
                        print(f"\n🔸 IMMEDIATE SHUTDOWN requested - cancelling remaining tasks...")
                        for f in pending:
                            if not f.done():
                                f.cancel()
                        pending.clear()
                        break

                    # Check for graceful shutdown
                    if graceful_shutdown_requested.is_set():
                        remaining_jobs = total_jobs - completed_jobs
                        if remaining_jobs > 0:
                            print(f"\n🔸 GRACEFUL SHUTDOWN in progress - {remaining_jobs} jobs remaining...")
                        else:
                            print("\n🔸 GRACEFUL SHUTDOWN complete - all jobs finished")

                    filepath = future_to_file[future]
                    try:
                        success = future.result()
                        if success is True:
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
                        elif success is False:
                            failed += 1
                        # If success is None (cancelled), don't count it as anything
                    except Exception as e:
                        print(f"ERROR: Unexpected exception for {filepath}: {e}")
                        failed += 1

            except KeyboardInterrupt:
                # Convert Ctrl+C in main thread into PAUSE and block here until input
                if VERBOSE and not QUIET:
                    print("\n" + "="*70)
                    print("🔸 Ctrl+C detected - PAUSING all workers and showing menu")
                    print("="*70)

                if worker_paused.is_set():
                    worker_paused.clear()
                pause_requested.set()

                # Suppress progress rendering while the menu is active
                suppress_progress_display.set()
                # Give workers a moment to print their "PAUSED" status
                time.sleep(MENU_SETTLE_MS / 1000.0)
                # Show menu synchronously in main thread to ensure prompt is visible
                show_pause_menu()
                suppress_progress_display.clear()
                # After closing menu, force a redraw of progress once
                with progress_lock:
                    if SHOW_PROGRESS and progress_data:
                        print()  # newline to separate menu
                        sys.stdout.flush()
                        display_progress()

                # After menu returns, check for shutdown
                if shutdown_requested.is_set():
                    print(f"\n🔸 IMMEDIATE SHUTDOWN requested from menu - cancelling remaining tasks...")
                    for f in pending:
                        if not f.done():
                            f.cancel()
                    break
    
    finally:
        # Cleanup any remaining processes
        if VERBOSE and not QUIET:
            print("🔸 Cleaning up...")
    
    # Always print final summary, even in quiet mode
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

    # Windows-only warning (do not exit)
    if not IS_WINDOWS:
        print("Warning: This script is designed for Windows. Some pause/resume features may not work on non-Windows systems.")

    # Default: register signal handler for Ctrl+C (overridden on Windows by console handler)
    signal.signal(signal.SIGINT, signal_handler)
    
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
    
    # Simple flag parsing for -q/--quiet and -v/--verbose
    args = sys.argv[1:]
    parsed_args = []
    for a in args:
        if a in ("-q", "--quiet"):
            QUIET = True
        elif a in ("-v", "--verbose"):
            VERBOSE = True
        else:
            parsed_args.append(a)

    if len(parsed_args) < 1 or len(parsed_args) > 2:
        print(f"Transcode Videos Script v{VERSION}")
        print("Usage: python transcode_videos.py [/path/to/videos] [max_workers] [--quiet|-q] [--verbose|-v]")
        print("  max_workers: Number of concurrent operations (default: 4)")
        print("Use -h or --help for detailed information.")
        sys.exit(1)

    input_dir = parsed_args[0]
    if not os.path.isdir(input_dir):
        print(f"Invalid directory: {input_dir}")
        sys.exit(1)

    # Optional: Allow user to specify number of workers
    if len(parsed_args) == 2:
        try:
            MAX_WORKERS = int(parsed_args[1])
            if MAX_WORKERS < 1:
                print("max_workers must be at least 1")
                sys.exit(1)
        except ValueError:
            print("max_workers must be a valid integer")
            sys.exit(1)

    # If quiet, suppress progress rendering globally (without enabling debug prints)
    if QUIET:
        suppress_progress_display.set()

    process_directory(input_dir)
