#!/usr/bin/env python3
"""
Retranscode Larger Files Script
Analyzes the transcode log and retranscodes files that became larger using a more aggressive preset.
"""

import os
import sys
import csv
import subprocess
import tempfile
import shutil
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Version ===
VERSION = "1.0.0"

# === Config ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ORIGINAL_LOG_FILE = os.path.join(SCRIPT_DIR, "transcode_log.csv")
RETRANSCODE_LOG_FILE = os.path.join(SCRIPT_DIR, "retranscode_log.csv")
PRESET = "Very Fast 1080p30"  # More aggressive built-in preset
SIZE_TOLERANCE = 0.10  # Skip if within 10% of original size
MAX_WORKERS = 7  # Number of concurrent operations
VERBOSE_HANDBRAKE = True  # Enable verbose output for debugging

# Thread lock for log file access
log_lock = threading.Lock()

def log_retranscode_result(filepath, status, original_size=None, old_transcoded_size=None, new_transcoded_size=None):
    """Log the retranscode result"""
    with log_lock:
        with open(RETRANSCODE_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow([
                    "filepath", "status", "timestamp", 
                    "original_size_mb", "old_transcoded_size_mb", "new_transcoded_size_mb", 
                    "old_compression_ratio", "new_compression_ratio", "improvement"
                ])
            
            # Convert sizes to MB
            original_mb = f"{original_size / (1024*1024):.2f}" if original_size is not None else ""
            old_transcoded_mb = f"{old_transcoded_size / (1024*1024):.2f}" if old_transcoded_size is not None else ""
            new_transcoded_mb = f"{new_transcoded_size / (1024*1024):.2f}" if new_transcoded_size is not None else ""
            
            # Calculate ratios
            old_ratio = ""
            new_ratio = ""
            improvement = ""
            
            if original_size and old_transcoded_size:
                old_ratio = f"{(old_transcoded_size / original_size):.3f}"
            
            if original_size and new_transcoded_size:
                new_ratio = f"{(new_transcoded_size / original_size):.3f}"
                
            if old_transcoded_size and new_transcoded_size:
                improvement = f"{((old_transcoded_size - new_transcoded_size) / old_transcoded_size * 100):.1f}%"
            
            writer.writerow([
                filepath, status, datetime.now().isoformat(),
                original_mb, old_transcoded_mb, new_transcoded_mb,
                old_ratio, new_ratio, improvement
            ])

def get_files_to_retranscode():
    """Analyze the log file and find files that became larger and should be retranscoded"""
    files_to_process = []
    
    if not os.path.exists(ORIGINAL_LOG_FILE):
        print(f"Error: Original log file not found: {ORIGINAL_LOG_FILE}")
        return files_to_process
    
    print("Analyzing transcode log for files that became larger...")
    
    with open(ORIGINAL_LOG_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["status"] == "success":
                try:
                    before_size = float(row["before_size_mb"]) if row["before_size_mb"] else None
                    after_size = float(row["after_size_mb"]) if row["after_size_mb"] else None
                    
                    if before_size and after_size:
                        compression_ratio = after_size / before_size
                        size_increase_percent = (compression_ratio - 1.0) * 100
                        
                        # Check if file became larger and is beyond tolerance
                        if compression_ratio > (1.0 + SIZE_TOLERANCE):
                            filepath = row["filepath"]
                            
                            # Check if file still exists
                            if os.path.exists(filepath):
                                files_to_process.append({
                                    'filepath': filepath,
                                    'original_size_mb': before_size,
                                    'current_size_mb': after_size,
                                    'compression_ratio': compression_ratio,
                                    'size_increase_percent': size_increase_percent
                                })
                                print(f"Found: {os.path.basename(filepath)} - increased by {size_increase_percent:.1f}%")
                            else:
                                print(f"Skipping missing file: {filepath}")
                        
                except (ValueError, TypeError) as e:
                    continue  # Skip rows with invalid data
    
    return files_to_process

def retranscode_file(file_info):
    """Retranscode a single file using the more aggressive preset"""
    filepath = file_info['filepath']
    original_size_mb = file_info['original_size_mb']
    current_size_mb = file_info['current_size_mb']
    
    print(f"\nRetranscoding: {os.path.basename(filepath)}")
    print(f"  Original: {original_size_mb:.2f} MB")
    print(f"  Current:  {current_size_mb:.2f} MB")
    print(f"  Increase: {file_info['size_increase_percent']:.1f}%")
    
    # Get current file size in bytes
    current_size_bytes = os.path.getsize(filepath)
    original_size_bytes = int(original_size_mb * 1024 * 1024)
    
    # Create temp file
    dirpath, filename = os.path.split(filepath)
    name, ext = os.path.splitext(filename)
    
    local_temp_dir = tempfile.gettempdir()
    temp_fd, temp_path = tempfile.mkstemp(suffix=ext, dir=local_temp_dir)
    os.close(temp_fd)
    
    try:
        # Build HandBrake command
        cmd = [
            "HandBrakeCLI",
            "--preset", PRESET,
            "-i", filepath,
            "-o", temp_path,
            "--all-subtitles",
            "--markers"
        ]
        
        if VERBOSE_HANDBRAKE:
            cmd.append("--verbose=1")
        
        print(f"  Running: {' '.join(cmd)}")
        
        # Execute HandBrake
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        if result.returncode != 0:
            print(f"  ERROR: HandBrake failed with return code {result.returncode}")
            print(f"  STDERR: {result.stderr}")
            log_retranscode_result(filepath, "failed", original_size_bytes, current_size_bytes)
            return False
        
        # Check if temp file was created
        if not os.path.exists(temp_path):
            print(f"  ERROR: Temp file was not created")
            log_retranscode_result(filepath, "failed", original_size_bytes, current_size_bytes)
            return False
        
        # Get new file size
        new_size_bytes = os.path.getsize(temp_path)
        if new_size_bytes == 0:
            print(f"  ERROR: Transcoded file is empty")
            os.remove(temp_path)
            log_retranscode_result(filepath, "failed", original_size_bytes, current_size_bytes)
            return False
        
        new_size_mb = new_size_bytes / (1024 * 1024)
        new_compression_ratio = new_size_bytes / original_size_bytes
        
        print(f"  New size: {new_size_mb:.2f} MB (ratio: {new_compression_ratio:.3f})")
        
        # Check if the new transcode is actually better
        if new_size_bytes >= current_size_bytes:
            print(f"  SKIP: New transcode is not smaller ({new_size_mb:.2f} MB >= {current_size_mb:.2f} MB)")
            os.remove(temp_path)
            log_retranscode_result(filepath, "skipped_not_better", original_size_bytes, current_size_bytes, new_size_bytes)
            return True
        
        # Replace the current file with the new one
        shutil.move(temp_path, filepath)
        
        improvement_percent = ((current_size_bytes - new_size_bytes) / current_size_bytes) * 100
        print(f"  SUCCESS: Reduced by {improvement_percent:.1f}% ({current_size_mb:.2f} -> {new_size_mb:.2f} MB)")
        
        log_retranscode_result(filepath, "success", original_size_bytes, current_size_bytes, new_size_bytes)
        return True
        
    except Exception as e:
        print(f"  ERROR: Exception during retranscode: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        log_retranscode_result(filepath, "failed", original_size_bytes, current_size_bytes)
        return False

def main():
    print(f"Retranscode Larger Files Script v{VERSION}")
    print("=" * 50)
    print(f"Analyzing files that became larger than {SIZE_TOLERANCE*100:.0f}% of original size")
    print(f"Using preset: {PRESET}")
    print(f"Max workers: {MAX_WORKERS}")
    print()
    
    # Get files that need retranscoding
    files_to_process = get_files_to_retranscode()
    
    if not files_to_process:
        print("No files found that need retranscoding.")
        return
    
    print(f"\nFound {len(files_to_process)} files to retranscode:")
    for file_info in files_to_process:
        print(f"  - {os.path.basename(file_info['filepath'])} (+{file_info['size_increase_percent']:.1f}%)")
    
    # Confirm with user
    response = input(f"\nProceed with retranscoding {len(files_to_process)} files? [y/N]: ")
    if response.lower() not in ['y', 'yes']:
        print("Cancelled.")
        return
    
    print(f"\nStarting retranscode with {MAX_WORKERS} workers...")
    print("=" * 50)
    
    # Process files
    successful = 0
    failed = 0
    skipped = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(retranscode_file, file_info): file_info 
                         for file_info in files_to_process}
        
        # Process completed tasks
        for future in as_completed(future_to_file):
            file_info = future_to_file[future]
            try:
                success = future.result()
                if success:
                    # Check if it was actually retranscoded or skipped
                    # We'll assume success means improvement for now
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"ERROR: Unexpected exception for {file_info['filepath']}: {e}")
                failed += 1
    
    print(f"\nRetranscode complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total processed: {successful + failed}")
    print(f"Log file: {RETRANSCODE_LOG_FILE}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print(f"Retranscode Larger Files Script v{VERSION}")
            print("Usage: python retranscode_larger_files.py")
            print()
            print("This script analyzes transcode_log.csv and retranscodes files that became")
            print(f"larger than {SIZE_TOLERANCE*100:.0f}% of their original size using a more aggressive preset.")
            print()
            print("Configuration:")
            print(f"  - Preset: {PRESET}")
            print(f"  - Size tolerance: {SIZE_TOLERANCE*100:.0f}%")
            print(f"  - Max workers: {MAX_WORKERS}")
            print(f"  - Input log: {ORIGINAL_LOG_FILE}")
            print(f"  - Output log: {RETRANSCODE_LOG_FILE}")
            sys.exit(0)
        else:
            print("Unknown argument. Use -h or --help for usage information.")
            sys.exit(1)
    
    main()
