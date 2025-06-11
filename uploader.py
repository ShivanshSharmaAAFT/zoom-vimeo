import os
import csv
import logging
import re
from datetime import datetime
from dotenv import load_dotenv
import concurrent.futures
from vimeo import VimeoClient

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
DOWNLOAD_DIR = "zoom_downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
CSV_FILE = "meetings.csv"
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")

# Log file paths
VIMEO_SUCCESS_LOG = "vimeo_success.log"
VIMEO_FAILURE_LOG = "vimeo_failure.log"
VIMEO_DEBUG_LOG = "vimeo_debug.log"

# Maximum number of concurrent uploads
MAX_UPLOAD_WORKERS = 3

# --- Logger Setup ---
def setup_logger(log_file, name, level=logging.INFO):
    """Sets up a logger to write to a specific file."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    return logger

vimeo_success_logger = setup_logger(VIMEO_SUCCESS_LOG, "vimeo_success_logger", level=logging.INFO)
vimeo_failure_logger = setup_logger(VIMEO_FAILURE_LOG, "vimeo_failure_logger", level=logging.ERROR)
vimeo_debug_logger = setup_logger(VIMEO_DEBUG_LOG, "vimeo_debug_logger", level=logging.DEBUG)

# --- Vimeo API Helper Functions ---
def extract_vimeo_folder_info_from_uri(vimeo_uri):
    vimeo_debug_logger.debug(f"extract_vimeo_folder_info_from_uri called with URI: '{vimeo_uri}' (Type: {type(vimeo_uri)})")

    match_web = re.search(r"vimeo\.com/manage/folders/(\d+)", vimeo_uri)
    if match_web:
        vimeo_debug_logger.debug(f"Matched web folder URL. Folder ID: {match_web.group(1)}")
        return (match_web.group(1), None, None)

    match_api = re.search(r"/(users|me|teams)(?:/(\d+))?/(albums|projects)/(\d+)", vimeo_uri)
    if match_api:
        context_type = match_api.group(1)
        context_id = match_api.group(2)
        folder_id = match_api.group(4)

        vimeo_debug_logger.debug(f"Matched API URI pattern. Context Type: {context_type}, Context ID: {context_id}, Folder ID: {folder_id}")

        user_id = None
        team_id = None
        if context_type == 'users':
            user_id = context_id
        elif context_type == 'teams':
            team_id = context_id

        return (folder_id, user_id, team_id)
    
    vimeo_debug_logger.debug(f"No matching URI pattern found for '{vimeo_uri}'")
    return (None, None, None)

def upload_video_to_vimeo(file_path, file_name, vimeo_access_token, vimeo_folder_id=None, vimeo_user_id=None, vimeo_team_id=None):
    """
    Uploads a video file to Vimeo and optionally adds it to a specified folder (project/album).
    Only prints a message once the upload and folder assignment are complete or an error occurs.
    """
    if not os.path.exists(file_path):
        vimeo_failure_logger.error(f"Local file not found for upload: '{file_path}'")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Local file not found for '{file_name}'. Skipping.")
        return False, "Local file not found"

    client = VimeoClient(
        token=vimeo_access_token,
        key=None,
        secret=None
    )

    try:
        video_metadata = {
            'name': file_name,
            'privacy': {'view': 'anybody'}
        }
        
        # Callback function for progress updates - now does nothing to keep console clean
        def on_progress_callback(bytes_uploaded, total_bytes):
            pass # Do nothing during progress to keep console clean

        # No "Starting upload" message here, it will be handled by process_vimeo_upload
        vimeo_response = client.upload(file_path, data=video_metadata, on_progress=on_progress_callback)

        if not vimeo_response:
            vimeo_failure_logger.error(f"Failed to initiate Vimeo upload for '{file_name}'. Response: {vimeo_response}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Failed to initiate Vimeo upload for '{file_name}'. See logs.")
            return False, "Failed to initiate Vimeo upload"

        video_uri = vimeo_response
        video_id = video_uri.split('/')[-1]

        vimeo_success_logger.info(f"Successfully uploaded '{file_name}' to Vimeo. Video URI: {video_uri}")
        # print(f"[{datetime.now().strftime('%H:%M:%S')}] Successfully uploaded '{file_name}' to Vimeo. Video URI: {video_uri}")
        # This print moved to the end after folder assignment or if no folder specified

        if vimeo_folder_id:
            add_to_folder_api_path = None
            if vimeo_user_id:
                add_to_folder_api_path = f'/users/{vimeo_user_id}/projects/{vimeo_folder_id}/videos/{video_id}'
            elif vimeo_team_id:
                add_to_folder_api_path = f'/teams/{vimeo_team_id}/projects/{vimeo_folder_id}/videos/{video_id}'
            else:
                add_to_folder_api_path = f'/projects/{vimeo_folder_id}/videos/{video_id}'

            if add_to_folder_api_path:
                try:
                    vimeo_debug_logger.debug(f"Attempting PUT request to add video to folder: {add_to_folder_api_path}")
                    add_to_folder_response = client.put(add_to_folder_api_path)
                    if add_to_folder_response.status_code == 204:
                        vimeo_success_logger.info(f"Successfully added video {video_uri} to folder (project) {vimeo_folder_id}.")
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] SUCCESS: Uploaded '{file_name}' and added to folder (ID: {vimeo_folder_id}).")
                        return True, "Upload and folder addition successful"
                    else:
                        error_msg = (f"Failed to add video {video_uri} to folder {vimeo_folder_id}. "
                                     f"Status: {add_to_folder_response.status_code}, "
                                     f"Response: {add_to_folder_response.text}")
                        vimeo_failure_logger.error(error_msg)
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Failed to add '{file_name}' to folder. See logs.")
                        return False, error_msg
                except Exception as folder_e:
                    vimeo_failure_logger.error(
                        f"Error adding video {video_uri} to folder {vimeo_folder_id}: {folder_e}"
                    )
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Error adding '{file_name}' to folder. See logs.")
                    return False, f"Error adding video to folder: {folder_e}"
            else:
                vimeo_failure_logger.error(f"Could not construct folder assignment API path for {file_name}.")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Could not construct folder path for '{file_name}'.")
                return False, "Could not construct folder assignment API path."
        else:
            # If no folder was specified, print success after upload.
            print(f"[{datetime.now().strftime('%H:%M:%S')}] SUCCESS: Uploaded '{file_name}' to Vimeo root.")
            return True, "Upload successful, no folder specified (uploaded to root)"

    except Exception as e:
        vimeo_failure_logger.error(f"Error during Vimeo upload for '{file_name}': {e}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: During upload for '{file_name}': {e}. See logs.")
        return False, f"Vimeo upload failed: {e}"

# --- Main Processing Logic ---

def process_vimeo_upload(meeting_entry):
    meeting_id = meeting_entry["meeting_id"]
    vimeo_uri_from_csv = meeting_entry["vimeo_uri"]
    desired_filename = meeting_entry["desired_filename"]

    vimeo_debug_logger.debug(f"\n--- Processing '{desired_filename}' ---")
    vimeo_debug_logger.debug(f"  Vimeo URI from CSV (RAW): '{vimeo_uri_from_csv}'")

    local_file_path = os.path.join(DOWNLOAD_DIR, desired_filename)

    vimeo_debug_logger.debug(f"  Constructed local_file_path: '{local_file_path}'")
    file_exists_check = os.path.exists(local_file_path)
    vimeo_debug_logger.debug(f"  Does file exist at this path? {file_exists_check}")

    if not file_exists_check:
        vimeo_failure_logger.error(
            f"Skipping Vimeo upload for '{desired_filename}': Local file not found at '{local_file_path}'."
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Skipping upload for '{desired_filename}': Local file not found.")
        return

    folder_id, user_id, team_id = extract_vimeo_folder_info_from_uri(vimeo_uri_from_csv)
    
    vimeo_debug_logger.debug(f"Result of extract_vimeo_folder_info_from_uri: Folder ID: '{folder_id}', User ID: '{user_id}', Team ID: '{team_id}'")

    if not folder_id:
        vimeo_failure_logger.warning(
            f"Invalid or unparseable Vimeo URI '{vimeo_uri_from_csv}' for '{desired_filename}'. "
            f"Could not extract a valid folder ID. Video will be uploaded to root."
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Warning: Invalid Vimeo URI for '{desired_filename}'. Uploading to root.")

    # You could add a "Starting upload" message here if desired for clarity
    # print(f"[{datetime.now().strftime('%H:%M:%S')}] Attempting upload for '{desired_filename}'...")

    upload_success, message = upload_video_to_vimeo(
        local_file_path, desired_filename, VIMEO_ACCESS_TOKEN,
        vimeo_folder_id=folder_id, vimeo_user_id=user_id, vimeo_team_id=team_id
    )

    if not upload_success:
        pass # Error messages are already printed within upload_video_to_vimeo

# --- Main Execution ---

def main():
    print("Starting Vimeo upload script...")
    print(f"Downloads will be read from: {DOWNLOAD_DIR}")
    print(f"Success logs: {VIMEO_SUCCESS_LOG}")
    print(f"Failure logs: {VIMEO_FAILURE_LOG}")
    print(f"Debug logs: {VIMEO_DEBUG_LOG}")
    print("-" * 50)

    if not VIMEO_ACCESS_TOKEN:
        vimeo_failure_logger.critical("Vimeo Access Token not found in .env file. Please set VIMEO_ACCESS_TOKEN.")
        print("Script cannot proceed without Vimeo token. Exiting.")
        return

    meetings_for_upload = []
    try:
        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            
            expected_headers = ["Meeting ID", "Vimeo URI", "File Name"]
            
            if not all(header in reader.fieldnames for header in expected_headers):
                raise ValueError(
                    f"CSV must contain all required columns: {', '.join(expected_headers)}. "
                    f"Found: {', '.join(reader.fieldnames)}"
                )

            for row in reader:
                meetings_for_upload.append(
                    {
                        "meeting_id": row["Meeting ID"].strip(),
                        "vimeo_uri": row["Vimeo URI"].strip(),
                        "desired_filename": row["File Name"].strip(),
                    }
                )
    except FileNotFoundError:
        vimeo_failure_logger.critical(f"CSV file '{CSV_FILE}' not found. Please ensure it's in the same directory.")
        print(f"Error: CSV file '{CSV_FILE}' not found. Exiting.")
        return
    except ValueError as e:
        vimeo_failure_logger.critical(f"Error with CSV file '{CSV_FILE}': {e}")
        print(f"Error with CSV file: {e}. Exiting.")
        return
    except Exception as e:
        vimeo_failure_logger.critical(f"Error reading CSV file '{CSV_FILE}': {e}")
        print(f"An unexpected error occurred while reading CSV: {e}. Exiting.")
        return

    if not meetings_for_upload:
        print("No valid meeting entries found in the CSV file for upload. Exiting.")
        return

    print(f"Found {len(meetings_for_upload)} meetings to process for Vimeo upload.")
    print("-" * 50)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as executor:
        list(executor.map(process_vimeo_upload, meetings_for_upload))

    print("\n" + "-" * 50)
    print("All Vimeo upload tasks completed or attempted.")
    print(f"Check success log: {VIMEO_SUCCESS_LOG} for successfully uploaded files.")
    print(f"Check failure log: {VIMEO_FAILURE_LOG} for any issues or skipped uploads.")
    print(f"Check debug log: {VIMEO_DEBUG_LOG} for detailed processing information.")
    print("-" * 50)

if __name__ == "__main__":
    main()
