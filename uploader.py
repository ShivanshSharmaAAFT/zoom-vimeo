import os
import csv
import logging
import re
from dotenv import load_dotenv
from tqdm import tqdm
import concurrent.futures
from vimeo import VimeoClient # This imports the VimeoClient from the PyVimeo library (vimeo package)

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
VIMEO_DEBUG_LOG = "vimeo_debug.log" # New debug log file

# Maximum number of concurrent uploads
MAX_UPLOAD_WORKERS = 3 

# --- Logger Setup ---
def setup_logger(log_file, name, level=logging.INFO, console_output=False):
    """Sets up a logger to write to a specific file, optionally also to console."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Clear existing handlers to prevent duplicate output
    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler (optional)
    if console_output:
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(message)s') # Simpler format for console
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger

vimeo_success_logger = setup_logger(VIMEO_SUCCESS_LOG, "vimeo_success_logger", level=logging.INFO)
vimeo_failure_logger = setup_logger(VIMEO_FAILURE_LOG, "vimeo_failure_logger", level=logging.ERROR)
vimeo_debug_logger = setup_logger(VIMEO_DEBUG_LOG, "vimeo_debug_logger", level=logging.DEBUG) # New debug logger

# --- Vimeo API Helper Functions ---
def extract_vimeo_folder_info_from_uri(vimeo_uri):
    """
    Extracts folder ID and optionally user/team ID from a Vimeo URI.
    Returns (folder_id, user_id, team_id). All can be None.
    """
    vimeo_debug_logger.debug(f"extract_vimeo_folder_info_from_uri called with URI: '{vimeo_uri}' (Type: {type(vimeo_uri)})")

    # Pattern for web folder URL: https://vimeo.com/manage/folders/{folder_id}
    match_web = re.search(r"vimeo\.com/manage/folders/(\d+)", vimeo_uri)
    if match_web:
        vimeo_debug_logger.debug(f"Matched web folder URL. Folder ID: {match_web.group(1)}")
        return (match_web.group(1), None, None) # Only folder_id, no user/team context needed for web URLs

    # Pattern for API URIs:
    # /users/{user_id}/albums/{album_id}
    # /me/albums/{album_id}
    # /users/{user_id}/projects/{project_id}
    # /me/projects/{project_id}
    # /teams/{team_id}/projects/{project_id}
    
    # Regex to capture context_type ('users', 'me', or 'teams'), context_id (optional),
    # folder_type ('albums' or 'projects'), and folder_id.
    # Group 1: context_type (users|me|teams)
    # Group 2: context_id (\d+) (optional, for users/teams)
    # Group 3: folder_type (albums|projects)
    # Group 4: folder_id (\d+)
    match_api = re.search(r"/(users|me|teams)(?:/(\d+))?/(albums|projects)/(\d+)", vimeo_uri)
    if match_api:
        context_type = match_api.group(1)
        context_id = match_api.group(2) # Will be None if 'me' is used
        folder_type = match_api.group(3)
        folder_id = match_api.group(4)

        vimeo_debug_logger.debug(f"Matched API URI pattern. Context Type: {context_type}, Context ID: {context_id}, Folder Type: {folder_type}, Folder ID: {folder_id}")

        user_id = None
        team_id = None
        if context_type == 'users':
            user_id = context_id
        elif context_type == 'teams':
            team_id = context_id
        # 'me' implies the authenticated user, so no explicit user_id needed in the path for API calls

        return (folder_id, user_id, team_id)
    
    vimeo_debug_logger.debug(f"No matching URI pattern found for '{vimeo_uri}'")
    return (None, None, None)

def upload_video_to_vimeo(file_path, file_name, vimeo_access_token, vimeo_folder_id=None, vimeo_user_id=None, vimeo_team_id=None):
    """
    Uploads a video file to Vimeo and optionally adds it to a specified folder (project/album).
    vimeo_user_id and vimeo_team_id are used to construct explicit API paths if necessary.
    """
    if not os.path.exists(file_path):
        vimeo_failure_logger.error(f"Local file not found for upload: '{file_path}'")
        return False, "Local file not found"

    client = VimeoClient(
        token=vimeo_access_token,
        key=None,
        secret=None
    )

    try:
        video_metadata = {
            'name': file_name,
            'privacy': {'view': 'unlisted'} # Unlisted allows sharing with link but not public search
            # If you want it public, use {'view': 'anybody'}
            # For password protection: {'view': 'password', 'password': 'your_password'}
        }
        
        with tqdm(total=os.path.getsize(file_path), unit='B', unit_scale=True, 
                  desc=f"Uploading {file_name}", ncols=100, leave=False) as pbar:
            def on_progress_callback(bytes_uploaded, total_bytes):
                # tqdm's update method expects the difference from the last update
                # So we calculate the delta and update
                pbar.update(bytes_uploaded - pbar.n) 

            vimeo_response = client.upload(file_path, data=video_metadata, on_progress=on_progress_callback)

        if not vimeo_response:
            vimeo_failure_logger.error(f"Failed to initiate Vimeo upload for '{file_name}'. Response: {vimeo_response}")
            return False, "Failed to initiate Vimeo upload"

        video_uri = vimeo_response
        video_id = video_uri.split('/')[-1]

        vimeo_success_logger.info(f"Successfully uploaded '{file_name}' to Vimeo. Video URI: {video_uri}")
        print(f"Uploaded '{file_name}' to Vimeo. Video URI: {video_uri}")

        # Step 2: Add video to specified folder (project/album) if an ID is provided
        if vimeo_folder_id:
            # Construct the API path based on whether user_id or team_id was provided
            add_to_folder_api_path = None
            if vimeo_user_id:
                # Use /users/{user_id}/projects/{project_id}/videos/{video_id}
                # Assuming 'projects' for team folders. If it's a personal album, it's /albums.
                # Since your CSV was /users/.../projects/, we'll stick to projects here.
                add_to_folder_api_path = f'/users/{vimeo_user_id}/projects/{vimeo_folder_id}/videos/{video_id}'
                vimeo_debug_logger.debug(f"Constructed user-specific project path: {add_to_folder_api_path}")
            elif vimeo_team_id:
                # Use /teams/{team_id}/projects/{project_id}/videos/{video_id}
                add_to_folder_api_path = f'/teams/{vimeo_team_id}/projects/{vimeo_folder_id}/videos/{video_id}'
                vimeo_debug_logger.debug(f"Constructed team-specific project path: {add_to_folder_api_path}")
            else:
                # Fallback to generic /projects/ if no user/team context is from URI
                # This is primarily for web UI folder URIs (e.g. vimeo.com/manage/folders/ID)
                add_to_folder_api_path = f'/projects/{vimeo_folder_id}/videos/{video_id}'
                vimeo_debug_logger.debug(f"Constructed generic project path (no explicit user/team ID): {add_to_folder_api_path}")

            if add_to_folder_api_path:
                try:
                    vimeo_debug_logger.debug(f"Attempting PUT request to add video to folder: {add_to_folder_api_path}")
                    add_to_folder_response = client.put(add_to_folder_api_path)
                    if add_to_folder_response.status_code == 204: # 204 No Content is success for PUT
                        vimeo_success_logger.info(f"Successfully added video {video_uri} to folder (project) {vimeo_folder_id}.")
                        print(f"Added video {file_name} to Vimeo folder (ID: {vimeo_folder_id}).")
                        return True, "Upload and folder addition successful"
                    else:
                        error_msg = (f"Failed to add video {video_uri} to folder {vimeo_folder_id}. "
                                    f"Status: {add_to_folder_response.status_code}, "
                                    f"Response: {add_to_folder_response.text}")
                        vimeo_failure_logger.error(error_msg)
                        return False, error_msg
                except Exception as folder_e:
                    vimeo_failure_logger.error(
                        f"Error adding video {video_uri} to folder {vimeo_folder_id}: {folder_e}"
                    )
                    return False, f"Error adding video to folder: {folder_e}"
            else:
                vimeo_failure_logger.error(f"Could not construct folder assignment API path for {file_name}.")
                return False, "Could not construct folder assignment API path."
        else:
            return True, "Upload successful, no folder specified (uploaded to root)"

    except Exception as e:
        vimeo_failure_logger.error(f"Error during Vimeo upload for '{file_name}': {e}")
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
        print(f"Skipping upload for {desired_filename}: Local file not found.")
        return

    # Extract folder info
    folder_id, user_id, team_id = extract_vimeo_folder_info_from_uri(vimeo_uri_from_csv)
    
    vimeo_debug_logger.debug(f"Result of extract_vimeo_folder_info_from_uri: Folder ID: '{folder_id}', User ID: '{user_id}', Team ID: '{team_id}'")

    if not folder_id: # Only care if folder_id is not found
        vimeo_failure_logger.warning(
            f"Invalid or unparseable Vimeo URI '{vimeo_uri_from_csv}' for '{desired_filename}'. "
            f"Could not extract a valid folder ID. Video will be uploaded to root."
        )
        print(f"Warning: Invalid Vimeo URI for {desired_filename}. Uploading to root.")

    print(f"Processing upload for '{desired_filename}' to Vimeo folder (ID: {folder_id or 'ROOT'}).")
    upload_success, message = upload_video_to_vimeo(
        local_file_path, desired_filename, VIMEO_ACCESS_TOKEN,
        vimeo_folder_id=folder_id, vimeo_user_id=user_id, vimeo_team_id=team_id
    )

    if not upload_success:
        print(f"Vimeo upload failed for '{desired_filename}'. Details in {VIMEO_FAILURE_LOG}.")

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
        return
    except ValueError as e:
        vimeo_failure_logger.critical(f"Error with CSV file '{CSV_FILE}': {e}")
        return
    except Exception as e:
        vimeo_failure_logger.critical(f"Error reading CSV file '{CSV_FILE}': {e}")
        return

    if not meetings_for_upload:
        print("No valid meeting entries found in the CSV file for upload. Exiting.")
        return

    print(f"Found {len(meetings_for_upload)} meetings to process for Vimeo upload.")
    print("-" * 50)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as executor:
        list(tqdm(executor.map(process_vimeo_upload, meetings_for_upload), 
                  total=len(meetings_for_upload), 
                  desc="Overall Vimeo Upload Processing", 
                  unit="video", 
                  ncols=100))

    print("\n" + "-" * 50)
    print("All Vimeo upload tasks completed or attempted.")
    print(f"Check success log: {VIMEO_SUCCESS_LOG}")
    print(f"Check failure log: {VIMEO_FAILURE_LOG}")
    print(f"Check debug log: {VIMEO_DEBUG_LOG}")
    print("-" * 50)

if __name__ == "__main__":
    main()
