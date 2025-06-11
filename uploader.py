import concurrent.futures
import os
import csv
import logging
import re
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables from .env file
# Create a .env file in the same directory as this script with the following:
# VIMEO_ACCESS_TOKEN=your_vimeo_access_token
load_dotenv()

# --- Configuration ---
DOWNLOAD_DIR = "zoom_downloads" # Directory where downloaded Zoom files are expected to be
os.makedirs(DOWNLOAD_DIR, exist_ok=True) # Ensure download directory exists (even if only for reading)

CSV_FILE = "meetings.csv" # The CSV file containing meeting IDs, desired filenames, and Vimeo URIs

# Vimeo configuration
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")

# --- Logging Configuration ---
# Log file paths for Vimeo uploads
VIMEO_SUCCESS_LOG = "vimeo_success.log"
VIMEO_FAILURE_LOG = "vimeo_failure.log"
VIMEO_DEBUG_LOG = "vimeo_debug.log" # Added for Vimeo debug logging

# --- Concurrency Configuration ---
MAX_UPLOAD_WORKERS = 3 # Maximum number of concurrent uploads for Vimeo

# --- Logger Setup ---
def setup_logger(log_file, name, level=logging.INFO):
    """
    Sets up a logger to write to a specific file.
    It also adds a StreamHandler to output logs to the console.
    """
    # Create a file handler for the specific log file
    handler = logging.FileHandler(log_file)
    # Define the log message format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Get a logger instance by name
    logger = logging.getLogger(name)
    logger.setLevel(level) # Set the minimum logging level

    # Clear existing handlers to prevent duplicate logging if called multiple times
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    # Add the file handler to the logger
    logger.addHandler(handler)

    # Add a stream handler to also output logs to the console
    # Ensure not to add multiple stream handlers to the same logger
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger

# Initialize distinct loggers for Vimeo upload success, failure, and debug
vimeo_success_logger = setup_logger(VIMEO_SUCCESS_LOG, "vimeo_success_logger", level=logging.INFO)
vimeo_failure_logger = setup_logger(VIMEO_FAILURE_LOG, "vimeo_failure_logger", level=logging.ERROR)
vimeo_debug_logger = setup_logger(VIMEO_DEBUG_LOG, "vimeo_debug_logger", level=logging.DEBUG)

# --- Vimeo API Helper Functions ---
# Import VimeoClient here to avoid circular dependencies or only import if Vimeo is used
try:
    from vimeo import VimeoClient
except ImportError:
    vimeo_failure_logger.critical("VimeoClient not found. Please install the 'vimeo' library: pip install vimeo")
    VimeoClient = None # Set to None if import fails to prevent further errors


def extract_vimeo_folder_info_from_uri(vimeo_uri):
    """
    Extracts Vimeo folder ID, user ID, and team ID from a Vimeo URI.
    Supports both web (vimeo.com/manage/folders/ID) and API (users/ID/projects/ID) formats.
    """
    if not isinstance(vimeo_uri, str):
        vimeo_debug_logger.debug(f"Input vimeo_uri is not a string: {vimeo_uri} (Type: {type(vimeo_uri)})")
        return (None, None, None)

    vimeo_debug_logger.debug(f"extract_vimeo_folder_info_from_uri called with URI: '{vimeo_uri}'")

    # Match web folder URL: vimeo.com/manage/folders/(\d+)
    match_web = re.search(r"vimeo\.com/manage/folders/(\d+)", vimeo_uri)
    if match_web:
        vimeo_debug_logger.debug(f"Matched web folder URL. Folder ID: {match_web.group(1)}")
        return (match_web.group(1), None, None)

    # Match API URI patterns: /(users|me|teams)(?:/(\d+))?/(albums|projects)/(\d+)
    # Note: 'me' is typically used for the authenticated user, so context_id might be None for 'me'
    match_api = re.search(r"/(users|me|teams)(?:/(\d+))?/(albums|projects)/(\d+)", vimeo_uri)
    if match_api:
        context_type = match_api.group(1)
        context_id = match_api.group(2) # Will be None for 'me'
        folder_type = match_api.group(3) # albums or projects
        folder_id = match_api.group(4)

        vimeo_debug_logger.debug(f"Matched API URI pattern. Context Type: {context_type}, Context ID: {context_id}, Folder ID: {folder_id}, Folder Type: {folder_type}")

        user_id = None
        team_id = None
        if context_type == 'users':
            user_id = context_id
        elif context_type == 'teams':
            team_id = context_id
        # 'me' context does not require a specific user_id/team_id in the path for projects/albums
        # if context_type == 'me', both user_id and team_id remain None

        return (folder_id, user_id, team_id)

    vimeo_debug_logger.debug(f"No matching URI pattern found for '{vimeo_uri}'")
    return (None, None, None)

def upload_video_to_vimeo(file_path, file_name, vimeo_access_token, vimeo_folder_id=None, vimeo_user_id=None, vimeo_team_id=None):
    """
    Uploads a video file to Vimeo and optionally adds it to a specified folder (project/album).
    Returns a tuple (success: bool, message: str, video_uri: str or None).
    """
    if not VimeoClient:
        return False, "VimeoClient library not installed.", None

    if not os.path.exists(file_path):
        vimeo_failure_logger.error(f"Local file not found for upload: '{file_path}'")
        return False, "Local file not found", None

    client = VimeoClient(
        token=vimeo_access_token,
        key=None, # Not needed for authenticated client
        secret=None # Not needed for authenticated client
    )

    try:
        video_metadata = {
            'name': file_name,
            'privacy': {'view': 'anybody'} # Default privacy setting
        }

        # Callback function for progress updates - does nothing to keep console clean
        def on_progress_callback(bytes_uploaded, total_bytes):
            pass

        vimeo_debug_logger.debug(f"Starting Vimeo upload for '{file_name}' from '{file_path}'...")
        vimeo_response_uri = client.upload(file_path, data=video_metadata, on_progress=on_progress_callback)

        if not vimeo_response_uri:
            vimeo_failure_logger.error(f"Failed to initiate Vimeo upload for '{file_name}'. No URI returned.")
            return False, "Failed to initiate Vimeo upload (no URI returned)", None

        video_uri = vimeo_response_uri
        video_id = video_uri.split('/')[-1]

        vimeo_success_logger.info(f"Successfully uploaded '{file_name}' to Vimeo. Video URI: {video_uri}")

        # If a folder ID is provided, attempt to add the video to it
        if vimeo_folder_id:
            add_to_folder_api_path = None
            if vimeo_user_id: # For user-owned projects
                add_to_folder_api_path = f'/users/{vimeo_user_id}/projects/{vimeo_folder_id}/videos/{video_id}'
            elif vimeo_team_id: # For team-owned projects
                add_to_folder_api_path = f'/teams/{vimeo_team_id}/projects/{vimeo_folder_id}/videos/{video_id}'
            else: # Assume it's a project under the authenticated user ('me') if no explicit user/team ID
                add_to_folder_api_path = f'/me/projects/{vimeo_folder_id}/videos/{video_id}'

            if add_to_folder_api_path:
                try:
                    vimeo_debug_logger.debug(f"Attempting PUT request to add video to folder: {add_to_folder_api_path}")
                    add_to_folder_response = client.put(add_to_folder_api_path)
                    if add_to_folder_response.status_code == 204: # 204 No Content indicates success for PUT
                        vimeo_success_logger.info(f"Successfully added video {video_uri} to folder (project) {vimeo_folder_id}.")
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] SUCCESS: Uploaded '{file_name}' and added to folder (ID: {vimeo_folder_id}).")
                        return True, "Upload and folder addition successful", video_uri
                    else:
                        error_msg = (f"Failed to add video {video_uri} to folder {vimeo_folder_id}. "
                                     f"Status: {add_to_folder_response.status_code}, "
                                     f"Response: {add_to_folder_response.text}")
                        vimeo_failure_logger.error(error_msg)
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Failed to add '{file_name}' to folder. See logs.")
                        return False, error_msg, video_uri
                except Exception as folder_e:
                    vimeo_failure_logger.error(
                        f"Error adding video {video_uri} to folder {vimeo_folder_id}: {folder_e}"
                    )
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Error adding '{file_name}' to folder. See logs.")
                    return False, f"Error adding video to folder: {folder_e}", video_uri
            else:
                vimeo_failure_logger.error(f"Could not construct folder assignment API path for {file_name}.")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Could not construct folder path for '{file_name}'. Uploaded to root.")
                # Even if folder assignment fails, the upload itself might be successful
                return False, "Could not construct folder assignment API path, uploaded to root", video_uri
        else:
            # If no folder was specified, print success after upload.
            print(f"[{datetime.now().strftime('%H:%M:%S')}] SUCCESS: Uploaded '{file_name}' to Vimeo root.")
            return True, "Upload successful, no folder specified (uploaded to root)", video_uri

    except Exception as e:
        vimeo_failure_logger.error(f"Error during Vimeo upload for '{file_name}': {e}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: During upload for '{file_name}': {e}. See logs.")
        return False, f"Vimeo upload failed: {e}", None

# --- Main Processing Logic for each meeting (Vimeo Upload) ---

def process_vimeo_upload(meeting_entry):
    """
    Processes a single meeting entry for Vimeo upload.
    Returns a dictionary with meeting ID, upload status, and an optional message.
    """
    meeting_id = meeting_entry["meeting_id"]
    vimeo_uri_from_csv = meeting_entry["vimeo_uri"]
    desired_filename = meeting_entry["desired_filename"]

    # Ensure .mp4 extension for the output file (assuming the local file is mp4)
    if not os.path.splitext(desired_filename)[1]:
        desired_filename += ".mp4"

    local_file_path = os.path.join(DOWNLOAD_DIR, desired_filename)

    # Check if the file already exists locally and if the status is already 'uploaded'
    # This acts as the main skipping mechanism for the uploader
    if meeting_entry.get("vimeo_upload_status") == "uploaded" and os.path.exists(local_file_path):
        vimeo_success_logger.info(
            f"Skipping Vimeo upload for Meeting ID {meeting_id} ('{desired_filename}'): "
            f"Already marked 'uploaded' and local file exists."
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Skipping upload for '{desired_filename}': Already uploaded.")
        return {"meeting_id": meeting_id, "upload_status": "uploaded", "vimeo_uri": meeting_entry.get("vimeo_uri"), "message": "skipped - already uploaded"}
    
    if not os.path.exists(local_file_path):
        vimeo_failure_logger.error(
            f"Skipping Vimeo upload for '{desired_filename}': Local file not found at '{local_file_path}'."
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Skipping upload for '{desired_filename}': Local file not found.")
        return {"meeting_id": meeting_id, "upload_status": "failed", "vimeo_uri": None, "message": "skipped - local file not found"}

    # Extract folder info from Vimeo URI
    folder_id, user_id, team_id = extract_vimeo_folder_info_from_uri(vimeo_uri_from_csv)

    if not folder_id and vimeo_uri_from_csv: # Only warn if a URI was provided but couldn't be parsed
        vimeo_failure_logger.warning(
            f"Invalid or unparseable Vimeo URI '{vimeo_uri_from_csv}' for '{desired_filename}'. "
            f"Could not extract a valid folder ID. Video will be uploaded to root."
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Warning: Invalid Vimeo URI for '{desired_filename}'. Uploading to root.")

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Attempting upload for '{desired_filename}'...")
    upload_success, message, new_vimeo_uri = upload_video_to_vimeo(
        local_file_path, desired_filename, VIMEO_ACCESS_TOKEN,
        vimeo_folder_id=folder_id, vimeo_user_id=user_id, vimeo_team_id=team_id
    )

    if upload_success:
        return {"meeting_id": meeting_id, "upload_status": "uploaded", "vimeo_uri": new_vimeo_uri or vimeo_uri_from_csv, "message": message}
    else:
        # If upload failed, retain original Vimeo URI if exists, otherwise None
        return {"meeting_id": meeting_id, "upload_status": "failed", "vimeo_uri": vimeo_uri_from_csv, "message": message}


# --- Main Execution Flow ---

def main():
    print("Starting Vimeo upload script...")
    print(f"Videos will be read from: {DOWNLOAD_DIR}")
    print(f"Vimeo Success logs: {VIMEO_SUCCESS_LOG}")
    print(f"Vimeo Failure logs: {VIMEO_FAILURE_LOG}")
    print(f"Vimeo Debug logs: {VIMEO_DEBUG_LOG}")
    print("-" * 50)

    if not VIMEO_ACCESS_TOKEN:
        vimeo_failure_logger.critical("Vimeo Access Token not found in .env file. Please set VIMEO_ACCESS_TOKEN.")
        print("Script cannot proceed with Vimeo uploads without token. Exiting.")
        return

    all_meetings_data = []
    csv_fieldnames = [] # To store the final fieldnames for writing the CSV

    try:
        # Open and read the CSV file
        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            current_headers = reader.fieldnames

            # Validate essential headers
            required_headers = ["Meeting ID", "File Name", "Vimeo URI"] # Vimeo URI is essential for upload
            if not all(header in current_headers for header in required_headers):
                missing_headers = [h for h in required_headers if h not in current_headers]
                raise ValueError(
                    f"CSV must contain all required columns: {', '.join(required_headers)}. "
                    f"Missing: {', '.join(missing_headers)}. Found: {', '.join(current_headers)}"
                )

            # Determine the fieldnames for writing back, ensuring status columns are included
            csv_fieldnames = list(current_headers) # Start with existing headers
            # Removed 'zoom_download_status' from required fieldnames for writing, as it's not managed here
            if "vimeo_upload_status" not in csv_fieldnames:
                csv_fieldnames.append("vimeo_upload_status")


            # Populate the list of all meetings from the CSV
            for row in reader:
                entry = {
                    "meeting_id": row["Meeting ID"].strip(),
                    "vimeo_uri": row.get("Vimeo URI", "").strip(), # Safely get Vimeo URI
                    "desired_filename": row["File Name"].strip(),
                    # We still read zoom_download_status if it exists, but don't actively use it for filtering uploads
                    "zoom_download_status": row.get("zoom_download_status", "").strip(), 
                    "vimeo_upload_status": row.get("vimeo_upload_status", "").strip(), # Get existing status or empty
                }
                all_meetings_data.append(entry)

    except FileNotFoundError:
        vimeo_failure_logger.critical(f"CSV file '{CSV_FILE}' not found. Please create it with columns: Meeting ID, File Name, Vimeo URI.")
        print(f"Error: CSV file '{CSV_FILE}' not found. Exiting.")
        return
    except ValueError as e:
        vimeo_failure_logger.critical(f"Error with CSV file '{CSV_FILE}': {e}")
        print(f"Error with CSV file: {e}. Exiting.")
        return
    except Exception as e:
        vimeo_failure_logger.critical(f"An unexpected error occurred while reading CSV: {e}")
        print(f"An unexpected error occurred while reading CSV: {e}. Exiting.")
        return

    if not all_meetings_data:
        print("No valid meeting entries found in the CSV file. Exiting.")
        return

    # --- Process Vimeo Uploads ---
    meetings_for_upload = []
    for meeting in all_meetings_data:
        output_filename = meeting["desired_filename"]
        if not os.path.splitext(output_filename)[1]:
            output_filename += ".mp4"
        local_file_path = os.path.join(DOWNLOAD_DIR, output_filename)

        # Consider for upload if local file exists AND vimeo_upload_status is not 'uploaded'
        if os.path.exists(local_file_path):
            if meeting["vimeo_upload_status"] != "uploaded" :
                meetings_for_upload.append(meeting)
            else:
                vimeo_success_logger.info(f"Skipping Vimeo upload for Meeting ID {meeting['meeting_id']}: Already marked 'uploaded'.")
        else:
            vimeo_failure_logger.warning(f"Skipping Vimeo upload for Meeting ID {meeting['meeting_id']}: Local file '{local_file_path}' not found. Cannot upload.")


    if meetings_for_upload:
        print(f"\n--- Starting Vimeo Uploads ({len(meetings_for_upload)} meetings) ---")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as executor:
            upload_results = list(tqdm(executor.map(process_vimeo_upload, meetings_for_upload),
                                        total=len(meetings_for_upload),
                                        desc="Uploading to Vimeo",
                                        unit="video",
                                        ncols=100))

        # Update all_meetings_data with upload results
        for result in upload_results:
            meeting_id_to_update = result["meeting_id"]
            status = result["upload_status"]
            vimeo_uri = result.get("vimeo_uri") # Get updated Vimeo URI if returned

            for meeting_entry in all_meetings_data:
                if meeting_entry["meeting_id"] == meeting_id_to_update:
                    meeting_entry["vimeo_upload_status"] = status
                    if vimeo_uri: # Update Vimeo URI if a new one was returned (e.g., successful upload)
                        meeting_entry["vimeo_uri"] = vimeo_uri
                    break
    else:
        print("\n--- No Vimeo uploads needed or possible at this time. ---")


    # Write the updated data back to the CSV file (only upload statuses are actively managed here)
    try:
        with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=csv_fieldnames)
            writer.writeheader()
            for entry in all_meetings_data:
                row_to_write = {header: entry.get(
                    "meeting_id" if header == "Meeting ID" else
                    "vimeo_uri" if header == "Vimeo URI" else
                    "desired_filename" if header == "File Name" else
                    "zoom_download_status" if header == "zoom_download_status" else # Preserve if it exists
                    "vimeo_upload_status" if header == "vimeo_upload_status" else
                    header, # Fallback for any other header
                    "" # Default empty string if key not found
                ) for header in csv_fieldnames}
                writer.writerow(row_to_write)
        vimeo_success_logger.info(f"Successfully updated '{CSV_FILE}' with Vimeo upload statuses.")
    except Exception as e:
        vimeo_failure_logger.critical(f"Error writing back to CSV file '{CSV_FILE}': {e}")
        print(f"Error: Failed to write updated data to CSV: {e}")


    print("\n" + "-" * 50)
    print("All Vimeo upload tasks completed or attempted.")
    print(f"Check success log: {VIMEO_SUCCESS_LOG} for successfully processed files.")
    print(f"Check failure log: {VIMEO_FAILURE_LOG} for any issues or skipped uploads.")
    print("-" * 50)


if __name__ == "__main__":
    main()
