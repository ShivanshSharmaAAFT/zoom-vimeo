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
VIMEO_SUCCESS_LOG = "vimeo_success.log"
VIMEO_FAILURE_LOG = "vimeo_failure.log"
MAX_UPLOAD_WORKERS = 3

# --- Logger Setup ---
def setup_logger(log_file, name, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

success_logger = setup_logger(VIMEO_SUCCESS_LOG, "vimeo_success_logger")
failure_logger = setup_logger(VIMEO_FAILURE_LOG, "vimeo_failure_logger")

# --- Vimeo API Helper Functions ---
def extract_vimeo_album_id_from_uri(vimeo_uri):
    # --- DEBUG ---
    print(f"  DEBUG: extract_vimeo_album_id_from_uri called with URI: '{vimeo_uri}' (Type: {type(vimeo_uri)})")
    # --- END DEBUG ---

    # Try matching web folder URL first
    match_web = re.search(r"vimeo\.com/manage/folders/(\d+)", vimeo_uri)
    if match_web:
        # --- DEBUG ---
        print(f"  DEBUG: Matched web folder URL. ID: {match_web.group(1)}")
        # --- END DEBUG ---
        return match_web.group(1)
    
    # Try matching API URI patterns (personal albums, personal projects, team projects)
    # This regex is designed to capture the last numeric ID after /albums/ or /projects/
    # and allows for /users/{user_id}/, /me/, or /teams/{team_id}/ prefixes
    match_api = re.search(r"/(?:users/\d+|me|teams/\d+)/(?:albums|projects)/(\d+)", vimeo_uri)
    if match_api:
        # --- DEBUG ---
        print(f"  DEBUG: Matched API URI pattern. ID: {match_api.group(1)}")
        # --- END DEBUG ---
        return match_api.group(1)
    
    # --- DEBUG ---
    print(f"  DEBUG: No matching URI pattern found for '{vimeo_uri}'")
    # --- END DEBUG ---
    return None

def upload_video_to_vimeo(file_path, file_name, vimeo_access_token, vimeo_folder_id=None):
    if not os.path.exists(file_path):
        failure_logger.error(f"Local file not found for upload: '{file_path}'")
        return False, "Local file not found"

    client = VimeoClient(
        token=vimeo_access_token,
        key=None,
        secret=None
    )

    try:
        video_metadata = {
            'name': file_name,
            'privacy': {'view': 'unlisted'}
        }
        
        with tqdm(total=os.path.getsize(file_path), unit='B', unit_scale=True, 
                  desc=f"Uploading {file_name}", ncols=100, leave=False) as pbar:
            def on_progress_callback(bytes_uploaded, total_bytes):
                pbar.update(bytes_uploaded - pbar.n) 

            vimeo_response = client.upload(file_path, data=video_metadata, on_progress=on_progress_callback)

        if not vimeo_response:
            failure_logger.error(f"Failed to initiate Vimeo upload for '{file_name}'. Response: {vimeo_response}")
            return False, "Failed to initiate Vimeo upload"

        video_uri = vimeo_response
        video_id = video_uri.split('/')[-1]

        success_logger.info(f"Successfully uploaded '{file_name}' to Vimeo. Video URI: {video_uri}")
        print(f"Uploaded '{file_name}' to Vimeo. Video URI: {video_uri}")

        if vimeo_folder_id:
            try:
                add_to_folder_response = client.put(f'/projects/{vimeo_folder_id}/videos/{video_id}')
                if add_to_folder_response.status_code == 204:
                    success_logger.info(f"Successfully added video {video_uri} to folder (project) {vimeo_folder_id}.")
                    print(f"Added video {file_name} to Vimeo folder (ID: {vimeo_folder_id}).")
                    return True, "Upload and folder addition successful"
                else:
                    error_msg = (f"Failed to add video {video_uri} to folder {vimeo_folder_id}. "
                                f"Status: {add_to_folder_response.status_code}, "
                                f"Response: {add_to_folder_response.text}")
                    failure_logger.error(error_msg)
                    return False, error_msg
            except Exception as folder_e:
                failure_logger.error(
                    f"Error adding video {video_uri} to folder {vimeo_folder_id}: {folder_e}"
                )
                return False, f"Error adding video to folder: {folder_e}"
        else:
            return True, "Upload successful, no folder specified (uploaded to root)"

    except Exception as e:
        failure_logger.error(f"Error during Vimeo upload for '{file_name}': {e}")
        return False, f"Vimeo upload failed: {e}"

# --- Main Processing Logic ---

def process_vimeo_upload(meeting_entry):
    meeting_id = meeting_entry["meeting_id"]
    vimeo_uri_from_csv = meeting_entry["vimeo_uri"]
    desired_filename = meeting_entry["desired_filename"]

    # --- DEBUG ---
    print(f"\n--- DEBUG for '{desired_filename}' ---")
    print(f"  DOWNLOAD_DIR: '{DOWNLOAD_DIR}'")
    print(f"  desired_filename from CSV: '{desired_filename}'")
    print(f"  Vimeo URI from CSV (RAW): '{vimeo_uri_from_csv}'")
    # --- END DEBUG ---

    local_file_path = os.path.join(DOWNLOAD_DIR, desired_filename)

    # --- DEBUG ---
    print(f"  Constructed local_file_path: '{local_file_path}'")
    file_exists_check = os.path.exists(local_file_path)
    print(f"  Does file exist at this path? {file_exists_check}")
    if not file_exists_check:
        print(f"  Listing contents of '{DOWNLOAD_DIR}':")
        try:
            dir_contents = os.listdir(DOWNLOAD_DIR)
            for item in dir_contents:
                print(f"    - {item}")
        except FileNotFoundError:
            print(f"  Error: {DOWNLOAD_DIR} directory itself not found.")
        except Exception as e:
            print(f"  Error listing directory contents: {e}")
    print(f"--- END DEBUG ---")


    if not file_exists_check:
        failure_logger.error(
            f"Skipping Vimeo upload for '{desired_filename}': Local file not found at '{local_file_path}'."
        )
        print(f"Skipping upload for {desired_filename}: Local file not found.")
        return

    vimeo_folder_id = extract_vimeo_album_id_from_uri(vimeo_uri_from_csv)
    
    # --- DEBUG ---
    print(f"  DEBUG: Result of extract_vimeo_album_id_from_uri: '{vimeo_folder_id}'")
    # --- END DEBUG ---

    if not vimeo_folder_id:
        failure_logger.warning(
            f"Invalid or unparseable Vimeo URI '{vimeo_uri_from_csv}' for '{desired_filename}'. "
            f"Could not extract folder ID. Video will be uploaded to root."
        )
        print(f"Warning: Invalid Vimeo URI for {desired_filename}. Uploading to root.")

    print(f"Processing upload for '{desired_filename}' to Vimeo folder (ID: {vimeo_folder_id or 'ROOT'}).")
    upload_success, message = upload_video_to_vimeo(
        local_file_path, desired_filename, VIMEO_ACCESS_TOKEN, vimeo_folder_id
    )

    if not upload_success:
        print(f"Vimeo upload failed for '{desired_filename}'. Details in {VIMEO_FAILURE_LOG}.")

# --- Main Execution ---

def main():
    print("Starting Vimeo upload script...")
    print(f"Reading files from: {DOWNLOAD_DIR}")
    print(f"Success logs: {VIMEO_SUCCESS_LOG}")
    print(f"Failure logs: {VIMEO_FAILURE_LOG}")
    print("-" * 50)

    if not VIMEO_ACCESS_TOKEN:
        failure_logger.critical("Vimeo Access Token not found in .env file. Please set VIMEO_ACCESS_TOKEN.")
        print("Script cannot proceed without Vimeo token. Exiting.")
        return

    meetings_for_upload = []
    try:
        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            
            # Define expected headers
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
        failure_logger.critical(f"CSV file '{CSV_FILE}' not found. Please ensure it's in the same directory.")
        return
    except ValueError as e:
        failure_logger.critical(f"Error with CSV file '{CSV_FILE}': {e}")
        return
    except Exception as e:
        failure_logger.critical(f"Error reading CSV file '{CSV_FILE}': {e}")
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
    print("-" * 50)

if __name__ == "__main__":
    main()
