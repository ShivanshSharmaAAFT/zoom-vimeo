import requests
import concurrent.futures
import json
import os
import csv
import logging
import base64
from datetime import datetime
import time
from dotenv import load_dotenv
from tqdm import tqdm # Import tqdm for overall progress bar

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Dynamically load Zoom App credentials from environment variables
ZOOM_ACCOUNTS_CONFIG = []

for i in range(1, 27):
    account_char = chr(64 + i)
    account_prefix = f"ZOOM_ACCOUNT_{account_char}"
    account_id = os.getenv(f"{account_prefix}_ACCOUNT_ID")
    client_id = os.getenv(f"{account_prefix}_CLIENT_ID")
    client_secret = os.getenv(f"{account_prefix}_CLIENT_secret")

    if account_id and client_id and client_secret:
        ZOOM_ACCOUNTS_CONFIG.append(
            {
                "name": f"Account_{account_char}",
                "account_id": account_id,
                "client_id": client_id,
                "client_secret": client_secret,
            }
        )
    else:
        if i == 1:
            print("WARNING: No Zoom account credentials found in .env file. Please check your .env configuration.")
        break

DOWNLOAD_DIR = "zoom_downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

CSV_FILE = "meetings.csv"

SUCCESS_LOG = "success.log"
FAILURE_LOG = "failure.log"

MAX_WORKERS = 5 

# --- Logger Setup ---
def setup_logger(log_file, name, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.hasHandlers(): # Prevent duplicate handlers if called multiple times
        logger.handlers.clear()
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

success_logger = setup_logger(SUCCESS_LOG, "success_logger")
failure_logger = setup_logger(FAILURE_LOG, "failure_logger")

# --- Zoom API Helper Functions ---

def get_access_token(account_config):
    token_url = "https://zoom.us/oauth/token"
    client_id = account_config["client_id"]
    client_secret = account_config["client_secret"]
    account_id = account_config["account_id"]

    auth_string = f"{client_id}:{client_secret}".encode("utf-8")
    encoded_auth_string = base64.b64encode(auth_string).decode("utf-8")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_auth_string}",
    }
    data = {
        "grant_type": "account_credentials",
        "account_id": account_id,
    }

    try:
        response = requests.post(token_url, headers=headers, data=data)
        response.raise_for_status()
        token_data = response.json()
        return token_data.get("access_token")
    except requests.exceptions.RequestException as e:
        failure_logger.error(
            f"Failed to get access token for account '{account_config['name']}': {e}"
        )
        print(f"[ERROR] Failed to get access token for '{account_config['name']}'. See logs.")
        return None

def get_meeting_recordings(meeting_id, access_token, account_name):
    api_url = f"https://api.zoom.us/v2/meetings/{meeting_id}/recordings"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        recording_data = response.json()

        if recording_data.get("recording_files"):
            for record_file in recording_data["recording_files"]:
                if record_file["file_type"] == "MP4" and record_file["file_extension"] == "MP4":
                    return record_file["download_url"]
            for record_file in recording_data["recording_files"]:
                if record_file.get("download_url"):
                    return record_file["download_url"]
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            failure_logger.warning(
                f"Meeting ID {meeting_id} not found or no recordings for account '{account_name}'. "
                f"Error: {e.response.status_code} - {e.response.text}"
            )
            print(f"[WARN] Meeting {meeting_id} not found or no recordings for '{account_name}'. See logs.")
        elif e.response.status_code == 401:
             failure_logger.error(
                f"Unauthorized access for Meeting ID {meeting_id} using account '{account_name}'. "
                f"Access token might be expired or invalid. Error: {e.response.status_code} - {e.response.text}"
            )
             print(f"[ERROR] Unauthorized access for {meeting_id} with '{account_name}'. Token invalid. See logs.")
        else:
            failure_logger.error(
                f"API error fetching recordings for Meeting ID {meeting_id} with account '{account_name}': {e}"
            )
            print(f"[ERROR] API error for {meeting_id} ({account_name}). See logs.")
        return None
    except requests.exceptions.RequestException as e:
        failure_logger.error(
            f"Network error fetching recordings for Meeting ID {meeting_id} with account '{account_name}': {e}"
        )
        print(f"[ERROR] Network error for {meeting_id} ({account_name}). See logs.")
        return None

def download_file(url, destination_path, access_token, meeting_id, account_name):
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    try:
        # No tqdm progress bar for individual files to reduce clunkiness
        # Instead, we just download and report overall success/failure
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(destination_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
        success_logger.info(
            f"Successfully downloaded Meeting ID {meeting_id} to '{destination_path}' using account '{account_name}'."
        )
        print(f"[SUCCESS] Downloaded: {os.path.basename(destination_path)}")
        return True
    except requests.exceptions.RequestException as e:
        failure_logger.error(
            f"Failed to download Meeting ID {meeting_id} from URL '{url}' to '{destination_path}' "
            f"using account '{account_name}': {e}"
        )
        print(f"[FAILED] {os.path.basename(destination_path)} - {e}. See logs.")
        return False

# --- Main Processing Logic ---

def process_meeting_download(meeting_entry):
    meeting_id = meeting_entry["meeting_id"]
    desired_filename = meeting_entry["desired_filename"]
    
    if not os.path.splitext(desired_filename)[1]:
        desired_filename += ".mp4"

    output_path = os.path.join(DOWNLOAD_DIR, desired_filename)

    if os.path.exists(output_path):
        success_logger.info(
            f"Skipping Meeting ID {meeting_id}: File '{output_path}' already exists."
        )
        print(f"[SKIPPED] {desired_filename} (Already exists).")
        return True

    print(f"[INFO] Starting process for {desired_filename} (Meeting ID: {meeting_id})...")

    found_download_url = None
    used_account_name = None
    current_access_token = None

    for account_config in ZOOM_ACCOUNTS_CONFIG:
        account_name = account_config["name"]
        current_access_token = get_access_token(account_config)

        if current_access_token:
            download_url = get_meeting_recordings(meeting_id, current_access_token, account_name)
            if download_url:
                found_download_url = download_url
                used_account_name = account_name
                break

    if found_download_url:
        download_success = download_file(
            found_download_url, output_path, current_access_token, meeting_id, used_account_name
        )
        if not download_success:
            failure_logger.error(
                f"Final download attempt failed for Meeting ID {meeting_id} (download error)."
            )
            # Message already printed by download_file
            return False
        return True
    else:
        failure_logger.error(
            f"Could not find a downloadable recording for Meeting ID {meeting_id} after trying all configured accounts."
        )
        print(f"[ERROR] No download URL found for {desired_filename} after trying all accounts. See logs.")
        return False

# --- Main Application Entry Point ---
if __name__ == "__main__":
    print("Starting Zoom Meeting Downloader...")
    print(f"Downloads will be saved to: {DOWNLOAD_DIR}")
    print(f"Success logs: {SUCCESS_LOG}")
    print(f"Failure logs: {FAILURE_LOG}")
    print("-" * 50)

    if not ZOOM_ACCOUNTS_CONFIG:
        print("CRITICAL: No Zoom account credentials found in .env file. Exiting.")
        exit()

    meetings_to_download = []
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
                meetings_to_download.append(
                    {
                        "meeting_id": row["Meeting ID"].strip(),
                        "vimeo_uri": row["Vimeo URI"].strip(),
                        "desired_filename": row["File Name"].strip(),
                    }
                )
    except FileNotFoundError:
        print(f"CRITICAL: CSV file '{CSV_FILE}' not found. Please ensure it's in the same directory. Exiting.")
        exit()
    except ValueError as e:
        print(f"CRITICAL: Error with CSV file '{CSV_FILE}': {e}. Exiting.")
        exit()
    except Exception as e:
        print(f"CRITICAL: Error reading CSV file '{CSV_FILE}': {e}. Exiting.")
        exit()

    if not meetings_to_download:
        print("WARN: No valid meeting entries found in the CSV file for download. Exiting.")
        exit()

    print(f"Found {len(meetings_to_download)} meetings to process for download.")
    print("-" * 50)

    # Use tqdm for overall progress
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_meeting_download, meeting): meeting for meeting in meetings_to_download}
        
        # Wrap as_completed with tqdm for overall progress bar
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(meetings_to_download), desc="Overall Download Progress", unit="file", ncols=80):
            try:
                # Result is processed by process_meeting_download, just check for exceptions here
                future.result() 
            except Exception as exc:
                meeting_entry = futures[future]
                print(f"\n[ERROR] Meeting {meeting_entry.get('meeting_id')} generated an unhandled exception: {exc}. See logs.")
                failure_logger.error(f"Meeting {meeting_entry.get('meeting_id')} generated an unhandled exception: {exc}")
                
    print("\n" + "-" * 50)
    print("All download tasks completed or attempted.")
    print(f"Check success log: {SUCCESS_LOG}")
    print(f"Check failure log: {FAILURE_LOG}")
    print("-" * 50)
