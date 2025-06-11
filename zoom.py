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
from tqdm import tqdm

# Load environment variables from .env file
# Create a .env file in the same directory as this script with the following:
# ZOOM_ACCOUNT_A_ACCOUNT_ID=your_zoom_account_id_A
# ZOOM_ACCOUNT_A_CLIENT_ID=your_zoom_client_id_A
# ZOOM_ACCOUNT_A_CLIENT_SECRET=your_zoom_client_secret_A
# ZOOM_ACCOUNT_B_ACCOUNT_ID=your_zoom_account_id_B (Optional, for more accounts)
# ... and so on up to ZOOM_ACCOUNT_Z
load_dotenv()

# --- Configuration ---
ZOOM_ACCOUNTS_CONFIG = []

# Loop through potential Zoom accounts A to Z from environment variables
for i in range(1, 27): # ASCII for 'A' (65) to 'Z' (90)
    account_char = chr(64 + i) # Converts 1 to 'A', 2 to 'B', etc.
    account_prefix = f"ZOOM_ACCOUNT_{account_char}"
    account_id = os.getenv(f"{account_prefix}_ACCOUNT_ID")
    client_id = os.getenv(f"{account_prefix}_CLIENT_ID")
    client_secret = os.getenv(f"{account_prefix}_CLIENT_SECRET")

    # Only add an account to the config if all three credentials are provided
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
        # If the first account (Account_A) is not configured, print a warning
        if i == 1:
            print("WARNING: No Zoom account credentials found for ZOOM_ACCOUNT_A. "
                  "Please ensure ZOOM_ACCOUNT_A_ACCOUNT_ID, ZOOM_ACCOUNT_A_CLIENT_ID, "
                  "and ZOOM_ACCOUNT_A_CLIENT_SECRET are set in your .env file.")
        # Stop checking for more accounts if a consecutive one is missing
        break

DOWNLOAD_DIR = "zoom_downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True) # Ensure download directory exists

CSV_FILE = "meetings.csv" # The CSV file containing meeting IDs and desired filenames

# --- Logging Configuration ---
# Define separate log files for success and failure
SUCCESS_LOG = "success.log"
FAILURE_LOG = "failure.log"

# --- Concurrency Configuration ---
MAX_WORKERS = 5 # Number of concurrent threads to use for downloading meetings

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
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger

# Initialize distinct loggers for success and failure
success_logger = setup_logger(SUCCESS_LOG, "success_logger")
failure_logger = setup_logger(FAILURE_LOG, "failure_logger")

# --- Zoom API Helper Functions ---

def get_access_token(account_config):
    """
    Obtains a Zoom Server-to-Server OAuth access token for a given Zoom account.
    Access tokens are typically valid for 1 hour.
    """
    token_url = "https://zoom.us/oauth/token"
    client_id = account_config["client_id"]
    client_secret = account_config["client_secret"]
    account_id = account_config["account_id"]

    # Encode client ID and secret for Basic Authorization header
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
        # Make the POST request to Zoom's OAuth token endpoint
        response = requests.post(token_url, headers=headers, data=data)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        token_data = response.json()
        return token_data.get("access_token")
    except requests.exceptions.RequestException as e:
        # Log any request-related errors (connection, timeout, HTTP status errors)
        failure_logger.error(
            f"Failed to get access token for account '{account_config['name']}': {e}"
        )
        return None

def get_meeting_recordings(meeting_id, access_token, account_name):
    """
    Fetches recording details for a given Zoom meeting ID using the provided access token.
    Prioritizes MP4 files for download, then falls back to any available download URL.
    """
    api_url = f"https://api.zoom.us/v2/meetings/{meeting_id}/recordings"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        # Make the GET request to Zoom's recordings API
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        recording_data = response.json()

        if recording_data.get("recording_files"):
            # First, try to find an MP4 file specifically
            for record_file in recording_data["recording_files"]:
                if record_file["file_type"] == "MP4" and record_file["file_extension"] == "MP4":
                    return record_file["download_url"]
            # If no MP4 is found, return the download URL of any file that has one
            for record_file in recording_data["recording_files"]:
                if record_file.get("download_url"):
                    return record_file["download_url"]
        return None # No suitable downloadable recording found
    except requests.exceptions.HTTPError as e:
        # Handle specific HTTP error codes from Zoom API
        if e.response.status_code == 404:
            failure_logger.warning(
                f"Meeting ID {meeting_id} not found or no recordings for account '{account_name}'. "
                f"Error: {e.response.status_code} - {e.response.text}"
            )
        elif e.response.status_code == 401:
            failure_logger.error(
                f"Unauthorized access for Meeting ID {meeting_id} using account '{account_name}'. "
                f"Access token might be expired or invalid. Error: {e.response.status_code} - {e.response.text}"
            )
        else:
            failure_logger.error(
                f"API error fetching recordings for Meeting ID {meeting_id} with account '{account_name}': {e}"
            )
        return None
    except requests.exceptions.RequestException as e:
        # Log general network errors
        failure_logger.error(
            f"Network error fetching recordings for Meeting ID {meeting_id} with account '{account_name}': {e}"
        )
        return None

def download_file(url, destination_path, access_token, meeting_id, account_name):
    """
    Downloads a file from a given URL with authorization.
    Streams the content to handle potentially large recording files efficiently.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    try:
        # Use stream=True to download the file in chunks
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status() # Raise HTTPError for bad responses
            with open(destination_path, 'wb') as f:
                # Iterate over content in chunks and write to file
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk: # Filter out keep-alive new chunks
                        f.write(chunk)

        success_logger.info(
            f"Successfully downloaded Meeting ID {meeting_id} to '{destination_path}' using account '{account_name}'."
        )
        return True
    except requests.exceptions.RequestException as e:
        # Log any request-related errors during download
        failure_logger.error(
            f"Failed to download Meeting ID {meeting_id} from URL '{url}' to '{destination_path}' "
            f"using account '{account_name}': {e}"
        )
        return False

# --- Main Processing Logic for each meeting ---

def process_meeting_download(meeting_entry):
    """
    Processes a single meeting entry.
    Attempts to find and download the recording using configured Zoom accounts.
    Returns a dictionary with meeting ID, status, and an optional message.
    """
    meeting_id = meeting_entry["meeting_id"]
    desired_filename = meeting_entry["desired_filename"]

    # Ensure .mp4 extension for the output file
    if not os.path.splitext(desired_filename)[1]:
        desired_filename += ".mp4"

    output_path = os.path.join(DOWNLOAD_DIR, desired_filename)

    # Check if the file already exists on disk
    if os.path.exists(output_path):
        success_logger.info(
            f"Skipping Meeting ID {meeting_id}: File '{output_path}' already exists."
        )
        return {"meeting_id": meeting_id, "status": "downloaded", "message": "skipped - file exists"}

    found_download_url = None
    used_account_name = None
    current_access_token = None

    # Iterate through all configured Zoom accounts to find the meeting recording
    for account_config in ZOOM_ACCOUNTS_CONFIG:
        account_name = account_config["name"]
        current_access_token = get_access_token(account_config)

        if current_access_token:
            download_url = get_meeting_recordings(meeting_id, current_access_token, account_name)
            if download_url:
                found_download_url = download_url
                used_account_name = account_name
                break # Found the URL, stop trying other accounts

    if found_download_url:
        # If a download URL was found, proceed with downloading the file
        download_success = download_file(
            found_download_url, output_path, current_access_token, meeting_id, used_account_name
        )
        if download_success:
            return {"meeting_id": meeting_id, "status": "downloaded", "message": "downloaded successfully"}
        else:
            return {"meeting_id": meeting_id, "status": "failed", "message": "download error"}
    else:
        # If no download URL was found after trying all accounts
        failure_logger.error(
            f"Could not find a downloadable recording for Meeting ID {meeting_id} after trying all configured accounts."
        )
        return {"meeting_id": meeting_id, "status": "failed", "message": "no downloadable recording found"}

# --- Main Execution Flow ---

def main():
    print("Starting Zoom meeting download script...")
    print(f"Downloads will be saved to: {DOWNLOAD_DIR}")
    print(f"Success logs: {SUCCESS_LOG}")
    print(f"Failure logs: {FAILURE_LOG}")
    print("-" * 50)

    # Check if any Zoom accounts are configured before proceeding
    if not ZOOM_ACCOUNTS_CONFIG:
        failure_logger.critical("No Zoom accounts configured. Please set up ZOOM_ACCOUNT_X_ACCOUNT_ID, ZOOM_ACCOUNT_X_CLIENT_ID, and ZOOM_ACCOUNT_X_CLIENT_SECRET in your .env file.")
        print("Script cannot proceed without Zoom account configurations. Exiting.")
        return

    all_meetings_data = []
    csv_fieldnames = [] # To store the final fieldnames for writing the CSV

    try:
        # Open and read the CSV file
        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            current_headers = reader.fieldnames

            # Validate essential headers
            required_headers = ["Meeting ID", "File Name"]
            if not all(header in current_headers for header in required_headers):
                missing_headers = [h for h in required_headers if h not in current_headers]
                raise ValueError(
                    f"CSV must contain all required columns: {', '.join(required_headers)}. "
                    f"Missing: {', '.join(missing_headers)}. Found: {', '.join(current_headers)}"
                )

            # Determine the fieldnames for writing back, ensuring 'zoom_download_status' is included
            csv_fieldnames = list(current_headers) # Start with existing headers
            if "zoom_download_status" not in csv_fieldnames:
                csv_fieldnames.append("zoom_download_status")

            # Populate the list of all meetings from the CSV
            for row in reader:
                entry = {
                    "meeting_id": row["Meeting ID"].strip(),
                    "vimeo_uri": row.get("Vimeo URI", "").strip(), # Safely get Vimeo URI if it exists
                    "desired_filename": row["File Name"].strip(),
                    "zoom_download_status": row.get("zoom_download_status", "").strip(), # Get existing status or empty
                }
                all_meetings_data.append(entry)

    except FileNotFoundError:
        failure_logger.critical(f"CSV file '{CSV_FILE}' not found. Please create it with columns: Meeting ID, File Name (and optionally Vimeo URI).")
        print(f"Error: CSV file '{CSV_FILE}' not found. Exiting.")
        return
    except ValueError as e:
        failure_logger.critical(f"Error with CSV file '{CSV_FILE}': {e}")
        print(f"Error with CSV file: {e}. Exiting.")
        return
    except Exception as e:
        failure_logger.critical(f"An unexpected error occurred while reading CSV: {e}")
        print(f"An unexpected error occurred while reading CSV: {e}. Exiting.")
        return

    if not all_meetings_data:
        print("No valid meeting entries found in the CSV file. Exiting.")
        return

    meetings_to_process = []
    # Filter meetings that need processing
    for meeting_idx, meeting in enumerate(all_meetings_data):
        output_filename = meeting["desired_filename"]
        if not os.path.splitext(output_filename)[1]:
            output_filename += ".mp4"
        output_path = os.path.join(DOWNLOAD_DIR, output_filename)

        # A meeting needs processing if its status is not 'downloaded' OR the file doesn't exist on disk.
        # This covers cases where the CSV says 'downloaded' but the file was deleted.
        if meeting["zoom_download_status"] != "downloaded" or not os.path.exists(output_path):
            meetings_to_process.append(meeting)
        else:
            success_logger.info(f"Skipping Meeting ID {meeting['meeting_id']}: Already marked 'downloaded' and file '{output_path}' exists.")


    print(f"Found {len(all_meetings_data)} total meetings in CSV.")
    print(f"Processing {len(meetings_to_process)} meetings (excluding already downloaded ones).")
    print("-" * 50)

    if not meetings_to_process:
        print("All relevant meetings are already downloaded or marked as such. Nothing to process. Exiting.")
        return

    results = []
    # Use ThreadPoolExecutor to process meeting downloads concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # tqdm wraps the executor.map to provide a live progress bar in the console
        results = list(tqdm(executor.map(process_meeting_download, meetings_to_process),
                            total=len(meetings_to_process),
                            desc="Processing Meetings", # Description for the progress bar
                            unit="meeting", # Unit for the progress bar
                            ncols=100)) # Number of columns for the progress bar (adjust as needed)

    # Update all_meetings_data with results
    for result in results:
        meeting_id_to_update = result["meeting_id"]
        status = result["status"]
        message = result["message"] # For more detailed logging if needed

        # Find the original entry in all_meetings_data and update its status
        for meeting_entry in all_meetings_data:
            if meeting_entry["meeting_id"] == meeting_id_to_update:
                meeting_entry["zoom_download_status"] = status
                # You could add a message column too if desired, e.g., meeting_entry["last_run_message"] = message
                break

    # Write the updated data back to the CSV file
    try:
        with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=csv_fieldnames)
            writer.writeheader()
            for entry in all_meetings_data:
                # Create a row dictionary that matches the csv_fieldnames
                # This ensures all columns are present and in the correct order
                row_to_write = {header: entry.get(
                    # Map the internal dictionary keys to CSV headers
                    "meeting_id" if header == "Meeting ID" else
                    "vimeo_uri" if header == "Vimeo URI" else
                    "desired_filename" if header == "File Name" else
                    "zoom_download_status" if header == "zoom_download_status" else
                    header, # Fallback for any other header
                    "" # Default empty string if key not found
                ) for header in csv_fieldnames}
                writer.writerow(row_to_write)
        success_logger.info(f"Successfully updated '{CSV_FILE}' with download statuses.")
    except Exception as e:
        failure_logger.critical(f"Error writing back to CSV file '{CSV_FILE}': {e}")
        print(f"Error: Failed to write updated data to CSV: {e}")


    print("\n" + "-" * 50)
    print("All download tasks completed or attempted.")
    print(f"Check success log: {SUCCESS_LOG} for successfully downloaded files.")
    print(f"Check failure log: {FAILURE_LOG} for any issues or skipped meetings.")
    print("-" * 50)


if __name__ == "__main__":
    main()
