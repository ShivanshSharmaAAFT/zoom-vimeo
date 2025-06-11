import requests
import concurrent.futures
import json
import os
import csv
import logging
import base64
from datetime import datetime
import time
from dotenv import load_dotenv # Import load_dotenv
from tqdm import tqdm # Import tqdm for progress bars

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Dynamically load Zoom App credentials from environment variables
ZOOM_ACCOUNTS_CONFIG = []

# Iterate through a range or a predefined list of account suffixes
# We'll support up to 26 accounts (Account_A to Account_Z)
for i in range(1, 27):
    account_char = chr(64 + i) # A, B, C...
    account_prefix = f"ZOOM_ACCOUNT_{account_char}"
    account_id = os.getenv(f"{account_prefix}_ACCOUNT_ID")
    client_id = os.getenv(f"{account_prefix}_CLIENT_ID")
    client_secret = os.getenv(f"{account_prefix}_CLIENT_SECRET")

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
        # Stop if no more accounts are found sequentially
        if i == 1: # If Account_A isn't found, means no accounts are configured
            print("WARNING: No Zoom account credentials found in .env file. Please check your .env configuration.")
        break # Exit loop if a sequential account is missing

# Directory where downloaded meetings will be saved
DOWNLOAD_DIR = "zoom_downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Input CSV file containing meeting IDs and desired filenames
CSV_FILE = "meetings.csv"

# Log file paths
SUCCESS_LOG = "success.log"
FAILURE_LOG = "failure.log"

# Maximum number of concurrent downloads
MAX_WORKERS = 5 # Adjust based on your server's bandwidth and Zoom API rate limits

# --- Logger Setup ---
def setup_logger(log_file, name, level=logging.INFO):
    """Sets up a logger to write to a specific file."""
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

success_logger = setup_logger(SUCCESS_LOG, "success_logger")
failure_logger = setup_logger(FAILURE_LOG, "failure_logger")

# --- Zoom API Helper Functions ---

def get_access_token(account_config):
    """
    Obtains a Zoom Server-to-Server OAuth access token.
    Access tokens are valid for 1 hour.
    """
    token_url = "https://zoom.us/oauth/token"
    client_id = account_config["client_id"]
    client_secret = account_config["client_secret"]
    account_id = account_config["account_id"]

    # Base64 encode client ID and client secret
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
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        token_data = response.json()
        return token_data.get("access_token")
    except requests.exceptions.RequestException as e:
        failure_logger.error(
            f"Failed to get access token for account '{account_config['name']}': {e}"
        )
        return None

def get_meeting_recordings(meeting_id, access_token, account_name):
    """
    Fetches recording details for a given Zoom meeting ID.
    Looks for MP4 files.
    """
    # Zoom API endpoint to get meeting recordings
    api_url = f"https://api.zoom.us/v2/meetings/{meeting_id}/recordings"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        recording_data = response.json()

        # Check if the meeting has recordings and specifically look for 'MP4' files
        if recording_data.get("recording_files"):
            for record_file in recording_data["recording_files"]:
                # Prioritize MP4 video recordings
                if record_file["file_type"] == "MP4" and record_file["file_extension"] == "MP4":
                    return record_file["download_url"]
            # If no MP4, try to return the first available recording download URL
            for record_file in recording_data["recording_files"]:
                if record_file.get("download_url"):
                    return record_file["download_url"]
        return None # No downloadable recording found
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            failure_logger.warning(
                f"Meeting ID {meeting_id} not found or no recordings for account '{account_name}'. "
                f"Error: {e.response.status_code} - {e.response.text}"
            )
        elif e.response.status_code == 401: # Invalid token
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
        failure_logger.error(
            f"Network error fetching recordings for Meeting ID {meeting_id} with account '{account_name}': {e}"
        )
        return None

def download_file(url, destination_path, access_token, meeting_id, account_name):
    """
    Downloads a file from a given URL with authorization and displays a progress bar.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0)) # Get total file size

            # Progress bar setup
            with tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                desc=f"Downloading {os.path.basename(destination_path)}",
                ncols=100, # Width of the progress bar
                miniters=1, # Minimum number of iterations before update
                leave=False # Do not leave individual progress bars on screen
            ) as pbar:
                with open(destination_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk: # Filter out keep-alive new chunks
                            f.write(chunk)
                            pbar.update(len(chunk)) # Update the progress bar

        success_logger.info(
            f"Successfully downloaded Meeting ID {meeting_id} to '{destination_path}' using account '{account_name}'."
        )
        return True
    except requests.exceptions.RequestException as e:
        failure_logger.error(
            f"Failed to download Meeting ID {meeting_id} from URL '{url}' to '{destination_path}' "
            f"using account '{account_name}': {e}"
        )
        return False

# --- Main Processing Logic ---

def process_meeting_download(meeting_entry):
    """
    Processes a single meeting download request.
    Tries each Zoom account until successful or all accounts fail.
    """
    meeting_id = meeting_entry["meeting_id"]
    desired_filename = meeting_entry["desired_filename"]
    # vimeo_uri = meeting_entry["vimeo_uri"] # Read but not used in Zoom download logic

    # Ensure the desired_filename has a valid extension if not provided (e.g., if just "My_Meeting")
    if not os.path.splitext(desired_filename)[1]:
        desired_filename += ".mp4" # Default to .mp4 if no extension is given

    output_path = os.path.join(DOWNLOAD_DIR, desired_filename)

    # Check if file already exists
    if os.path.exists(output_path):
        success_logger.info(
            f"Skipping Meeting ID {meeting_id}: File '{output_path}' already exists."
        )
        print(f"Skipping {desired_filename}: Already exists.")
        return

    found_download_url = None
    used_account_name = None
    current_access_token = None # Store the token to pass to download_file

    for account_config in ZOOM_ACCOUNTS_CONFIG:
        account_name = account_config["name"]
        print(f"Attempting to find meeting {meeting_id} with {account_name}...")
        current_access_token = get_access_token(account_config)

        if current_access_token:
            download_url = get_meeting_recordings(meeting_id, current_access_token, account_name)
            if download_url:
                found_download_url = download_url
                used_account_name = account_name
                break # Found a download URL, no need to check other accounts
        else:
            print(f"Could not get access token for {account_name}.")


    if found_download_url:
        print(f"Initiating download for {desired_filename} using {used_account_name}...")
        download_success = download_file(
            found_download_url, output_path, current_access_token, meeting_id, used_account_name
        )
        if not download_success:
            print(f"Download failed for {desired_filename}. Check {FAILURE_LOG} for details.")
    else:
        print(f"Could not find a downloadable recording for Meeting ID {meeting_id} after trying all configured accounts.")
        failure_logger.error(
            f"Could not find a downloadable recording for Meeting ID {meeting_id} after trying all configured accounts."
        )

# --- Main Execution ---

def main():
    print("Starting Zoom meeting download script...")
    print(f"Downloads will be saved to: {DOWNLOAD_DIR}")
    print(f"Success logs: {SUCCESS_LOG}")
    print(f"Failure logs: {FAILURE_LOG}")
    print("-" * 50)

    if not ZOOM_ACCOUNTS_CONFIG:
        failure_logger.critical("No Zoom accounts configured. Please set up ZOOM_ACCOUNT_X_ACCOUNT_ID, ZOOM_ACCOUNT_X_CLIENT_ID, and ZOOM_ACCOUNT_X_CLIENT_SECRET in your .env file.")
        print("Script cannot proceed without Zoom account configurations. Exiting.")
        return

    meetings_to_download = []
    try:
        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            
            # Define expected headers
            expected_headers = ["Meeting ID", "Vimeo URI", "File Name"]
            
            # Validate headers in the CSV
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
        failure_logger.critical(f"CSV file '{CSV_FILE}' not found. Please create it with columns: {', '.join(expected_headers)}.")
        return
    except ValueError as e:
        failure_logger.critical(f"Error with CSV file '{CSV_FILE}': {e}")
        return
    except Exception as e:
        failure_logger.critical(f"Error reading CSV file '{CSV_FILE}': {e}")
        return

    if not meetings_to_download:
        print("No valid meeting entries found in the CSV file. Exiting.")
        return

    print(f"Found {len(meetings_to_download)} meetings to process.")
    print("-" * 50)


    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(tqdm(executor.map(process_meeting_download, meetings_to_download), 
                  total=len(meetings_to_download), 
                  desc="Overall Meeting Processing", 
                  unit="meeting", 
                  ncols=100))

    print("\n" + "-" * 50)
    print("All download tasks completed or attempted.")
    print(f"Check success log: {SUCCESS_LOG}")
    print(f"Check failure log: {FAILURE_LOG}")
    print("-" * 50)


if __name__ == "__main__":
    main()
import requests
import concurrent.futures
import json
import os
import csv
import logging
import base64
from datetime import datetime
import time
from dotenv import load_dotenv # Import load_dotenv
from tqdm import tqdm # Import tqdm for progress bars

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Dynamically load Zoom App credentials from environment variables
ZOOM_ACCOUNTS_CONFIG = []

# Iterate through a range or a predefined list of account suffixes
# You can extend this logic if you have many accounts with a systematic naming
# We'll support up to 26 accounts (Account_A to Account_Z)
for i in range(1, 27):
    account_char = chr(64 + i) # A, B, C...
    account_prefix = f"ZOOM_ACCOUNT_{account_char}"
    account_id = os.getenv(f"{account_prefix}_ACCOUNT_ID")
    client_id = os.getenv(f"{account_prefix}_CLIENT_ID")
    client_secret = os.getenv(f"{account_prefix}_CLIENT_SECRET")

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
        # Stop if no more accounts are found sequentially
        if i == 1: # If Account_A isn't found, means no accounts are configured
            print("WARNING: No Zoom account credentials found in .env file. Please check your .env configuration.")
        break # Exit loop if a sequential account is missing

# Directory where downloaded meetings will be saved
DOWNLOAD_DIR = "zoom_downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Input CSV file containing meeting IDs and desired filenames
CSV_FILE = "meetings.csv"

# Log file paths
SUCCESS_LOG = "success.log"
FAILURE_LOG = "failure.log"

# Maximum number of concurrent downloads
MAX_WORKERS = 5 # Adjust based on your server's bandwidth and Zoom API rate limits

# --- Logger Setup ---
def setup_logger(log_file, name, level=logging.INFO):
    """Sets up a logger to write to a specific file."""
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

success_logger = setup_logger(SUCCESS_LOG, "success_logger")
failure_logger = setup_logger(FAILURE_LOG, "failure_logger")

# --- Zoom API Helper Functions ---

def get_access_token(account_config):
    """
    Obtains a Zoom Server-to-Server OAuth access token.
    Access tokens are valid for 1 hour.
    """
    token_url = "https://zoom.us/oauth/token"
    client_id = account_config["client_id"]
    client_secret = account_config["client_secret"]
    account_id = account_config["account_id"]

    # Base64 encode client ID and client secret
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
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        token_data = response.json()
        return token_data.get("access_token")
    except requests.exceptions.RequestException as e:
        failure_logger.error(
            f"Failed to get access token for account '{account_config['name']}': {e}"
        )
        return None

def get_meeting_recordings(meeting_id, access_token, account_name):
    """
    Fetches recording details for a given Zoom meeting ID.
    Looks for MP4 files.
    """
    # Zoom API endpoint to get meeting recordings
    api_url = f"https://api.zoom.us/v2/meetings/{meeting_id}/recordings"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        recording_data = response.json()

        # Check if the meeting has recordings and specifically look for 'MP4' files
        if recording_data.get("recording_files"):
            for record_file in recording_data["recording_files"]:
                # Prioritize MP4 video recordings
                if record_file["file_type"] == "MP4" and record_file["file_extension"] == "MP4":
                    return record_file["download_url"]
            # If no MP4, try to return the first available recording download URL
            for record_file in recording_data["recording_files"]:
                if record_file.get("download_url"):
                    return record_file["download_url"]
        return None # No downloadable recording found
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            failure_logger.warning(
                f"Meeting ID {meeting_id} not found or no recordings for account '{account_name}'. "
                f"Error: {e.response.status_code} - {e.response.text}"
            )
        elif e.response.status_code == 401: # Invalid token
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
        failure_logger.error(
            f"Network error fetching recordings for Meeting ID {meeting_id} with account '{account_name}': {e}"
        )
        return None

def download_file(url, destination_path, access_token, meeting_id, account_name):
    """
    Downloads a file from a given URL with authorization and displays a progress bar.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0)) # Get total file size

            # Progress bar setup
            # desc: A string to precede the progress bar
            # total: The total number of items or bytes to iterate over
            # unit: The unit to display (e.g., 'B' for bytes, 'KB', 'MB')
            # unit_scale: If True, automatically scales the unit
            # unit_divisor: Divisor for unit_scale
            # leave: If True, leaves the progress bar on screen after completion
            with tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                desc=f"Downloading {os.path.basename(destination_path)}",
                ncols=100, # Width of the progress bar
                miniters=1, # Minimum number of iterations before update
                leave=False # Do not leave individual progress bars on screen
            ) as pbar:
                with open(destination_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk: # Filter out keep-alive new chunks
                            f.write(chunk)
                            pbar.update(len(chunk)) # Update the progress bar

        success_logger.info(
            f"Successfully downloaded Meeting ID {meeting_id} to '{destination_path}' using account '{account_name}'."
        )
        return True
    except requests.exceptions.RequestException as e:
        failure_logger.error(
            f"Failed to download Meeting ID {meeting_id} from URL '{url}' to '{destination_path}' "
            f"using account '{account_name}': {e}"
        )
        return False

# --- Main Processing Logic ---

def process_meeting_download(meeting_entry):
    """
    Processes a single meeting download request.
    Tries each Zoom account until successful or all accounts fail.
    """
    meeting_id = meeting_entry["meeting_id"]
    desired_filename = meeting_entry["desired_filename"]
    
    # Ensure the desired_filename has a valid extension if not provided (e.g., if just "My_Meeting")
    if not os.path.splitext(desired_filename)[1]:
        desired_filename += ".mp4" # Default to .mp4 if no extension is given

    output_path = os.path.join(DOWNLOAD_DIR, desired_filename)

    # Check if file already exists
    if os.path.exists(output_path):
        success_logger.info(
            f"Skipping Meeting ID {meeting_id}: File '{output_path}' already exists."
        )
        print(f"Skipping {desired_filename}: Already exists.")
        return

    found_download_url = None
    used_account_name = None
    current_access_token = None # Store the token to pass to download_file

    for account_config in ZOOM_ACCOUNTS_CONFIG:
        account_name = account_config["name"]
        print(f"Attempting to find meeting {meeting_id} with {account_name}...")
        current_access_token = get_access_token(account_config)

        if current_access_token:
            download_url = get_meeting_recordings(meeting_id, current_access_token, account_name)
            if download_url:
                found_download_url = download_url
                used_account_name = account_name
                break # Found a download URL, no need to check other accounts
        else:
            print(f"Could not get access token for {account_name}.")


    if found_download_url:
        print(f"Initiating download for {desired_filename} using {used_account_name}...")
        download_success = download_file(
            found_download_url, output_path, current_access_token, meeting_id, used_account_name
        )
        if not download_success:
            print(f"Download failed for {desired_filename}. Check {FAILURE_LOG} for details.")
    else:
        print(f"Could not find a downloadable recording for Meeting ID {meeting_id} after trying all configured accounts.")
        failure_logger.error(
            f"Could not find a downloadable recording for Meeting ID {meeting_id} after trying all configured accounts."
        )

# --- Main Execution ---

def main():
    print("Starting Zoom meeting download script...")
    print(f"Downloads will be saved to: {DOWNLOAD_DIR}")
    print(f"Success logs: {SUCCESS_LOG}")
    print(f"Failure logs: {FAILURE_LOG}")
    print("-" * 50)

    if not ZOOM_ACCOUNTS_CONFIG:
        failure_logger.critical("No Zoom accounts configured. Please set up ZOOM_ACCOUNT_X_ACCOUNT_ID, ZOOM_ACCOUNT_X_CLIENT_ID, and ZOOM_ACCOUNT_X_CLIENT_SECRET in your .env file.")
        print("Script cannot proceed without Zoom account configurations. Exiting.")
        return

    meetings_to_download = []
    try:
        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            # Validate headers in the CSV
            if "meeting_id" not in reader.fieldnames or "desired_filename" not in reader.fieldnames:
                raise ValueError("CSV must contain 'meeting_id' and 'desired_filename' columns.")

            for row in reader:
                meetings_to_download.append(
                    {
                        "meeting_id": row["meeting_id"].strip(),
                        "desired_filename": row["desired_filename"].strip(),
                    }
                )
    except FileNotFoundError:
        failure_logger.critical(f"CSV file '{CSV_FILE}' not found. Please create it with 'meeting_id' and 'desired_filename' columns.")
        return
    except ValueError as e:
        failure_logger.critical(f"Error with CSV file '{CSV_FILE}': {e}")
        return
    except Exception as e:
        failure_logger.critical(f"Error reading CSV file '{CSV_FILE}': {e}")
        return

    if not meetings_to_download:
        print("No valid meeting entries found in the CSV file. Exiting.")
        return

    print(f"Found {len(meetings_to_download)} meetings to process.")
    print("-" * 50)


    # Using a global progress bar for the overall process if desired,
    # or just individual progress bars for each download.
    # For many parallel downloads, individual progress bars are often clearer.

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Map the process_meeting_download function to each meeting entry
        # The executor handles running these in parallel
        list(tqdm(executor.map(process_meeting_download, meetings_to_download), 
                  total=len(meetings_to_download), 
                  desc="Overall Meeting Processing", 
                  unit="meeting", 
                  ncols=100))

    print("\n" + "-" * 50)
    print("All download tasks completed or attempted.")
    print(f"Check success log: {SUCCESS_LOG}")
    print(f"Check failure log: {FAILURE_LOG}")
    print("-" * 50)


if __name__ == "__main__":
    main()
