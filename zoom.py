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

# Google Sheets API imports
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
ZOOM_ACCOUNTS_CONFIG = []

for i in range(1, 27):
    account_char = chr(64 + i)
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
        if i == 1:
            print("WARNING: No Zoom account credentials found in .env file. Please check your .env configuration.")
        break

DOWNLOAD_DIR = "zoom_downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

CSV_FILE = "meetings.csv"

# --- Google Sheets Configuration ---
# Make sure to place your service account key JSON file in the same directory
# or provide the full path to it.
SERVICE_ACCOUNT_FILE = 'service_account_credentials.json'
SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID") # Get this from your .env file
SUCCESS_SHEET_NAME = "Success Log"
FAILURE_SHEET_NAME = "Failure Log"

# --- Logger Setup (Modified for Google Sheets) ---
# Initialize Google Sheets service globally or pass it around
sheets_service = None

def initialize_sheets_service():
    """Initializes the Google Sheets API service."""
    global sheets_service
    if sheets_service is None:
        try:
            creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            sheets_service = build('sheets', 'v4', credentials=creds)
            print("Google Sheets service initialized successfully.")
            return True
        except Exception as e:
            print(f"Error initializing Google Sheets service: {e}")
            print(f"Please ensure '{SERVICE_ACCOUNT_FILE}' exists and is valid.")
            return False
    return True # Service already initialized

def log_to_google_sheet(sheet_name, data):
    """Appends a row of data to the specified Google Sheet."""
    if not sheets_service:
        print("Google Sheets service not initialized. Cannot log.")
        return

    range_name = f"'{sheet_name}'!A1" # Appends to the first available row

    body = {
        'values': [data]
    }
    try:
        result = sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        # print(f"Logged to {sheet_name}: {data}") # Uncomment for verbose logging
    except HttpError as e:
        print(f"Failed to log to Google Sheet '{sheet_name}': {e}")
        print("Please check if the service account has editor access to the spreadsheet.")
    except Exception as e:
        print(f"An unexpected error occurred while logging to Google Sheet '{sheet_name}': {e}")


# --- Zoom API Helper Functions (No changes needed here for logging) ---

def get_access_token(account_config):
    """
    Obtains a Zoom Server-to-Server OAuth access token.
    Access tokens are valid for 1 hour.
    """
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
        log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "ERROR", f"Failed to get access token for account '{account_config['name']}': {e}"])
        return None

def get_meeting_recordings(meeting_id, access_token, account_name):
    """
    Fetches recording details for a given Zoom meeting ID.
    Looks for MP4 files.
    """
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
            # Prioritize MP4 files explicitly
            for record_file in recording_data["recording_files"]:
                if record_file["file_type"] == "MP4" and record_file["file_extension"] == "MP4":
                    return record_file["download_url"]
            # Fallback to any download_url if no MP4 is found
            for record_file in recording_data["recording_files"]:
                if record_file.get("download_url"):
                    return record_file["download_url"]
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "WARNING", f"Meeting ID {meeting_id} not found or no recordings for account '{account_name}'. Error: {e.response.status_code} - {e.response.text}"])
        elif e.response.status_code == 401:
            log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "ERROR", f"Unauthorized access for Meeting ID {meeting_id} using account '{account_name}'. Access token might be expired or invalid. Error: {e.response.status_code} - {e.response.text}"])
        else:
            log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "ERROR", f"API error fetching recordings for Meeting ID {meeting_id} with account '{account_name}': {e}"])
        return None
    except requests.exceptions.RequestException as e:
        log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "ERROR", f"Network error fetching recordings for Meeting ID {meeting_id} with account '{account_name}': {e}"])
        return None

def download_file(url, destination_path, access_token, meeting_id, account_name):
    """
    Downloads a file from a given URL with authorization.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(destination_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        log_to_google_sheet(SUCCESS_SHEET_NAME, [datetime.now().isoformat(), "INFO", f"Successfully downloaded Meeting ID {meeting_id} to '{destination_path}' using account '{account_name}'."])
        return True
    except requests.exceptions.RequestException as e:
        log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "ERROR", f"Failed to download Meeting ID {meeting_id} from URL '{url}' to '{destination_path}' using account '{account_name}': {e}"])
        return False

# --- Main Processing Logic ---

def process_meeting_download(meeting_entry):
    """
    Processes a single meeting download request.
    Tries each Zoom account until successful or all accounts fail.
    """
    meeting_id = meeting_entry["meeting_id"]
    desired_filename = meeting_entry["desired_filename"]

    if not os.path.splitext(desired_filename)[1]:
        desired_filename += ".mp4"

    output_path = os.path.join(DOWNLOAD_DIR, desired_filename)

    if os.path.exists(output_path):
        log_to_google_sheet(SUCCESS_SHEET_NAME, [datetime.now().isoformat(), "INFO", f"Skipping Meeting ID {meeting_id}: File '{output_path}' already exists."])
        return

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
            # Error already logged in download_file
            pass
    else:
        log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "ERROR", f"Could not find a downloadable recording for Meeting ID {meeting_id} after trying all configured accounts."])

# --- Main Execution ---

def main():
    print("Starting Zoom meeting download script...")
    print(f"Downloads will be saved to: {DOWNLOAD_DIR}")
    print("Logs will be written to Google Sheets.")
    print("-" * 50)

    if not initialize_sheets_service():
        print("Script cannot proceed without Google Sheets service. Exiting.")
        return

    # Create initial headers for the sheets if they don't exist (optional, but good practice)
    # You might want to run this once manually or check for existing headers in your script
    try:
        # Check if Success Log sheet exists and has headers
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = sheet_metadata.get('sheets', '')
        success_sheet_exists = any(s['properties']['title'] == SUCCESS_SHEET_NAME for s in sheets)
        failure_sheet_exists = any(s['properties']['title'] == FAILURE_SHEET_NAME for s in sheets)

        if not success_sheet_exists:
             # Create the sheet if it doesn't exist
            batch_update_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': SUCCESS_SHEET_NAME
                        }
                    }
                }]
            }
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body=batch_update_body).execute()
            log_to_google_sheet(SUCCESS_SHEET_NAME, ["Timestamp", "Level", "Message"])
        else:
             # Check if headers exist, if not, append them
             result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID, range=f"'{SUCCESS_SHEET_NAME}'!A1:C1").execute()
             values = result.get('values', [])
             if not values or values[0] != ["Timestamp", "Level", "Message"]:
                 log_to_google_sheet(SUCCESS_SHEET_NAME, ["Timestamp", "Level", "Message"])

        if not failure_sheet_exists:
            batch_update_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': FAILURE_SHEET_NAME
                        }
                    }
                }]
            }
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body=batch_update_body).execute()
            log_to_google_sheet(FAILURE_SHEET_NAME, ["Timestamp", "Level", "Message"])
        else:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID, range=f"'{FAILURE_SHEET_NAME}'!A1:C1").execute()
            values = result.get('values', [])
            if not values or values[0] != ["Timestamp", "Level", "Message"]:
                log_to_google_sheet(FAILURE_SHEET_NAME, ["Timestamp", "Level", "Message"])

    except Exception as e:
        print(f"Could not set up Google Sheet headers or create sheets: {e}")
        print("Please ensure the service account has appropriate permissions and the spreadsheet ID is correct.")
        return


    if not ZOOM_ACCOUNTS_CONFIG:
        log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "CRITICAL", "No Zoom accounts configured. Please set up ZOOM_ACCOUNT_X_ACCOUNT_ID, ZOOM_ACCOUNT_X_CLIENT_ID, and ZOOM_ACCOUNT_X_CLIENT_SECRET in your .env file."])
        print("Script cannot proceed without Zoom account configurations. Exiting.")
        return

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
        log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "CRITICAL", f"CSV file '{CSV_FILE}' not found. Please create it with columns: {', '.join(expected_headers)}."])
        print(f"Error: CSV file '{CSV_FILE}' not found. Exiting.")
        return
    except ValueError as e:
        log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "CRITICAL", f"Error with CSV file '{CSV_FILE}': {e}"])
        print(f"Error with CSV file: {e}. Exiting.")
        return
    except Exception as e:
        log_to_google_sheet(FAILURE_SHEET_NAME, [datetime.now().isoformat(), "CRITICAL", f"An unexpected error occurred while reading CSV: {e}. Exiting."])
        print(f"An unexpected error occurred while reading CSV: {e}. Exiting.")
        return

    if not meetings_to_download:
        print("No valid meeting entries found in the CSV file. Exiting.")
        return

    print(f"Found {len(meetings_to_download)} meetings to process.")
    print("-" * 50)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(tqdm(executor.map(process_meeting_download, meetings_to_download),
                    total=len(meetings_to_download),
                    desc="Processing Meetings",
                    unit="meeting",
                    ncols=100))

    print("\n" + "-" * 50)
    print("All download tasks completed or attempted.")
    print(f"Check your Google Sheet '{SPREADSHEET_ID}' for success and failure logs.")
    print("-" * 50)


if __name__ == "__main__":
    main()
