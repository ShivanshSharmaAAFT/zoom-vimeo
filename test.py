import os
from dotenv import load_dotenv
from vimeo import VimeoClient
import json

# Load environment variables from .env file
load_dotenv()

VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")

if not VIMEO_ACCESS_TOKEN:
    print("Error: VIMEO_ACCESS_TOKEN not found in .env file. Please set it.")
    exit()

client = VimeoClient(
    token=VIMEO_ACCESS_TOKEN,
    key=None,
    secret=None
)

def verify_access_token():
    """Verifies the access token and returns user info and scopes."""
    print("Verifying Vimeo Access Token...")
    try:
        response = client.get('/oauth/verify')
        if response.status_code == 200:
            token_info = response.json()
            print("\n--- Access Token Verification Successful ---")
            print(f"  Authenticated User URI: {token_info.get('user', {}).get('uri')}")
            # Print raw scope data for clearer debugging
            print(f"  Token Scopes (RAW from API): {token_info.get('scope')}")
            print("------------------------------------------")
            return token_info
        else:
            print(f"Error verifying token: {response.status_code} - {response.text}")
            print("This usually means the token is invalid or expired.")
            return None
    except Exception as e:
        print(f"An error occurred during token verification: {e}")
        return None

def get_current_user_details():
    """Fetches the authenticated user's full details."""
    print("Fetching current user details (/me endpoint)...")
    try:
        response = client.get('/me')
        if response.status_code == 200:
            user_details = response.json()
            print("\n--- Current User Details ---")
            print(f"  Name: {user_details.get('name')}")
            print(f"  User URI: {user_details.get('uri')}")
            print(f"  Account Type: {user_details.get('account_type')}") # Will be 'basic', 'plus', 'pro', 'business', 'enterprise'
            print(f"  Is Plus: {user_details.get('is_plus')}")
            print(f"  Is Pro: {user_details.get('is_pro')}")
            print(f"  Is Business: {user_details.get('is_business')}")
            print("----------------------------")
            return user_details
        else:
            print(f"Error getting current user details: {response.status_code} - {response.text}")
            print("This could indicate an issue with your token or user account status.")
            return None
    except Exception as e:
        print(f"An error occurred while fetching user details: {e}")
        return None


def list_teams_and_folders():
    """
    Lists all teams the authenticated user belongs to and then lists
    folders (projects) within each team.
    """
    token_info = verify_access_token()
    if not token_info:
        print("Token verification failed. Cannot proceed with API calls.")
        return

    user_details = get_current_user_details()
    if not user_details:
        print("Could not retrieve current user details. Cannot proceed with listing teams/folders.")
        return

    print("\nAttempting to list Vimeo teams and their folders...")
    try:
        # First, list teams the user is a part of
        teams_response = client.get('/me/teams')
        if teams_response.status_code != 200:
            print(f"Error fetching teams: {teams_response.status_code} - {teams_response.text}")
            print("Possible reasons: user is not part of any teams, or account type lacks API access to teams.")
            # Do NOT exit, fall through to personal folders check
        else:
            teams_data = teams_response.json().get('data', [])
            if teams_data:
                print(f"\n--- Found {len(teams_data)} Vimeo Team(s) ---")
                for team in teams_data:
                    team_name = team.get('name')
                    team_uri = team.get('uri') 
                    team_id = team_uri.split('/')[-1]

                    print(f"\nTeam Name: {team_name}")
                    print(f"Team ID: {team_id}")
                    print("-" * 30)

                    print(f"  Fetching folders (projects) for Team '{team_name}'...")
                    page = 1
                    per_page = 100
                    team_folders = []

                    while True:
                        folders_response = client.get(f'/teams/{team_id}/projects?page={page}&per_page={per_page}')
                        if folders_response.status_code == 200:
                            folders_data = folders_response.json()
                            folders = folders_data.get('data', [])
                            team_folders.extend(folders)
                            
                            if not folders_data.get('paging', {}).get('next'):
                                break
                            page += 1
                        else:
                            print(f"  Error fetching folders for Team '{team_name}': {folders_response.status_code} - {folders_response.text}")
                            print("  Possible reasons: token scopes for team projects, or specific team permissions.")
                            break
                    
                    if team_folders:
                        print(f"  Found {len(team_folders)} folder(s) in Team '{team_name}':")
                        for folder in team_folders:
                            folder_name = folder.get('name')
                            folder_uri = folder.get('uri') 
                            folder_id = folder_uri.split('/')[-1] if folder_uri else 'N/A' 

                            print(f"    Folder Name: {folder_name}")
                            print(f"    Folder ID (for CSV): {folder_id}")
                            print(f"    Recommended CSV Vimeo URI: https://vimeo.com/manage/folders/{folder_id}")
                            print(f"    API URI: {folder_uri}")
                            print("    " + "-" * 20)
                print("\n--- End of Team Folders ---")
            else:
                print("No teams found for your Vimeo account.")
        
        # Always attempt to list personal folders as a fallback or for verification
        print("\nAttempting to list personal folders (albums):")
        list_personal_folders()

    except Exception as e:
        print(f"An unexpected error occurred while listing teams or folders: {e}")

def list_personal_folders():
    """Lists all personal folders (albums) for the authenticated user."""
    try:
        response = client.get('/me/albums')
        if response.status_code == 200:
            data = response.json().get('data', [])
            if data:
                print("\n--- Your Personal Vimeo Folders (Albums) ---")
                for folder in data:
                    name = folder.get('name')
                    uri = folder.get('uri')
                    album_id = uri.split('/')[-1] if uri else 'N/A'
                    print(f"  Name: {name}")
                    print(f"  ID (for CSV): {album_id}")
                    print(f"  Recommended CSV Vimeo URI: https://vimeo.com/manage/folders/{album_id}")
                    print(f"  API URI: {uri}")
                    print("-" * 30)
            else:
                print("No personal folders (albums) found.")
        else:
            print(f"Error fetching personal folders: {response.status_code} - {response.text}")
            print("Possible reasons: token scopes, or no personal albums exist.")
    except Exception as e:
        print(f"An error occurred while listing personal folders: {e}")


if __name__ == "__main__":
    list_teams_and_folders()
