import os
import json
import yt_dlp
import requests
import time
import re
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import pickle
import sys
import argparse
import gspread

# Output folders
OUTPUT_FOLDER = "downloaded_videos"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Google API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]

# Queue file path
QUEUE_FILE = os.path.join("upload_queue", "tiktok_queue.json")

# TikTok session configuration
TIKTOK_SESSION_ID = os.getenv('TIKTOK_SESSION_ID')  # You'll need to set this environment variable

# Google Sheets configuration
TOKEN_FILE = 'token.pickle'
SPREADSHEET_NAME = 'TikTok Upload Queue'

def get_google_services():
    """Get or create Google Drive and Sheets services."""
    creds = None
    # Check if token.pickle file exists
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Load credentials from environment variable
            credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if credentials_json:
                credentials_data = json.loads(credentials_json)
                creds = Credentials.from_authorized_user_info(credentials_data, SCOPES)
            else:
                raise Exception("Missing Google API credentials. Please provide them via environment variable.")

        # Save the credentials for the next run
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)

    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)
    return sheets_service, drive_service

def get_or_create_spreadsheet():
    """Get or create the queue spreadsheet."""
    sheets_service, drive_service = get_google_services()
    
    # Search for existing spreadsheet
    results = drive_service.files().list(
        q=f"name='{SPREADSHEET_NAME}' and mimeType='application/vnd.google-apps.spreadsheet'",
        spaces='drive'
    ).execute()
    
    spreadsheet_id = None
    if results.get('files'):
        spreadsheet_id = results['files'][0]['id']
        
        # Update headers for existing spreadsheet
        values = [['Timestamp', 'Platform', 'Username', 'Source URL', 'Title', 'Description', 'Tags', 'Drive URL', 'Status']]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Queue!A1:I1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()
    else:
        # Create new spreadsheet if it doesn't exist
        spreadsheet = {
            'properties': {
                'title': SPREADSHEET_NAME
            },
            'sheets': [{
                'properties': {
                    'title': 'Queue',
                    'gridProperties': {
                        'frozenRowCount': 1
                    }
                }
            }]
        }
        
        spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet).execute()
        spreadsheet_id = spreadsheet['spreadsheetId']
        
        # Add headers
        values = [['Timestamp', 'Platform', 'Username', 'Source URL', 'Title', 'Description', 'Tags', 'Drive URL', 'Status']]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Queue!A1:I1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()
    
    # Make spreadsheet publicly accessible
    try:
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=permission
        ).execute()
        print(f"Spreadsheet URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    except Exception as e:
        print(f"Error making spreadsheet public: {e}")
    
    return spreadsheet_id

def get_sheet():
    """Get the queue sheet."""
    try:
        # Get credentials and spreadsheet ID
        sheets_service, _ = get_google_services()
        spreadsheet_id = get_or_create_spreadsheet()
        return (sheets_service, spreadsheet_id)
    except Exception as e:
        print(f"Error getting sheet: {e}")
        return None

def is_url_in_queue(url, sheet_info):
    """Check if URL has already been processed."""
    try:
        if not sheet_info:
            return False
            
        sheets_service, spreadsheet_id = sheet_info
        
        # Get all values from the sheet
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='Queue!A:I'
        ).execute()
        
        values = result.get('values', [])
        
        # Check if URL exists in any row
        for row in values:
            if len(row) > 3 and url in row[3]:  # URL is in fourth column
                return True
        return False
    except Exception as e:
        print(f"Error checking queue: {e}")
        return False

def get_platform_and_username(url):
    """Extract platform and username from URL."""
    if "tiktok.com" in url:
        match = re.search(r'@([^/]+)', url)
        if match:
            return "TikTok", match.group(1)
    elif "youtube.com" in url or "youtu.be" in url:
        # For YouTube Shorts, username is in the video description
        return "YouTube", None  # Will get username from video metadata
    elif "tumblr.com" in url:
        match = re.search(r'//([^.]+)\.tumblr\.com', url)
        if match:
            return "Tumblr", match.group(1)
    elif "pinterest.com" in url or "pin.it" in url:
        return "Pinterest", None  # Will get username from metadata
    elif "instagram.com" in url:
        return "Instagram", None  # Will get username from metadata
        
    return None, None

def add_to_queue(video_path, metadata):
    """Add video to upload queue in Google Sheets."""
    print("add_to_queue function called")  # Verify function execution
    try:
        print("Starting to add video to queue")
        print(f"Video path: {video_path}")
        print(f"Metadata: {metadata}")

        # Get sheet
        print("Attempting to get sheet information")
        sheet_info = get_sheet()
        if not sheet_info:
            print("Failed to get sheet")
            return

        print("Successfully retrieved sheet information")
        sheets_service, spreadsheet_id = sheet_info

        # Skip if URL already in queue
        print("Checking if URL is already in queue")
        if is_url_in_queue(metadata['webpage_url'], sheet_info):
            print(f"Video already in queue: {metadata['webpage_url']}")
            if os.path.exists(video_path):
                print("Removing local file as it's already in queue")
                os.remove(video_path)  # Clean up local file
            return

        print("Uploading video to Google Drive")
        # Upload to Drive
        drive_url = upload_to_drive(video_path)
        if not drive_url:
            print("Failed to upload to Drive")
            return

        print(f"Drive URL: {drive_url}")
        # Add to sheet
        print("Preparing to add video information to the Google Sheet")
        timestamp = "2025-01-19T21:40:28-08:00"  # Using provided timestamp
        row = [[
            timestamp,
            metadata['platform'],
            metadata['username'],
            metadata['webpage_url'],  # Source URL
            metadata.get('title', 'Untitled'),
            metadata.get('description', ''),
            ','.join(metadata.get('tags', [])),
            drive_url,  # Drive URL
            'pending'  # Status
        ]]

        print("Adding video information to the Google Sheet")
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range='Queue!A:I',  # Updated range to include new columns
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body={'values': row}
        ).execute()

        print(f"Successfully added to queue: {metadata.get('title', 'Untitled')}")

        # Clean up local file
        if os.path.exists(video_path):
            print(f"Removing local file: {video_path}")
            os.remove(video_path)

    except Exception as e:
        print(f"Error adding to queue: {e}")
        if os.path.exists(video_path):
            print(f"Removing local file after error: {video_path}")
            os.remove(video_path)  # Clean up local file even if there's an error

def upload_to_drive(file_path, folder_name='TikTok Videos', max_retries=3, retry_delay=1):
    """Upload a file to Google Drive and return its public URL."""
    try:
        _, drive_service = get_google_services()
        
        # Get or create folder
        folder_id = None
        response = drive_service.files().list(
            q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'",
            spaces='drive'
        ).execute()
        
        if response.get('files'):
            folder_id = response['files'][0]['id']
        else:
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = drive_service.files().create(body=folder_metadata).execute()
            folder_id = folder['id']
        
        # Upload file with retries
        for attempt in range(max_retries):
            try:
                # Prepare file metadata
                file_metadata = {
                    'name': os.path.basename(file_path),
                    'parents': [folder_id]
                }
                
                # Create media
                media = MediaFileUpload(
                    file_path,
                    mimetype='video/mp4',
                    resumable=True
                )
                
                # Upload file
                file = drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                # Make file publicly accessible with retries
                for permission_attempt in range(max_retries):
                    try:
                        permission = {
                            'type': 'anyone',
                            'role': 'reader'
                        }
                        drive_service.permissions().create(
                            fileId=file['id'],
                            body=permission
                        ).execute()
                        break
                    except HttpError as e:
                        if e.resp.status == 503 and permission_attempt < max_retries - 1:
                            print(f"Transient error setting permissions, retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            continue
                        raise
                
                # Get direct download URL
                file_url = f"https://drive.google.com/uc?export=download&id={file['id']}"
                print(f"Uploaded to Drive: {file_url}")
                return file_url
                
            except HttpError as e:
                if e.resp.status == 503 and attempt < max_retries - 1:
                    print(f"Transient error uploading file, retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                raise
                
        return None
        
    except Exception as e:
        print(f"Error uploading to Drive: {str(e)}")
        return None

def upload_to_tiktok(video_path, caption=""):
    """Upload video directly to TikTok."""
    if not TIKTOK_SESSION_ID:
        print("Error: TikTok session ID not found. Please set the TIKTOK_SESSION_ID environment variable.")
        return False

    try:
        # Initialize TikTok auth
        auth = AuthBackend(session_id=TIKTOK_SESSION_ID)
        
        # Upload the video
        upload_video(
            filename=video_path,
            description=caption,
            auth=auth
        )
        
        print(f"Successfully uploaded video to TikTok: {video_path}")
        return True

    except Exception as e:
        print(f"Failed to upload to TikTok: {str(e)}")
        return False

def sanitize_filename(filename):
    """Sanitize filename to be safe for filesystems."""
    # Remove invalid characters
    invalid_chars = '<>:"/\\|?*\u3000#'
    for char in invalid_chars:
        filename = filename.replace(char, '')
    
    # Remove emojis and other non-ASCII characters
    filename = ''.join(c for c in filename if ord(c) < 128)
    
    # Replace spaces and dots in the middle
    filename = filename.replace(' ', '_').replace('..', '.')
    
    # Limit length
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:196] + ext
        
    # Ensure filename is not empty
    if not filename or filename.startswith('.'):
        filename = 'video' + filename
    
    return filename

def get_video_path(info, ext=None):
    """Get sanitized video path from info dict."""
    if not ext:
        ext = info.get('ext', 'mp4')
    
    # Use video ID instead of title for filename
    video_id = info.get('id', 'video')
    platform = info.get('extractor_key', '').lower()
    filename = f"{platform}_{video_id}.{ext}"
    
    return os.path.join(OUTPUT_FOLDER, filename)

def download_video_youtube(url):
    """Download a video from YouTube."""
    try:
        cookies_file_path = os.path.join(os.path.dirname(__file__), 'cookies.txt')
        ydl_opts = {
            'format': 'best[ext=mp4]',
            'outtmpl': os.path.join(OUTPUT_FOLDER, '%(extractor_key)s_%(id)s.%(ext)s'),
            'quiet': True,
            'cookiefile': cookies_file_path  # Use the new cookies file from the repository
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return get_video_path(info), info
    except Exception as e:
        print(f"Failed to download YouTube video: {url}. Error: {str(e)}")
        return None, None

def download_video_tiktok(url):
    """Download a video from TikTok."""
    try:
        ydl_opts = {
            'format': 'best[ext=mp4]',
            'outtmpl': os.path.join(OUTPUT_FOLDER, '%(extractor_key)s_%(id)s.%(ext)s'),
            'quiet': True
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return get_video_path(info), info
    except Exception as e:
        print(f"Failed to download TikTok video: {url}. Error: {str(e)}")
        return None, None

def download_video_instagram(url):
    """Download a video from Instagram."""
    try:
        ydl_opts = {
            'format': 'best[ext=mp4]',
            'outtmpl': os.path.join(OUTPUT_FOLDER, '%(extractor_key)s_%(id)s.%(ext)s'),
            'quiet': True
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return get_video_path(info), info
    except Exception as e:
        print(f"Failed to download Instagram video: {url}. Error: {str(e)}")
        return None, None

def download_video_tumblr(url):
    """Download a video from Tumblr."""
    try:
        ydl_opts = {
            'format': 'best[ext=mp4]',
            'outtmpl': os.path.join(OUTPUT_FOLDER, '%(extractor_key)s_%(id)s.%(ext)s'),
            'quiet': True
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return get_video_path(info), info
    except Exception as e:
        print(f"Failed to download Tumblr video: {url}. Error: {str(e)}")
        return None, None

def download_video_pinterest(url):
    """Download a video from Pinterest."""
    try:
        ydl_opts = {
            'format': 'best[ext=mp4]',
            'outtmpl': os.path.join(OUTPUT_FOLDER, '%(extractor_key)s_%(id)s.%(ext)s'),
            'quiet': True
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            print("\nPinterest metadata keys:", list(info.keys()))
            print("Uploader:", info.get('uploader'))
            print("Uploader ID:", info.get('uploader_id'))
            return get_video_path(info), info
    except Exception as e:
        print(f"Failed to download Pinterest video: {url}. Error: {str(e)}")
        return None, None

def process_url(url):
    print("process_url function called with URL: ", url)
    """Process a single URL based on its type."""
    try:
        # Clean up URL - remove tracking parameters
        print("Cleaning up URL")
        url = url.split('?')[0] if '?' in url else url
        
        # Expand shortened Pinterest URL
        if 'pin.it' in url:
            print("Expanding Pinterest URL")
            response = requests.head(url, allow_redirects=True)
            url = response.url
            print("Expanded URL: ", url)
            
        # Fix Tumblr URLs
        if 'tumblr.com' in url:
            print("Fixing Tumblr URL")
            # Remove any query parameters first
            url = url.split('?')[0]
            
            # Convert to blog.tumblr.com format if needed
            if 'www.tumblr.com' in url:
                match = re.search(r'tumblr\.com/([^/]+)(?:/post)?/(\d+)', url)
                if match:
                    blog_name, post_id = match.groups()
                    url = f"https://{blog_name}.tumblr.com/post/{post_id}"
                    print(f"Fixed Tumblr URL: {url}")

        video_path = None
        info = None
        
        print("Determining platform from URL")
        if "tiktok.com" in url:
            print("Processing TikTok URL")
            video_path, info = download_video_tiktok(url)
        elif "youtube.com" in url or "youtu.be" in url:
            print("Processing YouTube URL")
            video_path, info = download_video_youtube(url)
        elif "tumblr.com" in url:
            print("Processing Tumblr URL")
            video_path, info = download_video_tumblr(url)
        elif "pinterest.com" in url or "pin.it" in url:
            print("Processing Pinterest URL")
            video_path, info = download_video_pinterest(url)
        elif "instagram.com" in url:
            print("Processing Instagram URL")
            video_path, info = download_video_instagram(url)
        else:
            print(f"Unsupported URL: {url}")
            return
        
        if video_path and info:
            print("Video downloaded successfully")
            # Get platform and username from URL
            platform, username = get_platform_and_username(url)
            print(f"Platform: {platform}, Username: {username}")
            
            # For YouTube, get channel handle from video metadata
            if platform == "YouTube" and not username:
                print("Getting YouTube channel handle")
                # First try to get handle from channel URL directly
                channel_url = info.get('channel_url', '')
                if '/@' in channel_url:
                    username = channel_url.split('/@')[1].split('/')[0]  # Remove @ prefix since we add it later
                else:
                    # Try to get channel handle from uploader ID or metadata
                    try:
                        with yt_dlp.YoutubeDL({
                            'quiet': True,
                            'extract_flat': True,  # Don't download video info
                            'timeout': 10,  # Timeout after 10 seconds
                        }) as ydl:
                            # First check if uploader_id looks like a handle
                            uploader_id = info.get('uploader_id', '')
                            if uploader_id and not uploader_id.startswith('UC'):
                                username = uploader_id.lstrip('@')  # Remove @ if present
                            # If not, try to get from channel page
                            elif channel_url:
                                channel_info = ydl.extract_info(channel_url, download=False)
                                # Look for channel handle in metadata
                                handle = channel_info.get('channel_handle', '')
                                if handle:
                                    username = handle.lstrip('@')  # Remove @ if present
                                
                    except Exception as e:
                        print(f"Could not fetch YouTube channel handle: {str(e)}")
                
                # Fall back to display name if no handle found
                if not username:
                    username = info.get('uploader', 'Unknown')

            # For Pinterest, get username from metadata
            elif platform == "Pinterest" and not username:
                print("Getting Pinterest username")
                # Try to get username from original URL if available
                original_url = info.get('original_url', '')
                if original_url and 'pinterest.com/' in original_url:
                    try:
                        username = original_url.split('pinterest.com/')[1].split('/')[0]
                        if username and not username.isdigit() and username != 'pin':
                            return username
                    except:
                        pass

                # Try to get username from uploader URL
                uploader_url = info.get('uploader_url', '')
                if uploader_url:
                    if '/user/' in uploader_url:
                        username = uploader_url.split('/user/')[1].strip('/')
                    elif uploader_url.startswith('https://www.pinterest.com/'):
                        username = uploader_url.split('pinterest.com/')[1].strip('/')
                
                # Try uploader_id if it's not numeric
                if not username or username.isdigit():
                    uploader_id = info.get('uploader_id', '')
                    if uploader_id and not uploader_id.isdigit():
                        username = uploader_id
                
                # Fall back to display name only if we couldn't get a username
                if not username or username.isdigit() or username == 'pin':
                    username = info.get('uploader', 'Unknown')
            
            # For Instagram, get username from metadata
            elif platform == "Instagram" and not username:
                # Try to get username from original URL if available
                original_url = info.get('original_url', '')
                if original_url and 'instagram.com/' in original_url:
                    try:
                        username = original_url.split('instagram.com/')[1].split('/')[0]
                        if username and not username.isdigit():
                            return username
                    except:
                        pass

                # Try to get username from uploader URL
                uploader_url = info.get('uploader_url', '')
                if uploader_url:
                    if '/user/' in uploader_url:
                        username = uploader_url.split('/user/')[1].strip('/')
                    elif uploader_url.startswith('https://www.instagram.com/'):
                        username = uploader_url.split('instagram.com/')[1].strip('/')
                
                # Try uploader_id if it's not numeric
                if not username or username.isdigit():
                    uploader_id = info.get('uploader_id', '')
                    if uploader_id and not uploader_id.isdigit():
                        username = uploader_id
                
                # Fall back to display name only if we couldn't get a username
                if not username or username.isdigit():
                    username = info.get('uploader', 'Unknown')
            
            metadata = {
                'title': info.get('title', 'Untitled'),
                'description': info.get('description', ''),
                'tags': info.get('tags', []),
                'webpage_url': url,
                'platform': platform,
                'username': username
            }

            # Check for source video URL in description or other metadata
            source_url = None
            description = info.get('description', '')
            
            # Common source video patterns
            source_patterns = [
                r'https?://(?:www\.)?tiktok\.com/[^\s]+',
                r'https?://(?:www\.)?instagram\.com/[^\s]+',
                r'https?://(?:www\.)?youtube\.com/[^\s]+',
                r'https?://(?:www\.)?youtu\.be/[^\s]+',
                r'https?://[^.]+\.tumblr\.com/[^\s]+'
            ]
            
            # Search for source URL in description
            for pattern in source_patterns:
                match = re.search(pattern, description)
                if match:
                    source_url = match.group(0).split('?')[0]  # Remove query params
                    break
            
            # If source URL found, try to get its metadata
            if source_url:
                try:
                    with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
                        source_info = ydl.extract_info(source_url, download=False)
                        source_platform = None
                        if "tiktok.com" in source_url:
                            source_platform = "TikTok"
                        elif "youtube.com" in source_url or "youtu.be" in source_url:
                            source_platform = "YouTube"
                        elif "tumblr.com" in source_url:
                            source_platform = "Tumblr"
                        elif "pinterest.com" in source_url or "pin.it" in source_url:
                            source_platform = "Pinterest"
                        elif "instagram.com" in source_url:
                            source_platform = "Instagram"
                        
                        if source_platform == "YouTube":
                            # First try to get handle from channel URL directly
                            channel_url = source_info.get('channel_url', '')
                            if '/@' in channel_url:
                                source_username = channel_url.split('/@')[1].split('/')[0]
                            else:
                                # Try to get channel handle from uploader ID or metadata
                                uploader_id = source_info.get('uploader_id', '')
                                if uploader_id and not uploader_id.startswith('UC'):
                                    source_username = uploader_id.lstrip('@')
                                else:
                                    handle = source_info.get('channel_handle', '')
                                    if handle:
                                        source_username = handle.lstrip('@')
                                    else:
                                        source_username = source_info.get('uploader', 'Unknown')
                        elif source_platform == "Pinterest":
                            # Try to get username from original URL if available
                            original_url = source_info.get('original_url', '')
                            if original_url and 'pinterest.com/' in original_url:
                                try:
                                    source_username = original_url.split('pinterest.com/')[1].split('/')[0]
                                    if source_username and not source_username.isdigit() and source_username != 'pin':
                                        return source_username
                                except:
                                    pass
                            
                            # Try uploader URL if original URL didn't work
                            if not source_username:
                                uploader_url = source_info.get('uploader_url', '')
                                if uploader_url:
                                    if '/user/' in uploader_url:
                                        source_username = uploader_url.split('/user/')[1].strip('/')
                                    elif uploader_url.startswith('https://www.pinterest.com/'):
                                        source_username = uploader_url.split('pinterest.com/')[1].strip('/')
                            
                            # Try uploader_id if it's not numeric
                            if not source_username or source_username.isdigit():
                                uploader_id = source_info.get('uploader_id', '')
                                if uploader_id and not uploader_id.isdigit():
                                    source_username = uploader_id
                            
                            # Fall back to display name if needed
                            if not source_username or source_username.isdigit() or source_username == 'pin':
                                source_username = source_info.get('uploader', 'Unknown')
                        elif source_platform == "Instagram":
                            # Try to get username from original URL if available
                            original_url = source_info.get('original_url', '')
                            if original_url and 'instagram.com/' in original_url:
                                try:
                                    source_username = original_url.split('instagram.com/')[1].split('/')[0]
                                    if source_username and not source_username.isdigit():
                                        return source_username
                                except:
                                    pass

                            # Try to get username from uploader URL
                            uploader_url = source_info.get('uploader_url', '')
                            if uploader_url:
                                if '/user/' in uploader_url:
                                    source_username = uploader_url.split('/user/')[1].strip('/')
                                elif uploader_url.startswith('https://www.instagram.com/'):
                                    source_username = uploader_url.split('instagram.com/')[1].strip('/')
                            
                            # Try uploader_id if it's not numeric
                            if not source_username or source_username.isdigit():
                                uploader_id = source_info.get('uploader_id', '')
                                if uploader_id and not uploader_id.isdigit():
                                    source_username = uploader_id
                            
                            # Fall back to display name if needed
                            if not source_username or source_username.isdigit():
                                source_username = source_info.get('uploader', 'Unknown')
                        else:
                            source_username = source_info.get('uploader', 'Unknown')
                        
                        # Update metadata if we found valid source information
                        if source_platform and source_username:
                            metadata['platform'] = source_platform
                            metadata['username'] = source_username
                            print(f"Updated to source video creator: {source_username} on {source_platform}")
                except Exception as e:
                    print(f"Could not fetch source video info: {str(e)}")
            
            print("Calling add_to_queue")
            add_to_queue(video_path, metadata)
            
    except Exception as e:
        print(f"Error processing URL: {e}")

def main():
    """Main function to process URLs."""
    parser = argparse.ArgumentParser(description='Download and queue videos for TikTok upload')
    parser.add_argument('--file', '-f', help='File containing URLs to process')
    parser.add_argument('urls', nargs='*', help='URLs to process')
    
    args = parser.parse_args()
    urls = []
    
    # Get URLs from file if specified
    if args.file and os.path.exists(args.file):
        with open(args.file, 'r') as f:
            urls.extend([line.strip() for line in f.readlines() if line.strip()])
    
    # Add URLs from command line
    if args.urls:
        urls.extend(args.urls)
    
    if not urls:
        print("No URLs provided. Use --file to specify a file with URLs or provide URLs as arguments.")
        return
    
    # Process each URL
    for url in urls:
        process_url(url)
    
    print("\nAll videos have been processed and added to the upload queue.")
    print("The queue has been synced to your Google Drive.")
    print("You can now set up Make.com to monitor the queue folder and post to TikTok.")

from flask import Flask, request, jsonify
import os

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "TikTok Upload Queue API is running!"

@app.route("/process", methods=["POST"])
def process_video():
    data = request.json
    url = data.get("url")
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    # Call your script's `process_url` function
    try:
        process_url(url)
        return jsonify({"message": f"Processed URL: {url}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
