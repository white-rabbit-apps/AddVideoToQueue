# Import necessary libraries
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
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials
import pickle
import argparse
import gspread
import tempfile
import shutil
import io
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, jsonify
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
OUTPUT_FOLDER = "downloaded_videos"
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]
QUEUE_FILE = os.path.join("upload_queue", "tiktok_queue.json")
TIKTOK_SESSION_ID = os.getenv('TIKTOK_SESSION_ID')
TOKEN_FILE = 'token.pickle'
SPREADSHEET_NAME = 'CommuniKitty Video Upload Queue'
SHEET_NAME = 'Queue'  # Replace with the actual sheet name

# Function to retrieve the SPREADSHEET_ID using the spreadsheet name
def get_spreadsheet_id_by_name(spreadsheet_name):
    logging.debug(f"Searching for spreadsheet with name: {spreadsheet_name}")
    try:
        # Search for the spreadsheet by name
        results = drive_service.files().list(
            q=f"name='{spreadsheet_name}' and mimeType='application/vnd.google-apps.spreadsheet'",
            spaces='drive',
            fields='files(id, name)').execute()
        items = results.get('files', [])

        if not items:
            logging.error("No spreadsheet found with the specified name.")
            return None
        else:
            # Return the first matching spreadsheet ID
            spreadsheet_id = items[0]['id']
            logging.debug(f"Spreadsheet ID found: {spreadsheet_id}")
            return spreadsheet_id
    except Exception as e:
        logging.error(f"Error retrieving spreadsheet ID: {str(e)}")
        return None

# Initialize Google Drive and Sheets API clients
creds = None
if os.path.exists(TOKEN_FILE):
    with open(TOKEN_FILE, 'rb') as token:
        creds = pickle.load(token)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open(TOKEN_FILE, 'wb') as token:
        pickle.dump(creds, token)

drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# Use this function to get the SPREADSHEET_ID
SPREADSHEET_ID = get_spreadsheet_id_by_name(SPREADSHEET_NAME)

# Ensure output directory exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Initialize Flask app
app = Flask(__name__)

# Google Services

def get_google_services():
    logging.debug("Attempting to get Google services.")
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)
            logging.debug("Loaded credentials from token file.")

    if not creds or not creds.valid:
        logging.debug("Credentials are not valid, refreshing or obtaining new credentials.")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            logging.debug("Credentials refreshed.")
        else:
            credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if credentials_json:
                with open('credentials.json', 'w') as creds_file:
                    creds_file.write(credentials_json)
                logging.debug("Credentials JSON saved to file.")
                credentials_data = json.loads(credentials_json)
                flow = InstalledAppFlow.from_client_config(credentials_data, SCOPES)
                creds = flow.run_local_server(port=0)
                logging.debug("Obtained new credentials via local server.")
            else:
                logging.error("Missing Google API credentials.")
                raise Exception("Missing Google API credentials.")

        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)
            logging.debug("Credentials saved to token file.")

    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)
    logging.debug("Google services initialized.")
    return sheets_service, drive_service

def get_or_create_spreadsheet():
    logging.debug("Attempting to get or create spreadsheet.")
    sheets_service, drive_service = get_google_services()
    
    results = drive_service.files().list(
        q=f"name='{SPREADSHEET_NAME}' and mimeType='application/vnd.google-apps.spreadsheet'",
        spaces='drive'
    ).execute()
    
    spreadsheet_id = None
    if results.get('files'):
        spreadsheet_id = results['files'][0]['id']
        logging.debug("Spreadsheet found.")
        
        values = [['Timestamp', 'Platform', 'Username', 'Source URL', 'Title', 'Description', 'Tags', 'Drive URL', 'Status']]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Queue!A1:I1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()
    else:
        logging.debug("Spreadsheet not found, creating new one.")
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
        
        values = [['Timestamp', 'Platform', 'Username', 'Source URL', 'Title', 'Description', 'Tags', 'Drive URL', 'Status']]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Queue!A1:I1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()
    
    try:
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=permission
        ).execute()
        logging.debug(f"Spreadsheet URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    except Exception as e:
        logging.error(f"Error making spreadsheet public: {e}")
    
    return spreadsheet_id

def get_sheet():
    logging.debug("Attempting to get sheet.")
    try:
        sheets_service, _ = get_google_services()
        spreadsheet_id = get_or_create_spreadsheet()
        return (sheets_service, spreadsheet_id)
    except Exception as e:
        logging.error(f"Error getting sheet: {e}")
        return None

def is_url_in_queue(url, sheet_info):
    logging.debug(f"Checking if URL is in queue: {url}")
    try:
        if not sheet_info:
            return False
            
        sheets_service, spreadsheet_id = sheet_info
        
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='Queue!A:I'
        ).execute()
        
        values = result.get('values', [])
        
        for row in values:
            if len(row) > 3 and url in row[3]:  
                logging.debug(f"URL found in queue: {url}")
                return True
        logging.debug(f"URL not found in queue: {url}")
        return False
    except Exception as e:
        logging.error(f"Error checking queue: {e}")
        return False

def get_platform_and_username(url):
    logging.debug(f"Getting platform and username for URL: {url}")
    if "tiktok.com" in url:
        match = re.search(r'@([^/]+)', url)
        if match:
            logging.debug(f"Platform and username found: TikTok, {match.group(1)}")
            return "TikTok", match.group(1)
    elif "youtube.com" in url or "youtu.be" in url:
        logging.debug(f"Platform found: YouTube")
        return "YouTube", None  
    elif "tumblr.com" in url:
        match = re.search(r'//([^.]+)\.tumblr\.com', url)
        if match:
            logging.debug(f"Platform and username found: Tumblr, {match.group(1)}")
            return "Tumblr", match.group(1)
    elif "pinterest.com" in url or "pin.it" in url:
        logging.debug(f"Platform found: Pinterest")
        return "Pinterest", None  
    elif "instagram.com" in url:
        logging.debug(f"Platform found: Instagram")
        return "Instagram", None  
        
    logging.debug(f"Platform and username not found for URL: {url}")
    return None, None

def add_to_queue(video_path, metadata):
    logging.debug(f"Adding video to queue: {video_path}")
    try:
        sheet_info = get_sheet()
        if not sheet_info:
            logging.error("Failed to get sheet")
            return

        sheets_service, spreadsheet_id = sheet_info

        if is_url_in_queue(metadata['webpage_url'], sheet_info):
            logging.debug(f"Video already in queue: {metadata['webpage_url']}")
            if os.path.exists(video_path):
                logging.debug("Removing local file as it's already in queue")
                os.remove(video_path)  
            return

        drive_url = upload_to_drive(video_path)
        if not drive_url:
            logging.error("Failed to upload to Drive")
            return

        timestamp = datetime.now().isoformat()  
        row = [[
            timestamp,
            metadata['platform'],
            metadata['username'],
            metadata['webpage_url'],  
            metadata.get('title', 'Untitled'),
            metadata.get('description', ''),
            ','.join(metadata.get('tags', [])),
            drive_url,  
            'pending'  
        ]]

        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range='Queue!A:I',  
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body={'values': row}
        ).execute()

        logging.debug(f"Successfully added to queue: {metadata.get('title', 'Untitled')}")

        if os.path.exists(video_path):
            logging.debug(f"Removing local file: {video_path}")
            os.remove(video_path)

    except Exception as e:
        logging.error(f"Error adding to queue: {e}")
        if os.path.exists(video_path):
            logging.debug(f"Removing local file after error: {video_path}")
            os.remove(video_path)  

def upload_to_drive(video_path):
    logging.debug(f"Uploading video to Drive: {video_path}")
    try:
        file_metadata = {'name': video_path.split('/')[-1]}
        media = MediaFileUpload(video_path, mimetype='video/mp4')
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='webViewLink').execute()
        return file.get('webViewLink')
    except Exception as e:
        logging.error(f"Error uploading to Drive: {str(e)}")
        return None

def update_google_sheet(metadata, drive_url):
    logging.debug("Updating Google Sheet with video metadata.")
    try:
        # Ensure all metadata keys are present
        metadata.setdefault('title', 'Untitled')
        metadata.setdefault('uploader', 'Unknown')
        metadata.setdefault('description', '')
        metadata.setdefault('tags', [])
        metadata.setdefault('source_url', '')
        
        # Prepare the data to match the spreadsheet columns
        values = [[
            datetime.now().isoformat(),  # Timestamp
            'Instagram',  # Platform
            metadata['uploader'],  # Username
            metadata['source_url'],  # Source URL
            metadata['title'],  # Title
            metadata['description'],  # Description
            ', '.join(metadata['tags']),  # Tags
            drive_url,  # Drive URL
            'pending'  # Status
        ]]
        body = {'values': values}
        range_name = f"{SHEET_NAME}!A1"
        sheets_service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=range_name,
                                                      valueInputOption="RAW", body=body).execute()
    except Exception as e:
        logging.error(f"Error updating Google Sheet: {str(e)}")

def process_video_data(video_path, metadata):
    logging.info(f"Uploading video to Google Drive: {video_path}")
    try:
        drive_url = upload_to_drive(video_path)
        logging.info(f"Video uploaded to Google Drive: {drive_url}")
        
        # Ensure all metadata keys are present
        metadata.setdefault('title', 'Untitled')
        metadata.setdefault('uploader', 'Unknown')
        metadata.setdefault('description', '')
        metadata.setdefault('tags', [])
        metadata.setdefault('source_url', '')
        
        # Determine platform from source URL
        if "tiktok.com" in metadata['source_url']:
            platform = 'TikTok'
            username = metadata.get('uploader', 'Unknown')  # TikTok username
        elif "instagram.com" in metadata['source_url']:
            platform = 'Instagram'
            username = metadata.get('channel', 'Unknown')  # Instagram channel
        else:
            platform = 'Unknown'
            username = 'Unknown'
        
        # Prepare the data to match the spreadsheet columns
        values = [[
            datetime.now().isoformat(),  # Timestamp
            platform,  # Platform
            username,  # Username
            metadata['source_url'],  # Source URL
            metadata['title'],  # Title
            metadata['description'],  # Description
            ', '.join(metadata['tags']),  # Tags
            drive_url,  # Drive URL
            'pending'  # Status
        ]]
        body = {'values': values}
        range_name = f"{SHEET_NAME}!A1"
        sheets_service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=range_name,
                                                      valueInputOption="RAW", body=body).execute()
    except Exception as e:
        logging.error(f"Error processing video data: {str(e)}")

def upload_video_to_drive(video_data):
    logging.debug("Uploading video to Google Drive...")
    try:
        # Implement upload logic here
        logging.debug("Video uploaded to Google Drive successfully.")
    except Exception as e:
        logging.error(f"Error uploading video to Google Drive: {str(e)}")

def add_to_google_sheet(metadata):
    logging.debug("Adding video information to Google Sheet...")
    try:
        sheet_info = get_sheet()
        if not sheet_info:
            logging.error("Failed to get sheet")
            return

        sheets_service, spreadsheet_id = sheet_info

        row = [
            metadata.get('timestamp', ''),
            metadata.get('platform', ''),
            metadata.get('username', ''),
            metadata.get('webpage_url', ''),
            metadata.get('title', 'Untitled'),
            metadata.get('description', ''),
            ','.join(metadata.get('tags', [])),
            'Drive URL',  
            'pending'
        ]

        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range='Queue!A:I',
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body={'values': [row]}
        ).execute()
        
        logging.debug("Video information added to Google Sheet successfully.")
    except Exception as e:
        logging.error(f"Error adding to Google Sheet: {str(e)}")

def download_video_tiktok(url):
    logging.debug(f"Downloading TikTok video: {url}")
    try:
        ydl_opts = {
            'format': 'best[ext=mp4]',
            'outtmpl': os.path.join(OUTPUT_FOLDER, '%(extractor_key)s_%(id)s.%(ext)s'),
            'quiet': True
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            video_path = ydl.prepare_filename(info)
            logging.debug(f"Video downloaded to: {video_path}")
            return video_path, info
    except Exception as e:
        logging.error(f"Failed to download TikTok video: {url}. Error: {str(e)}")
        return None, None

def download_video_instagram(url):
    logging.debug(f"Downloading Instagram video: {url}")
    try:
        ydl_opts = {
            'format': 'best[ext=mp4]',
            'outtmpl': os.path.join(OUTPUT_FOLDER, '%(extractor_key)s_%(id)s.%(ext)s'),
            'quiet': True
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            video_path = ydl.prepare_filename(info)
            logging.debug(f"Video downloaded to: {video_path}")
            return video_path, info
    except Exception as e:
        logging.error(f"Failed to download Instagram video: {url}. Error: {str(e)}")
        return None, None

def process_url(url):
    logging.info(f"Processing URL: {url}")
    try:
        if "tiktok.com" in url:
            logging.debug("Detected TikTok URL.")
            video_path, info = download_video_tiktok(url)
        elif "instagram.com" in url:
            logging.debug("Detected Instagram URL.")
            video_path, info = download_video_instagram(url)
        else:
            logging.warning("Unsupported URL format.")
            return None, None

        if not video_path or not info:
            logging.warning("Failed to download video or extract metadata.")
            return None, None

        # Add source URL to metadata
        info['source_url'] = url

        logging.info("Video processing completed.")
        process_video_data(video_path, info)
        return video_path, info
    except Exception as e:
        logging.error(f"Error processing URL: {url}. Error: {str(e)}")
        return None, None

# Flask Routes

@app.route('/', methods=['GET'])
def home():
    logging.debug("Home route accessed.")
    return jsonify({'message': 'Welcome to the Video Batch Processor'})

@app.route('/process', methods=['POST'])
def process_video():
    logging.debug("Process video route accessed.")
    url = request.json.get('url')
    process_url(url)
    return jsonify({'message': 'Processing started'})

# Main

def main():
    logging.debug("Main function called.")
    parser = argparse.ArgumentParser(description='Process video URLs.')
    parser.add_argument('url', type=str, help='The URL of the video to process')
    args = parser.parse_args()
    process_url(args.url)

if __name__ == "__main__":
    logging.debug("App running.")
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
