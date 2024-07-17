import os
from api.s3_api import download_images, fetch_alerts
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaFileUpload

from utils.states import save_state, load_state


# Funções para autenticação e upload para o Google Drive
def authenticate_gdrive():
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)


def upload_file(service, local_file_path, folder_id):
    file_name = os.path.basename(local_file_path)
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(local_file_path, resumable=True)
    file = service.files().create(body=file_metadata,
                                  media_body=media, fields='id').execute()
    print(f"File '{file_name}' uploaded successfully to Google Drive.")


def upload_images_and_labels(images_dir, labels_dir, gdrive_folder_id):
    service = authenticate_gdrive()
    for root, _, files in os.walk(images_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            upload_file(service, local_file_path, gdrive_folder_id)
    for root, _, files in os.walk(labels_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            upload_file(service, local_file_path, gdrive_folder_id)


if __name__ == "__main__":
    batch_size = 100 
    offset = load_state()

    while True:
        alerts = fetch_alerts(batch_size, offset)
        if not alerts:
            break

        download_images(alerts, 'dataset/images')
        upload_images_and_labels(
            'dataset/images', 'dataset/labels', 'GOOGLE_DRIVE_FOLDER_ID')

        offset += batch_size
        save_state(offset)
