import os
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

load_dotenv()

SCOPES = [
    os.getenv('GOOGLE_SHEET_SCOPES')
    or 'https://www.googleapis.com/auth/spreadsheets.readonly'
]
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
SHEET_ID = (
    os.getenv('GOOGLE_GOOGLE_SHEET_ID')
    or os.getenv('GOOGLE_SHEET_ID')
)

def normalize_name(name):
    return str(name).strip().lower().replace(' ', '')

def get_gsheet_worksheet(title):
    try:
        credentials = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        gc = gspread.authorize(credentials)
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet(title)
        return ws
    except WorksheetNotFound:
        return None
    except Exception:
        return None

def get_all_sheet_data(tab_name):
    ws = get_gsheet_worksheet(tab_name)
    if ws is None:
        return [], f"Sheet/tab '{tab_name}' not found in Google Sheets."
    try:
        data = ws.get_all_records()
        return data, None
    except Exception as e:
        return [], f"Could not fetch data from '{tab_name}': {str(e)}"

def get_sheet_data_for_user(tab_name, user_full_name):
    data, error = get_all_sheet_data(tab_name)
    if error:
        return [], error
    user_key = normalize_name(user_full_name)
    user_rows = []
    for row in data:
        row_kam = normalize_name(
            row.get('KAM Name', '') or row.get('KAM_Name', '') or ''
        )
        if user_key == row_kam:
            user_rows.append(row)
    return user_rows, None

def filter_rows(rows, **kwargs):
    filtered = rows
    for key, value in kwargs.items():
        filtered = [
            row for row in filtered
            if str(row.get(key, '')).strip() == str(value).strip()
        ]
    return filtered

def get_unique_customer_names_from_sheet(tab_name="Sheet1", kam_name=None):
    data, error = get_all_sheet_data(tab_name)
    if error or not data:
        return []
    names = set()
    for row in data:
        if kam_name:
            row_kam = (
                row.get('KAM Name') or row.get('KAM_Name') or row.get('kam') or ''
            )
            if normalize_name(row_kam) != normalize_name(kam_name):
                continue
        name = (
            row.get('Customer Name')
            or row.get('Customer_Name')
            or row.get('customer_name')
        )
        if name:
            names.add(str(name).strip())
    return [(n, n) for n in sorted(names)]

def get_unique_kam_names_from_sheet(tab_name="Sheet1"):
    data, error = get_all_sheet_data(tab_name)
    if error or not data:
        return []
    names = set()
    for row in data:
        name = (
            row.get('KAM Name')
            or row.get('KAM_Name')
            or row.get('kam_name')
            or row.get('kam')
        )
        if name:
            names.add(str(name).strip())
    return [(n, n) for n in sorted(names)]

def get_unique_location_from_sheet(tab_name="Sheet1"):
    possible_keys = ["Location", "location", "Dispatch From", "dispatch_from"]
    data, error = get_all_sheet_data(tab_name)
    if error or not data:
        return []
    locations = set()
    for row in data:
        val = None
        for key in possible_keys:
            if row.get(key):
                val = row.get(key)
                break
        if val:
            locations.add(str(val).strip())
    return [(v, v) for v in sorted(locations)]

def get_unique_column_values_from_sheet(column_name, tab_name="Sheet1"):
    data, error = get_all_sheet_data(tab_name)
    if error or not data:
        return []
    keys = [
        column_name,
        column_name.replace(" ", "_"),
        column_name.lower(),
        column_name.title(),
    ]
    values = set()
    for row in data:
        val = None
        for key in keys:
            if row.get(key):
                val = row.get(key)
                break
        if val:
            values.add(str(val).strip())
    return [(v, v) for v in sorted(values)]
