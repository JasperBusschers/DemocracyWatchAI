import json
import os
import requests
from datetime import datetime


def sanitize_filename(name):
    """Sanitize the filename by replacing or removing invalid characters."""
    invalid_chars = ":?\"<>|*\\/"
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name


# Ensure the data directory exists
os.makedirs('raw', exist_ok=True)

# Path to your JSON file
json_file_path = 'response.json'

# Read JSON data from a file using UTF-8 encoding
with open(json_file_path, 'r', encoding='utf-8') as file:
    json_data = json.load(file)

# Iterate over each item in the 'items' list
for item in json_data['items']:
    vergadering = item.get('vergadering', {})
    pdf_path = vergadering.get('plenairehandelingen', {}).get('pdffilewebpath')
    if pdf_path:
        # Construct filename from vergadering details
        date_str = vergadering.get('datumbegin', '').split('T')[0]
        if date_str:
            date_formatted = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%m%d')
        else:
            date_formatted = 'unknown_date'

        description = vergadering.get('omschrijving-kort', [])
        if description:
            descriptor = description[0]
        else:
            descriptor = f"id_{vergadering.get('id', 'unknown')}"

        # Create a clean filename
        base_filename = f"{date_formatted}_{descriptor}"
        pdf_filename = sanitize_filename(base_filename) + '.pdf'

        # Specify the local path to save the PDF
        local_path = os.path.join('raw', pdf_filename)
        # Download the PDF file
        response = requests.get(pdf_path)
        # Check if the request was successful
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            print(f'Downloaded {pdf_filename} to {local_path}')
        else:
            print(f'Failed to download {pdf_filename}')
