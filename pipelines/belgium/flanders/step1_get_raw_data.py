import os
import requests
from datetime import datetime


def download_pdf_pipeline_step(type='plen', dagen=100, limiet=25, datumvan='01092024', datumtot='21102024',
                               output_dir='raw'):
    def sanitize_filename(name):
        """Sanitize the filename by replacing or removing invalid characters."""
        invalid_chars = ":?\"<>|*\\/"
        for char in invalid_chars:
            name = name.replace(char, '_')
        return name

    # Ensure the data directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Construct the API URL
    api_url = f'https://ws.vlpar.be:443/e/opendata/verg/vorige'
    params = {
        'type': type,
        'dagen': dagen,
        'limiet': limiet,
        'datumvan': datumvan,
        'datumtot': datumtot
    }

    # Make the API request
    headers = {'accept': 'application/json;charset=UTF-8'}
    response = requests.get(api_url, params=params, headers=headers)

    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")

    json_data = response.json()
    created_files = []
    links=[]
    youtubes = []
    # Iterate over each item in the 'items' list
    for item in json_data['items']:
        vergadering = item.get('vergadering', {})
        pdf_path = vergadering.get('plenairehandelingen', {}).get('pdffilewebpath')
        if pdf_path:
            # Construct filename from vergadering details
            date_str = vergadering.get('datumbegin', '').split('T')[0]
            date_formatted = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%m%d') if date_str else 'unknown_date'

            description = vergadering.get('omschrijving-kort', [])
            descriptor = description[0] if description else f"id_{vergadering.get('id', 'unknown')}"
            # get youtube link
            youtube =  vergadering.get('video-youtube-id', 'None')
            youtubes.append(youtube)
            # Create a clean filename
            base_filename = f"{date_formatted}_{descriptor}"
            pdf_filename = sanitize_filename(base_filename) + '.pdf'

            # Specify the local path to save the PDF
            local_path = os.path.join(output_dir, pdf_filename)

            # Download the PDF file
            print(pdf_path)
            links.append(pdf_path)
            try:
                pdf_response = requests.get(pdf_path)

                # Check if the request was successful and the content is non-empty
                if pdf_response.status_code == 200 and len(pdf_response.content) > 0:
                    if 'application/pdf' in pdf_response.headers.get('Content-Type', ''):
                        # Save the PDF file to disk
                        with open(local_path, 'wb') as f:
                            f.write(pdf_response.content)
                        print(f'Downloaded {pdf_filename} to {local_path}')
                        created_files.append(local_path)
                    else:
                        print(f"Failed to download {pdf_filename}. Content-Type is not PDF.")
                else:
                    print(
                        f"Failed to download {pdf_filename}. Response status code: {pdf_response.status_code}, content length: {len(pdf_response.content)}")
            except:
                print('failed')

    return created_files, links, youtubes