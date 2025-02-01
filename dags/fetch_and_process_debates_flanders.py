"""
DAG A: Full Pipeline Example
1. Download PDFs + XML (now extracting topic info into JSON instead of saving XML files)
2. Convert PDFs to Markdown
3. Convert Markdown to JSON (including topic info from XML)

Make sure to set the OPENAI_KEY environment variable in your Airflow environment/container.
"""

import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Import your class from the external module
from markdown_parsers.flanders_transcript import MarkdownToJsonPipelineStep


# ------------- STEP 1 FUNCTIONS (Download PDFs + Extract XML Topic Info) ------------- #

import requests
import xml.etree.ElementTree as ET


def process_document_xml(url,output_dir):
    """
    This function downloads the XML from the provided URL and extracts topic information.
    Instead of saving a file, it returns a list of dictionaries, each containing
    'title', 'date', 'link', and the first 1000 tokens of 'text'.
    """
    try:
        response = requests.get(url, headers={'Accept': 'application/xml'})
        if response.status_code != 200:
            print(f"Failed to retrieve XML from {url}. Status Code: {response.status_code}")
            return []

        xml_content = response.content
        root = ET.fromstring(xml_content)
        topics = []

        for doc in root.findall('.//document'):
            titel = doc.findtext('titel', default='unknown_titel')
            link = doc.findtext('url', default='unknown_url')
            date = doc.findtext('datum', default='unknown_date')

            output_filepath=output_dir+titel+'.pdf'
            try:
                pdf_response = requests.get(link)
                if pdf_response.status_code == 200 and len(pdf_response.content) > 0:
                    if 'application/pdf' in pdf_response.headers.get('Content-Type', ''):
                        with open(output_filepath, 'wb') as f:
                            f.write(pdf_response.content)
            except Exception as e:
                print(f"Failed to download PDF from {link}. Error: {e}")

            topics.append({
                'title': titel.split(' ')[0],
                'date': date,
                'link': link,
                'filepath': output_filepath
            })

        return topics

    except Exception as e:
        print(f"An error occurred while processing XML: {str(e)}")
        return []


    except Exception as e:
        print(f"Error processing XML from {url}: {e}")
        return []


def download_pdf_pipeline_step(
    type='comm',
    dagen=100,
    limiet=500,
    datumvan='01092024',
    datumtot='21102024',
    output_dir='/usr/local/airflow/data'
):
    def sanitize_filename(name):
        invalid_chars = ":?\"<>|*\\/"
        for char in invalid_chars:
            name = name.replace(char, '_')
        return name

    os.makedirs(output_dir, exist_ok=True)

    api_url = 'https://ws.vlpar.be:443/e/opendata/verg/vorige'
    params = {
        'type': type,
        'dagen': dagen,
        'limiet': limiet,
        'datumvan': datumvan,
        'datumtot': datumtot
    }

    headers = {'accept': 'application/json;charset=UTF-8'}
    response = requests.get(api_url, params=params, headers=headers)
    if response.status_code != 200:
        print(response.content)
        raise Exception(f"API request failed with status code {response.status_code}")

    json_data = response.json()
    created_files = []
    links = []
    youtubes = []
    topics_info_all = []  # To accumulate XML topic info across meetings.

    for item in json_data['items']:
        vergadering = item.get('vergadering', {})
        meeting_id = vergadering.get('id')
        link_for_xml = f"https://ws.vlpar.be/e/opendata/verg/{meeting_id}/hand?idPrsHighlight=0"

        # Extract XML topic text/info
        topics_info = process_document_xml(url=link_for_xml, output_dir=output_dir)
        for topic in topics_info:
            topic['meeting_id'] = meeting_id
        topics_info_all.append(topics_info)

        # Download PDF (for this meeting)
        pdf_key = 'plenairehandelingen' if type == 'plen' else 'commissiehandelingen'
        pdf_path = vergadering.get(pdf_key, {}).get('pdffilewebpath')
        if pdf_path:
            date_str = vergadering.get('datumbegin', '').split('T')[0]
            if date_str:
                try:
                    date_formatted = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%m%d')
                except ValueError:
                    date_formatted = 'unknown_date'
            else:
                date_formatted = 'unknown_date'

            description = vergadering.get('omschrijving-kort', [])
            descriptor = description[0] if description else f"id_{meeting_id}"
            youtube = vergadering.get('video-youtube-id', 'None')
            youtubes.append(youtube)

            base_filename = f"{date_formatted}_{descriptor}"
            pdf_filename = sanitize_filename(base_filename) + '.pdf'
            local_path = os.path.join(output_dir, pdf_filename)

            print(f"Downloading PDF: {pdf_path}")
            links.append(pdf_path)
            try:
                pdf_response = requests.get(pdf_path)
                if pdf_response.status_code == 200 and len(pdf_response.content) > 0:
                    if 'application/pdf' in pdf_response.headers.get('Content-Type', ''):
                        with open(local_path, 'wb') as f:
                            f.write(pdf_response.content)
                        print(f"Saved PDF to {local_path}")
                        created_files.append(local_path)
                    else:
                        print(f"Failed to download {pdf_filename}. Not a PDF.")
                else:
                    print(
                        f"Failed to download {pdf_filename}. "
                        f"Status code: {pdf_response.status_code}, "
                        f"content length: {len(pdf_response.content)}"
                    )
            except Exception as e:
                print(f"Failed to download PDF from {pdf_path}. Error: {e}")

    return {
        'pdf_files': created_files,
        'pdf_links': links,
        'youtubes': youtubes,
        'topics_info': topics_info_all
    }


# ------------- STEP 2 FUNCTION (PDF -> Markdown) ------------- #
def pdf_to_markdown_pipeline_step(pdf_files, output_dir='/usr/local/airflow/data/markdown'):
    import os
    import pathlib
    import pymupdf4llm
    from concurrent.futures import ThreadPoolExecutor

    os.makedirs(output_dir, exist_ok=True)

    def pdf_to_markdown(pdf_path, markdown_path):
        md_text = pymupdf4llm.to_markdown(pdf_path)
        pathlib.Path(markdown_path).write_bytes(md_text.encode())

    def process_pdf(pdf_path):
        if pdf_path.endswith('.pdf'):
            filename = os.path.basename(pdf_path)
            md_filename = filename.replace('.pdf', '.md')
            md_path = os.path.join(output_dir, md_filename)

            if os.path.exists(md_path):
                print(f"Skipping {pdf_path}, Markdown file already exists.")
                return md_path

            print(f"Converting {pdf_path} to Markdown...")
            pdf_to_markdown(pdf_path, md_path)
            return md_path
        else:
            print(f"Skipping non-PDF: {pdf_path}")
            return None

    processed_files = []

    with ThreadPoolExecutor() as executor:
        results = executor.map(process_pdf, pdf_files)

    processed_files = list(results)

    print(f"Converted {len(processed_files)} PDF files to Markdown.")
    return processed_files



# ------------- STEP 3 (Markdown -> JSON) using the imported class ------------- #

def step3_md_to_json(**context):
    """
    Pulls the Markdown list, PDF links, YouTube links, and topics info from XCom,
    then processes them into JSON using the OpenAI API (embedding),
    and embeds the topic info (title, date, link) into the final JSON.
    """
    ti = context['ti']
    data = ti.xcom_pull(task_ids='convert_to_markdown')
    markdown_files = data.get('markdown_files', [])
    pdf_links = data.get('pdf_links', [])
    youtubes = data.get('youtubes', [])
    topics_info = data.get('topics_info', [])

    # Retrieve your OpenAI API key from environment variables
    api_key = os.getenv('OPENAI_KEY')
    if not api_key:
        raise ValueError("OPENAI_KEY not found in environment variables.")

    # Instantiate and run your class-based pipeline
    pipeline = MarkdownToJsonPipelineStep(
        markdown_files=markdown_files,
        pdf_links=pdf_links,
        youtubes=youtubes,
        topics_info=topics_info,
        api_key=api_key,
        output_dir='/usr/local/airflow/data/json'
    )
    json_files = pipeline.run()

    return {'json_files': json_files}


# ------------- BUILD THE DAG ------------- #

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1
}

with DAG(
    dag_id='download_debates_flanders',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    params={
        "start_date_param": "01092024",  # default
        "end_date_param": "21102024"     # default
    }
) as dag:

    def step1_download(**context):
        """
        Calls download_pdf_pipeline_step for both 'plen' and 'comm'
        and returns combined results (files, links, youtubes, topics_info).
        Reads start/end dates from DAG params.
        """
        start_date = context['params'].get('start_date_param', '01092024')
        end_date = context['params'].get('end_date_param', '21102024')

        # Download 'plen'
        result1 = download_pdf_pipeline_step(
            type='plen',
            output_dir='/usr/local/airflow/data',
            datumvan=start_date,
            datumtot=end_date
        )
        # Optionally run 'comm'
        result2 = download_pdf_pipeline_step(
            type='comm',
            output_dir='/usr/local/airflow/data',
            datumvan=start_date,
            datumtot=end_date
        )

        # Combine them
        combined_pdf_files = result1['pdf_files'] + result2['pdf_files']
        combined_pdf_links = result1['pdf_links'] + result2['pdf_links']
        combined_youtubes = result1['youtubes'] + result2['youtubes']
        combined_topics_info = result1['topics_info'] + result2['topics_info']

        return {
            'pdf_files': combined_pdf_files,
            'pdf_links': combined_pdf_links,
            'youtubes': combined_youtubes,
            'topics_info': combined_topics_info
        }

    def step2_pdf_to_md(**context):
        """
        Pulls the PDF list and topics info from XCom, converts PDFs to Markdown,
        and returns Markdown paths along with pdf_links, youtubes, and topics_info.
        """
        ti = context['ti']
        data = ti.xcom_pull(task_ids='download_pdfs')
        pdf_files = data.get('pdf_files', [])
        topics_info = data.get('topics_info', [])
        topic_files = [topic['filepath'] for sublist in topics_info for topic in sublist]
        markdowns_topics = pdf_to_markdown_pipeline_step(
            pdf_files=set(topic_files),
            output_dir='/usr/local/airflow/data/markdown/topics'
        )
        markdowns = pdf_to_markdown_pipeline_step(
            pdf_files=pdf_files,
            output_dir='/usr/local/airflow/data/markdown'
        )
        return {
            'markdown_files': markdowns,
            'pdf_links': data.get('pdf_links', []),
            'youtubes': data.get('youtubes', []),
            'topics_info': data.get('topics_info', [])
        }

    download_pdfs_task = PythonOperator(
        task_id='download_pdfs',
        python_callable=step1_download,
        provide_context=True
    )

    convert_to_markdown_task = PythonOperator(
        task_id='convert_to_markdown',
        python_callable=step2_pdf_to_md,
        provide_context=True
    )

    convert_to_json_task = PythonOperator(
        task_id='convert_to_json',
        python_callable=step3_md_to_json,
        provide_context=True
    )

    # Define your task dependencies
    download_pdfs_task >> convert_to_markdown_task >> convert_to_json_task
