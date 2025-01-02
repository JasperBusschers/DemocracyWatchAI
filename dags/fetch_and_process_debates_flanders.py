"""
DAG A: Full Pipeline Example
1. Download PDFs + XMLs
2. Convert PDFs to Markdown
3. Convert Markdown to JSON

Make sure to set the OPENAI_KEY environment variable in your Airflow environment/container.
"""

import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# ------------- STEP 1 FUNCTIONS (Download PDFs + XML) ------------- #

def process_document_xml(url, output_dir='/usr/local/airflow/data/topics_texts'):
    def sanitize_filename(name):
        invalid_chars = ":?\"<>|*\\/"
        for char in invalid_chars:
            name = name.replace(char, '_')
        return name

    os.makedirs(output_dir, exist_ok=True)

    try:
        response = requests.get(url, headers={'Accept': 'application/xml'})
        if response.status_code != 200:
            print(f"Failed to retrieve XML from {url}. Status Code: {response.status_code}")
            return []

        xml_content = response.content
        root = ET.fromstring(xml_content)

        saved_files = []
        for doc in root.findall('.//document'):
            bestandsnaam = doc.findtext('bestandsnaam', default='unknown_filename')
            titel = doc.findtext('titel', default='unknown_titel')
            base_filename = f"{titel}_{bestandsnaam}"
            filename = sanitize_filename(base_filename) + '.xml'
            file_path = os.path.join(output_dir, filename)
            ET.ElementTree(doc).write(file_path, encoding='utf-8', xml_declaration=True)

            print(f"Saved XML document: {file_path}")
            saved_files.append(file_path)
        return saved_files

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

    for item in json_data['items']:
        vergadering = item.get('vergadering', {})
        link = f"https://ws.vlpar.be/e/opendata/verg/{vergadering.get('id')}/agd?aanpassingen=nee"

        # Download related XMLs
        process_document_xml(url=link, output_dir=os.path.join(output_dir, 'topics_texts'))

        # Download PDF
        pdf_key = 'plenairehandelingen' if type == 'plen' else 'commissiehandelingen'
        pdf_path = vergadering.get(pdf_key, {}).get('pdffilewebpath')
        if pdf_path:
            date_str = vergadering.get('datumbegin', '').split('T')[0]
            date_formatted = (
                datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%m%d')
                if date_str
                else 'unknown_date'
            )
            description = vergadering.get('omschrijving-kort', [])
            descriptor = description[0] if description else f"id_{vergadering.get('id', 'unknown')}"
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

    return created_files, links, youtubes


# ------------- STEP 2 FUNCTION (PDF -> Markdown) ------------- #

def pdf_to_markdown_pipeline_step(pdf_files, output_dir='/usr/local/airflow/data/markdown'):
    import pathlib
    import pymupdf4llm

    os.makedirs(output_dir, exist_ok=True)

    def pdf_to_markdown(pdf_path, markdown_path):
        md_text = pymupdf4llm.to_markdown(pdf_path)
        pathlib.Path(markdown_path).write_bytes(md_text.encode())

    processed_files = []
    for pdf_path in pdf_files:
        if pdf_path.endswith('.pdf'):
            filename = os.path.basename(pdf_path)
            md_filename = filename.replace('.pdf', '.md')
            md_path = os.path.join(output_dir, md_filename)
            print(f"Converting {pdf_path} to Markdown...")
            pdf_to_markdown(pdf_path, md_path)
            processed_files.append(md_path)
        else:
            print(f"Skipping non-PDF: {pdf_path}")

    print(f"Converted {len(processed_files)} PDF files to Markdown.")
    return processed_files


# ------------- STEP 3 FUNCTION (Markdown -> JSON) ------------- #
# -- Replaced with your NEW code snippet below --

import re
import traceback
import uuid
import json
import requests

def markdown_to_json_pipeline_step(markdown_files, pdf_links, youtubes, api_key, output_dir='json'):
    os.makedirs(output_dir, exist_ok=True)

    def get_embedding(text):
        url = "https://api.openai.com/v1/embeddings"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        data = {
            "input": text,
            "model": "text-embedding-3-small"
        }
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            return response.json()['data'][0]['embedding']
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    def extract_types_and_topics(text_lines, people):
        result = []
        all_names = people['present'] + people['absent_with_notice'] + people['absent_without_notice']

        # Remove leading/trailing spaces and deduplicate the names
        unique_names = list(set([name.strip() for name in all_names]))
        sections = []
        section = ''
        title = ''
        last_word_upper = False
        i = 0
        for line in text_lines:
            for word in line.split(" "):
                if word.replace(" ", "").replace(':', '').isupper() and last_word_upper:
                    title += ' ' + word
                    last_word_upper = True
                elif word.replace(" ", "").replace(':', '').isupper() and not last_word_upper and word != '':
                    if "Hoofdelijke stemming" in section:
                        sections.append([title + " : " + section, i])
                        last_word_upper = True
                        i += 1
                    else:
                        last_word_upper = True
                        sections.append([title + " : " + section, None])
                    section = ''
                    title = word
                else:
                    section += ' ' + word
                    last_word_upper = False
        for text, vote in sections:
            line = text.replace('\n', '')  # Remove newline characters
            match = re.match(r'((?:[A-Z]{2,}\s+){1,}[A-Z]{2,})(.*)', line.strip())
            for word in line.split(" "):
                if word.isupper() and last_word_upper:
                    title += ' ' + word
                elif word.isupper() and not last_word_upper and word != '':
                    if "Hoofdelijke stemming" in section:
                        sections.append([section, i])
                        i += 1
                    else:
                        sections.append([section, None])
                    section = ''
                    title = word
                else:
                    section += ' ' + word

            if match:
                type_part = match.group(1).strip()  # The capitalized part
                topic_part = match.group(2).strip()  # The lowercase part

                # Attempt to extract people from the text
                people_match = re.search(r'van (.*?)over', topic_part)
                people_found = people_match.group(1).strip() if people_match else None
                people_found = people_found.split(', ') if people_found is not None else []

                people_final = []
                for name in unique_names:
                    if name in topic_part:
                        people_final.append(format_name(name))

                result.append({
                    'id': str(uuid.uuid4()),
                    'type': type_part,
                    'topic': topic_part,
                    'vector': get_embedding(topic_part),
                    'people': people_final,
                    'voted': 'Hoofdelijke stemming' in line
                })

        return result

    def process_second_section(text, people, link):
        lines = text.split('\n\n')
        # Attempt to parse name/date from line 1
        try:
            name, date = lines[1].split(' – ')
        except:
            name, date = "Unknown", "Unknown"

        meeting = {
            "name": name,
            "date": date,
            "sections": [],
            "link": link,
            "country": 'Belgium',
            "region": 'Flanders'
        }

        # Attempt to find "INHOUD"
        if 'INHOUD' in lines[2]:
            meeting['sections'] = extract_types_and_topics(lines[2:-1], people)

        return meeting

    def jaccard_similarity_words(str1, str2, threshold=0.4):
        set1 = set(str1.split())
        set2 = set(str2.split())
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        similarity = intersection / union if union != 0 else 0
        return similarity >= threshold

    def process_debate(debate, people, topics):
        all_names = people['present'] + people['absent_with_notice'] + people['absent_without_notice']
        unique_names = list(set([name.strip() for name in all_names]))
        lines = debate.split('\n\n')
        statements = []
        text = ''
        topic = []
        last_was_topic = False

        for i, l in enumerate(lines):
            is_topic = False
            if i > 1 and ':' in l and "**" in l:
                statement = text.replace('**', '')
                for t in topics:
                    if jaccard_similarity_words(t[1], statement) and last_was_topic:
                        topic.append(t[0])
                        last_was_topic = True
                        is_topic = True
                    elif jaccard_similarity_words(t[1], statement):
                        topic = [t[0]]
                        last_was_topic = True
                        is_topic = True
                if not is_topic:
                    last_was_topic = False
                    statements.append([statement, topic])
                text = l
            else:
                text += l

        previous_statement = ''
        id_ = str(uuid.uuid4())
        next_statement = str(uuid.uuid4())
        output = []

        for s, tpcs in statements:
            person = s.split(':')[0]
            person = person.replace('Minister ', '')
            if 'De voorzitter' not in person:
                if '(' in person:
                    person, party = person.split('(')
                    party = party.replace(')', '')
                else:
                    party = None
                person_ref = None
                for n in unique_names:
                    if format_name(n) in format_name(person):
                        person_ref = format_name(n)
                statement_text = s.split(':', 1)[1] if len(s.split(':')) > 1 else ""
                json_output = {
                    'id': id_,
                    'topic': tpcs,
                    'person_ref': person_ref,
                    'previous': previous_statement,
                    "next": next_statement,
                    "Party": party,
                    "person": person,
                    "statement": statement_text,
                    'vector': get_embedding(statement_text)
                }
                output.append(json_output)
                previous_statement = id_
                id_ = next_statement
                next_statement = str(uuid.uuid4())

        return output

    def format_name(name: str) -> str:
        name = name.strip()
        # Insert a space between a lowercase letter followed by an uppercase letter
        name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name).replace('-', ' ').replace('.', '')
        return name

    def process_votes(votes, people, topics):
        all_names = people['present'] + people['absent_with_notice'] + people['absent_without_notice']
        unique_names = list(set([name.strip() for name in all_names]))
        votes_res = {'topics': topics, 'votes': []}

        for vote in votes:
            if 'A-stemmen:' in vote:
                vote = vote.split('A-stemmen:')[1]
                if 'NEEN-stemmen:' in vote:
                    yes, vote = vote.split('NEEN-stemmen:')
                    if 'ONTHOUDINGEN:' in vote:
                        no, abstained = vote.split('ONTHOUDINGEN:')
                    else:
                        abstained = ''
                        no = vote
                elif 'ONTHOUDINGEN:' in vote:
                    yes, abstained = vote.split('ONTHOUDINGEN:')
                    no = ''
                else:
                    yes = vote
                    no = ''
                    abstained = ''

                yes = yes.replace('\n', '').split(',')
                no = no.replace('\n', '').split(',')
                abstained = abstained.replace('\n', '').split(',')

                def process_vote_list(res):
                    new_res = []
                    for y in res:
                        found = False
                        for p in unique_names:
                            if format_name(p) in format_name(y):
                                new_res.append({'name': format_name(y), 'id': format_name(p)})
                                found = True
                        if not found and y.strip():
                            new_res.append({'name': format_name(y), 'id': None})
                    return new_res

                votes_res['votes'].append({
                    "id": str(uuid.uuid4()),
                    'no': process_vote_list(no),
                    'abstained': process_vote_list(abstained),
                    'yes': process_vote_list(yes)
                })

        return votes_res

    def extract_attendance(data):
        data = data.strip().replace("\n", "").replace('Aanwezig', '')
        if 'Afwezig met kennisgeving' in data:
            present, rest = data.split('Afwezig met kennisgeving')
        else:
            present, rest = data, ""
        if 'Afwezig zonder kennisgeving' in rest:
            absent_with_notice, absent_without_notice = rest.split('Afwezig zonder kennisgeving')
        else:
            absent_with_notice, absent_without_notice = '', ''

        attendance = {
            "present": [format_name(s) for s in present.split(',') if s.strip()],
            "absent_with_notice": [
                format_name(s) for s in re.split(r'[;,|]', absent_with_notice) if s.strip()
            ],
            "absent_without_notice": [format_name(s) for s in absent_without_notice.split(',') if s.strip()]
        }
        return attendance

    def process_markdown(md_path, processed_path):
        with open(md_path, 'r', encoding='utf-8') as md_file:
            content = md_file.read()

        sections = content.split('-----')
        text = "".join(sections[1:])

        # Attempt to parse out table_of_content, debate, rest
        try:
            table_of_content = text.split('**OPENING VAN DE VERGADERING**')[0]
            debate = text.split('**OPENING VAN DE VERGADERING**')[1]
            debate, rest = debate.split('De vergadering is gesloten')
            people = rest.split('Stemming nr.')[0]
        except Exception:
            table_of_content = text.split('**')[0]
            debate = '**'.join(text.split('**')[1:])
            rest = ''
            people = ''

        # Attempt to extract attendance
        if '**Aanwezigheden**' in people:
            people = people.split('**Aanwezigheden**')[1].replace(
                '**Individuele stemmingen Vlaamse Volksvertegenwoordigers**', ''
            )
            people = extract_attendance(people)
        else:
            people = {
                "present": [],
                "absent_with_notice": [],
                "absent_without_notice": []
            }

        votes = rest.split('Stemming nr.')[1:]
        # Build JSON output
        json_output = process_second_section(
            table_of_content, people, md_path.split('/')[-1].split('\\')[-1]
        )
        json_output['people_list'] = people
        json_output['statements'] = process_debate(
            debate,
            people,
            [[s['id'], s['type'] + s['topic']] for s in json_output['sections']]
        )
        json_output['votes'] = process_votes(
            votes,
            people,
            [s['id'] for s in json_output['sections'] if s['voted']]
        )
        return json_output

    processed_files = []

    for md_path, pdf_link, yt_link in zip(markdown_files, pdf_links, youtubes):
        if md_path.endswith('.md'):
            filename = os.path.basename(md_path)
            json_filename = filename.replace('.md', '.json')
            processed_json_path = os.path.join(output_dir, json_filename)

            print(f"Processing {filename}...")
            try:
                json_output = process_markdown(md_path, processed_json_path)
                json_output['youtube_link'] = yt_link
                json_output['pdf_link'] = pdf_link

                with open(processed_json_path, 'w', encoding='utf-8') as json_file:
                    json.dump(json_output, json_file, indent=4)

                processed_files.append(processed_json_path)
                print(f"Saved processed JSON for {filename}")
            except Exception as e:
                print(e)
                traceback.print_exc()
                print(f"failed on {filename}")

    print("All markdown files have been processed.")
    return processed_files


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
        # Airflow 2.2+ supports params. Users can provide or override these
        # in the UI via the "Trigger DAG" form.
        params={
            "start_date_param": "01092024",  # default
            "end_date_param": "21102024"  # default
        }
) as dag:
    def step1_download(**context):
        """
        Calls download_pdf_pipeline_step for both 'plen' and 'comm'
        and returns combined results (files, links, youtubes).

        Now reads start/end dates from DAG params.
        """
        # Pull from context['params'] so user can override in Airflow UI
        start_date = context['params'].get('start_date_param', '01092024')
        end_date = context['params'].get('end_date_param', '21102024')

        # Download 'plen'
        files, links, youtubes = download_pdf_pipeline_step(
            type='plen',
            output_dir='/usr/local/airflow/data',
            datumvan=start_date,
            datumtot=end_date
        )
        # Optionally run 'comm'
        files2, links2, youtubes2 = download_pdf_pipeline_step(
            type='comm',
            output_dir='/usr/local/airflow/data',
            datumvan=start_date,
            datumtot=end_date
        )
        # Combine the results
        files.extend(files2)
        links.extend(links2)
        youtubes.extend(youtubes2)

        return {
            'pdf_files': files,
            'pdf_links': links,
            'youtubes': youtubes
        }


    def step2_pdf_to_md(**context):
        """
        Pulls the PDF list from XCom, converts each to Markdown,
        returns the list of Markdown paths.
        """
        ti = context['ti']
        data = ti.xcom_pull(task_ids='download_pdfs')
        pdf_files = data.get('pdf_files', [])
        markdowns = pdf_to_markdown_pipeline_step(
            pdf_files=pdf_files,
            output_dir='/usr/local/airflow/data/markdown'
        )
        return {
            'markdown_files': markdowns,
            'pdf_links': data.get('pdf_links', []),
            'youtubes': data.get('youtubes', [])
        }


    def step3_md_to_json(**context):
        """
        Pulls the Markdown list, PDF links, and YouTube links from XCom,
        then processes them into JSON using the OpenAI API (embedding).
        """
        ti = context['ti']
        data = ti.xcom_pull(task_ids='convert_to_markdown')
        markdown_files = data.get('markdown_files', [])
        pdf_links = data.get('pdf_links', [])
        youtubes = data.get('youtubes', [])

        # Retrieve your OpenAI API key from environment variables
        api_key = os.getenv('OPENAI_KEY')
        if not api_key:
            raise ValueError("OPENAI_KEY not found in environment variables.")

        json_files = markdown_to_json_pipeline_step(
            markdown_files=markdown_files,
            pdf_links=pdf_links,
            youtubes=youtubes,
            api_key=api_key,
            output_dir='/usr/local/airflow/data/json'
        )
        return {'json_files': json_files}


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

    download_pdfs_task >> convert_to_markdown_task >> convert_to_json_task