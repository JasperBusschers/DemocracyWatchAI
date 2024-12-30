import os
import re
import traceback
import uuid
import json
import requests


def markdown_to_json_pipeline_step(markdown_files, pdf_links,youtubes, api_key, output_dir='json'):
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
        # Updated regex to match at least three capitalized words
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

                # Match to extract people names from the topic part
                people_match = re.search(r'van (.*?)over', topic_part)
                people = people_match.group(1).strip() if people_match else None
                people = people.split(', ') if people is not None else []
                people_final = []
                for name in unique_names:
                    if name in topic_part:
                        people_final.append(format_name(name))
                # Append the result with type, topic, text, and people
                result.append({
                    'id': str(uuid.uuid4()),
                    'type': type_part,
                    'topic': topic_part,
                    'vector': get_embedding(topic_part),
                    'people': people_final,
                    'voted': 'Hoofdelijke stemming' in line
                })

        return result

    # Function to process the second section based on the provided code
    def process_second_section(text, people, link):
        # Regular expressions to capture topics and people
        lines = text.split('\n\n')
        name, date = lines[1].split(' – ')

        # Extract meeting information
        meeting = {
            "name": name,
            "date": date,
            "sections": [], "link": link, 'country': 'Belgium', 'region': 'Flanders'
        }
        if 'INHOUD' in lines[2]:
            meeting['sections'] = (extract_types_and_topics(lines[2:-1], people))
        # Find all matches in the text

        # Convert the meeting dictionary to JSON

        # Print the JSON output
        return meeting

    def jaccard_similarity_words(str1, str2, threshold=0.4):
        # Split strings into words and convert to sets
        set1 = set(str1.split())
        set2 = set(str2.split())

        # Calculate intersection and union of the two sets
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))

        # Compute Jaccard similarity
        similarity = intersection / union if union != 0 else 0

        # Decide if they are the same based on the threshold
        if similarity >= threshold:
            return True
        else:
            return False

    def process_debate(debate, people, topics):
        all_names = people['present'] + people['absent_with_notice'] + people['absent_without_notice']

        # Remove leading/trailing spaces and deduplicate the names
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
                print(statement)
                for t in topics:
                    print("!!!!!!!!!!!!!!!",statement,t[1])
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
        id = str(uuid.uuid4())
        next_statement = str(uuid.uuid4())
        ouptut = []
        for s, topic in statements:
            person = s.split(':')[0]
            person = person.replace('Minister ', '')
            if not 'De voorzitter' in person:
                if '(' in person:
                    person, party = person.split('(')
                    party = party.replace(')', '')
                else:
                    party = None
                person_ref = None
                for n in unique_names:
                    if format_name(n) in format_name(person):
                        person_ref = format_name(n)
                json_output = {'id': id, 'topic': topic, 'person_ref': person_ref, 'previous': previous_statement,
                               "next": next_statement, "Party": party, "person": person, "statement": s.split(':')[1],
                               'vector': get_embedding(s.split(':')[1])}
                ouptut.append(json_output)
                previous_statement = id
                id = next_statement
                next_statement = str(uuid.uuid4())
        return ouptut

    def format_name(name: str) -> str:
        # Strip leading/trailing spaces
        name = name.strip()

        # Add spaces between a lowercase letter followed by a capital letter (without space)
        name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name).replace('-', ' ').replace('.', '')

        return name

    def process_votes(votes, people, topics):
        all_names = people['present'] + people['absent_with_notice'] + people['absent_without_notice']

        # Remove leading/trailing spaces and deduplicate the names
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
                yes_res = []

                def process(res):
                    new_res = []
                    for y in res:
                        found = False
                        for p in unique_names:
                            if format_name(p) in format_name(y):
                                new_res.append({'name': format_name(y), 'id': format_name(p)})
                                found = True
                        if not found and y != '':
                            new_res.append({'name': format_name(y), 'id': None})
                    return new_res

                votes_res['votes'].append(
                    {"id": str(uuid.uuid4()), 'no': process(no), 'abstained': process(abstained), 'yes': process(yes)})

        return votes_res

    def extract_attendance(data):
        # Remove extra spaces and line breaks
        data = data.strip()

        # Split the data into three main parts by the section headers
        data = data.replace("\n", "").replace('Aanwezig', '')
        if 'Afwezig met kennisgeving' in data:
            present, rest = data.split('Afwezig met kennisgeving')
        else:
            present, rest = data, ""
        if 'Afwezig zonder kennisgeving' in rest:
            absent_with_notice, absent_without_notice = rest.split('Afwezig zonder kennisgeving')
        else:
            absent_with_notice, absent_without_notice = '', ''

        # Prepare the output dictionary
        attendance = {
            "present": [format_name(s) for s in present.split(',')],
            "absent_with_notice": [format_name(s) for s in
                                   [s.split(':')[0] for s in re.split(r'[;,|]', absent_with_notice)] if s != ''],
            "absent_without_notice": [format_name(s) for s in absent_without_notice.split(',') if s != '']
        }

        # Convert the dictionary to a JSON string
        return attendance
    def process_markdown(md_path, processed_path):
        with open(md_path, 'r', encoding='utf-8') as md_file:
            content = md_file.read()

        sections = content.split('-----')
        text = "".join(sections[1:])
        try:
            table_of_content = text.split('**OPENING VAN DE VERGADERING**')[0]
            debate = text.split('**OPENING VAN DE VERGADERING**')[1]
            debate, rest = debate.split('De vergadering is gesloten')
            people = rest.split('Stemming nr.')[0]
        except:
            table_of_content = text.split('**')[0]
            debate = '**'.join(text.split('**')[1:])
            rest = ''
            people=''
        if '**Aanwezigheden**' in people:
            people = people.split('**Aanwezigheden**')[1].replace(
                '**Individuele stemmingen Vlaamse Volksvertegenwoordigers**', '')
            people = extract_attendance(people)
        else:
            people = {
                "present": [],
                "absent_with_notice": [],
                "absent_without_notice": []
            }
        votes = rest.split('Stemming nr.')[1:]
        json_output = process_second_section(table_of_content, people, md_path.split('/')[-1].split('\\')[1])
        json_output['people_list'] = people
        json_output['statements'] = process_debate(debate, people,
                                                   [[s['id'], s['type'] + s['topic']] for s in json_output['sections']])
        json_output['votes'] = process_votes(votes, people, [s['id'] for s in json_output['sections'] if s['voted']])

        return json_output

    processed_files = []

    for md_path, pdf_link ,yt in zip(markdown_files, pdf_links, youtubes):
        if md_path.endswith('.md'):
            filename = os.path.basename(md_path)
            json_filename = filename.replace('.md', '.json')
            processed_json_path = os.path.join(output_dir, json_filename)

            print(f"Processing {filename}...")
            try:
                json_output = process_markdown(md_path, processed_json_path)
                json_output['youtube_link']=yt
                json_output['pdf_link']=pdf_link
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

