import os
import re
import json
import uuid
import requests
import traceback

class MarkdownToJsonPipelineStep:
    def __init__(
        self,
        markdown_files,
        pdf_links,
        youtubes,
        topics_info,
        api_key,
        output_dir='json'
    ):
        self.markdown_files = markdown_files
        self.pdf_links = pdf_links
        self.youtubes = youtubes
        self.topics_info = topics_info
        self.api_key = api_key
        self.output_dir = output_dir

    def run(self):
        """
        Main entry point to execute the Markdown -> JSON processing step.
        Iterates over all markdown files, processes them, and writes out JSON files.
        """
        os.makedirs(self.output_dir, exist_ok=True)
        processed_files = []
        for md_path, pdf_link, yt_link, topic in zip(
            self.markdown_files, self.pdf_links, self.youtubes,self.topics_info
        ):
            if md_path.endswith('.md'):
                filename = os.path.basename(md_path)
                json_filename = filename.replace('.md', '.json')
                processed_json_path = os.path.join(self.output_dir, json_filename)

                print(f"Processing {filename}...")
                try:
                    json_output = self.process_markdown(md_path)
                    json_output['youtube_link'] = yt_link
                    json_output['pdf_link'] = pdf_link
                    json_output['topic_texts'] = topic
                    with open(processed_json_path, 'w', encoding='utf-8') as json_file:
                        json.dump(json_output, json_file, indent=4)

                    processed_files.append(processed_json_path)
                    print(f"Saved processed JSON for {filename}")
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    print(f"Failed on {filename}")

        print("All markdown files have been processed.")
        return processed_files

    def get_embedding(self, text):
        """
        Calls the OpenAI Embeddings API to get an embedding for the given text.
        """
        url = "https://api.openai.com/v1/embeddings"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
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

    def parse_section(self, section_text):
        """
        Parses a single section text from the table of contents to extract:
          - "type": the leading uppercase words (or first word).
          - "topic": the full section text, prefixed with ": ".
          - "voted": a Boolean that is True if vote information is detected.
          - "vote_text": (if applicable) the extracted vote details.
        """
        import re
        vote_pattern = r'–\s*([\d\(\)\s\-]+)\s*–'
        vote_match = re.search(vote_pattern, section_text)
        vote_text = None
        voted = False

        if vote_match:
            vote_text = vote_match.group(1).strip()
            voted = True
            # Remove the vote info from section_text for a cleaner header/topic.
            section_text = re.sub(vote_pattern, ' ', section_text)

        # Extract the leading uppercase words as the section "type"
        words = section_text.split()
        type_words = []
        for w in words:
            # Check if the word is fully uppercase. Note that numbers/punctuation are ignored.
            if w == w.upper():
                type_words.append(w)
            else:
                break
        type_str = " ".join(type_words) if type_words else (words[0] if words else "")

        return {
            "id": str(uuid.uuid4()),  # could add an ID for the section
            "type": type_str,
            "topic": ": " + section_text,
            "voted": voted,
            **({"vote_text": vote_text} if voted else {})
        }

    def extract_types_and_topics(self, text_lines, people):
        """
        Revised extraction of sections from the table of contents. It first joins
        the provided text lines, splits them back into individual lines, and removes
        any "INHOUD" line. Then it attempts to detect section breaks by looking for
        lines that are entirely uppercase.

        If no sections are detected, a fallback is used where each nonempty line is
        parsed individually.
        """
        lines = [line.strip() for line in "\n".join(text_lines).split("\n") if line.strip()]
        # Remove "INHOUD" lines
        lines = [line for line in lines if line.upper() != "INHOUD"]

        sections = []
        current_section = []

        for line in lines:
            # If this line is fully uppercase and we already have something in current_section,
            # we assume a new section starts.
            if line == line.upper() and current_section:
                section_text = " ".join(current_section)
                section_data = self.parse_section(section_text)
                if section_data and section_data.get("type"):
                    sections.append(section_data)
                current_section = [line]
            else:
                current_section.append(line)

        if current_section:
            section_text = " ".join(current_section)
            section_data = self.parse_section(section_text)
            if section_data and section_data.get("type"):
                sections.append(section_data)

        # Fallback if no sections found
        if not sections:
            for line in lines:
                section_data = self.parse_section(line)
                if section_data and section_data.get("type"):
                    sections.append(section_data)

        return sections

    def process_second_section(self, text, people, link):
        """
        Processes the first section of the markdown text:
        - Attempts to extract meeting name/date from the second block.
        - Then uses extract_types_and_topics to parse the table of contents (if found).
        """
        lines = text.split('\n\n')
        try:
            # Try to parse the meeting name and date from the second block.
            name, date = lines[1].split(' – ')
        except Exception:
            name, date = "Unknown", "Unknown"

        meeting = {
            "name": name,
            "date": date,
            "sections": [],
            "link": link,
            "country": 'Belgium',
            "region": 'Flanders'
        }

        # If the first block contains "INHOUD", process subsequent blocks.
        if lines and lines[0].upper().find("INHOUD") != -1:
            meeting['sections'] = self.extract_types_and_topics(lines[1:], people)
        else:
            # Fallback: process all blocks if "INHOUD" not found.
            meeting['sections'] = self.extract_types_and_topics(lines, people)

        return meeting

    def jaccard_similarity_words(self, str1, str2, threshold=0.4):
        set1 = set(str1.split())
        set2 = set(str2.split())
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        similarity = intersection / union if union != 0 else 0
        return similarity >= threshold

    def format_name(self, name: str) -> str:
        """
        Utility for normalizing names:
        inserts a space between a lowercase letter followed by uppercase,
        and strips out hyphens, periods, etc.
        """
        name = name.strip()
        name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name).replace('-', ' ').replace('.', '')
        return name

    def process_debate(self, debate, people, topics):
        """
        Splits the debate text into statements. A statement is assumed
        whenever we see something of the form "Person: some text".
        Then we build a JSON output for each statement.
        """
        all_names = (
            people['present']
            + people['absent_with_notice']
            + people['absent_without_notice']
        )
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
                # Attempt to match the statement to a known topic
                for t in topics:
                    if self.jaccard_similarity_words(t[1], statement) and last_was_topic:
                        topic.append(t[0])
                        last_was_topic = True
                        is_topic = True
                    elif self.jaccard_similarity_words(t[1], statement):
                        topic = [t[0]]
                        last_was_topic = True
                        is_topic = True

                if not is_topic:
                    last_was_topic = False
                    statements.append([statement, topic])
                text = l
            else:
                text += l

        # Build statement structures
        previous_statement = ''
        id_ = str(uuid.uuid4())
        next_statement = str(uuid.uuid4())
        output = []

        for s, tpcs in statements:
            person = s.split(':')[0]
            person = person.replace('Minister ', '')
            if 'De voorzitter' not in person:
                if '(' in person:
                    # e.g., "John Doe (Party)"
                    person, party = person.split('(')
                    party = party.replace(')', '')
                else:
                    party = None

                person_ref = None
                for n in unique_names:
                    if self.format_name(n) in self.format_name(person):
                        person_ref = self.format_name(n)

                # Everything after the first colon is the statement text
                parts = s.split(':', 1)
                statement_text = parts[1] if len(parts) > 1 else ""

                json_output = {
                    'id': id_,
                    'topic': tpcs,
                    'person_ref': person_ref,
                    'previous': previous_statement,
                    'next': next_statement,
                    'Party': party,
                    'person': person.strip(),
                    'statement': statement_text,
                    'vector': self.get_embedding(statement_text)
                }
                output.append(json_output)

                previous_statement = id_
                id_ = next_statement
                next_statement = str(uuid.uuid4())

        return output

    def process_votes(self, votes, people, topics):
        """
        Extract yes/no/abstentions from the lines that contain voting info.
        """
        all_names = (
            people['present']
            + people['absent_with_notice']
            + people['absent_without_notice']
        )
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
                            if self.format_name(p) in self.format_name(y):
                                new_res.append({'name': self.format_name(y), 'id': self.format_name(p)})
                                found = True
                                break
                        if not found and y.strip():
                            new_res.append({'name': self.format_name(y), 'id': None})
                    return new_res

                votes_res['votes'].append({
                    "id": str(uuid.uuid4()),
                    'no': process_vote_list(no),
                    'abstained': process_vote_list(abstained),
                    'yes': process_vote_list(yes)
                })

        return votes_res

    def extract_attendance(self, data):
        """
        Extracts attendance information from text that includes present/absent with/without notice.
        """
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
            "present": [self.format_name(s) for s in present.split(',') if s.strip()],
            "absent_with_notice": [
                self.format_name(s) for s in re.split(r'[;,|]', absent_with_notice) if s.strip()
            ],
            "absent_without_notice": [
                self.format_name(s) for s in absent_without_notice.split(',') if s.strip()
            ]
        }
        return attendance

    def process_markdown(self, md_path):
        """
        Orchestrates reading a single .md file, extracting table-of-contents,
        debate text, attendance info, votes, and building a JSON output.
        """
        with open(md_path, 'r', encoding='utf-8') as md_file:
            content = md_file.read()

        sections = content.split('-----')
        text = "".join(sections[1:])

        try:
            # Attempt to parse segments
            table_of_content = text.split('**OPENING VAN DE VERGADERING**')[0]
            debate = text.split('**OPENING VAN DE VERGADERING**')[1]
            debate, rest = debate.split('De vergadering is gesloten')
            people = rest.split('Stemming nr.')[0]
        except Exception:
            # Fallback if the above pattern isn't found
            table_of_content = text.split('**')[0]
            debate = '**'.join(text.split('**')[1:])
            rest = ''
            people = ''

        # Extract attendance
        if '**Aanwezigheden**' in people:
            people = people.split('**Aanwezigheden**')[1].replace(
                '**Individuele stemmingen Vlaamse Volksvertegenwoordigers**', ''
            )
            people = self.extract_attendance(people)
        else:
            people = {
                "present": [],
                "absent_with_notice": [],
                "absent_without_notice": []
            }

        # Extract votes
        votes = rest.split('Stemming nr.')[1:] if 'Stemming nr.' in rest else []

        # Build output
        json_output = self.process_second_section(
            table_of_content, people, os.path.basename(md_path)
        )
        json_output['people_list'] = people
        json_output['statements'] = self.process_debate(
            debate,
            people,
            [[s['id'], s['type'] + s['topic']] for s in json_output['sections']]
        )
        json_output['votes'] = self.process_votes(
            votes,
            people,
            [s['id'] for s in json_output['sections'] if s['voted']]
        )
        return json_output
