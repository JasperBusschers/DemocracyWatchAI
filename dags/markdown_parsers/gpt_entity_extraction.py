import openai
import json
import os
from openai import OpenAI

client = OpenAI(
    api_key=os.getenv('OPENAI_KEY'),  # This is the default and can be omitted
)


def markdown_to_json(markdown_text: str) -> dict:
    prompt = (
        "You are an assistant that can only reply in json valid syntax. "
        "Given this text, make the following json structure by extracting the entities\n\n"
        "{ "
        "\"title\" : title for document,\n"
        "\"summary\" : summarized in 400 lines the topic,\n"
        "  \"articles\" : [ {\"number\" : , \"text\", \"4sentence_description\"} ]}\n\n"
        "Make sure it is complete."
        "Give only json.\n\n"
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": markdown_text},
        ]
    )

    json_output = response.choices[0].message.content.strip('```json').strip('```')
    return json.loads(json_output)
