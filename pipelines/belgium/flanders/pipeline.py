import argparse
import os
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm
from step1_get_raw_data import download_pdf_pipeline_step
from step2_process_raw_data import pdf_to_markdown_pipeline_step
from step3_process_to_json import markdown_to_json_pipeline_step

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))),
                        'env', '.env')
load_dotenv(env_path)


def run_pipeline(output_dir, start_date, end_date):
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Get API key from environment variable
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment variables")

    # Step 1: Download PDFs
    print("Step 1: Downloading PDFs")
    files, paths, youtubes = download_pdf_pipeline_step(
        output_dir=os.path.join(output_dir, 'raw'),
        datumvan=start_date,
        datumtot=end_date
    )

    # Step 2: Convert PDFs to Markdown
    print("Step 2: Converting PDFs to Markdown")
    markdowns = []
    for pdf_file in tqdm(files, desc="Converting to Markdown"):
        markdown = pdf_to_markdown_pipeline_step([pdf_file], output_dir=os.path.join(output_dir, 'markdown'))
        markdowns.extend(markdown)

    # Step 3: Process Markdown to JSON
    print("Step 3: Processing Markdown to JSON")
    processed_jsons = markdown_to_json_pipeline_step(
        markdown_files=markdowns,
        pdf_links=paths,
        youtubes=youtubes,
        api_key=api_key,
        output_dir=os.path.join(output_dir, 'json')
    )

    print(f"Pipeline completed. Processed {len(processed_jsons)} files.")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Belgium-Flanders Data Processing Pipeline")
    parser.add_argument("--output_dir", type=str, default="data/Belgium-Flanders",
                        help="Output directory for processed files")
    parser.add_argument("--start_date", type=str,default='01092024',
                        help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", type=str,default='21102024',
                        help="End date in YYYY-MM-DD format")
    args = parser.parse_args()

    run_pipeline(args.output_dir, args.start_date, args.end_date)