import os
import pymupdf4llm
import pathlib

def pdf_to_markdown_pipeline_step(pdf_files, output_dir='data/Belgium-Flanders/markdown'):
    # Create the markdown directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Function to convert PDF to markdown
    def pdf_to_markdown(pdf_path, markdown_path):
        # Use pymupdf4llm to convert PDF to markdown
        md_text = pymupdf4llm.to_markdown(pdf_path)

        # Save the markdown as a UTF-8 encoded file
        pathlib.Path(markdown_path).write_bytes(md_text.encode())

    processed_files = []

    # Process all PDFs in the input list
    for pdf_path in pdf_files:
        if pdf_path.endswith('.pdf'):
            filename = os.path.basename(pdf_path)
            markdown_filename = filename.replace('.pdf', '.md')
            markdown_path = os.path.join(output_dir, markdown_filename)

            # Convert and save
            print(f"Processing {filename}...")
            pdf_to_markdown(pdf_path, markdown_path)
            print(f"Saved markdown as {markdown_filename}")
            processed_files.append(markdown_path)
        else:
            print(f"Skipping {pdf_path} as it's not a PDF file.")

    print(f"All PDFs have been processed. Total files converted: {len(processed_files)}")
    return processed_files