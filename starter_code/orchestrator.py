import json
import time
import os

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")


# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.

def process_with_gate(doc_or_list, final_kb):
    """Process a document or list of documents through quality gate."""
    if doc_or_list is None:
        return
    if isinstance(doc_or_list, list):
        for doc in doc_or_list:
            if doc and run_quality_gate(doc):
                final_kb.append(doc)
            else:
                print(f"[SKIP] Document failed quality gate: {doc.get('document_id', 'unknown') if doc else 'None'}")
    else:
        if doc_or_list and run_quality_gate(doc_or_list):
            final_kb.append(doc_or_list)
        else:
            print(f"[SKIP] Document failed quality gate: {doc_or_list.get('document_id', 'unknown')}")

def main():
    start_time = time.time()
    final_kb = []

    # --- FILE PATH SETUP ---
    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")

    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")
    # ----------------------------------------------

    # Process PDF
    print("Processing PDF...")
    pdf_doc = extract_pdf_data(pdf_path)
    process_with_gate(pdf_doc, final_kb)

    # Process Transcript
    print("Processing Transcript...")
    trans_doc = clean_transcript(trans_path)
    process_with_gate(trans_doc, final_kb)

    # Process HTML
    print("Processing HTML...")
    html_docs = parse_html_catalog(html_path)
    process_with_gate(html_docs, final_kb)

    # Process CSV
    print("Processing CSV...")
    csv_docs = process_sales_csv(csv_path)
    process_with_gate(csv_docs, final_kb)

    # Process Legacy Code
    print("Processing Legacy Code...")
    code_doc = extract_logic_from_code(code_path)
    process_with_gate(code_doc, final_kb)

    # Save final knowledge base
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_kb, f, ensure_ascii=False, indent=2)

    end_time = time.time()
    print(f"Pipeline finished in {end_time - start_time:.2f} seconds.")
    print(f"Total valid documents stored: {len(final_kb)}")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    main()
