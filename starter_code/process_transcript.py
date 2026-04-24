import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

def clean_transcript(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()

    # Remove noise tokens like [Music], [inaudible], [Laughter], [Music starts], etc.
    text = re.sub(r'\[(?:Music|Music starts|Music ends|inaudible|Laughter)\]', '', text, flags=re.IGNORECASE)

    # Strip timestamps [00:00:00]
    text = re.sub(r'\[\d{2}:\d{2}:\d{2}\]', '', text)

    # Extract price from Vietnamese words "năm trăm nghìn" = 500000 VND
    detected_price = None
    if 'năm trăm nghìn' in text.lower() or '500,000' in text:
        detected_price = 500000

    # Clean up extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    doc = {
        'document_id': 'video-transcript-001',
        'content': text,
        'source_type': 'Video',
        'author': 'Unknown',
        'timestamp': None,
        'source_metadata': {
            'detected_price_vnd': detected_price,
            'original_file': 'demo_transcript.txt'
        }
    }

    return doc

