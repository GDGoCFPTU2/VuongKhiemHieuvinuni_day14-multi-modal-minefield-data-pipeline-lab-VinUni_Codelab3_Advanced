# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

TOXIC_STRINGS = [
    'Null pointer exception',
    'null pointer',
    'NullPointerException',
    'ERROR:',
    'Exception:',
    'Traceback (most recent call last):',
    'FATAL:',
]

def run_quality_gate(document_dict):
    # Gate 1: Reject documents with 'content' length < 20 characters
    content = document_dict.get('content', '')
    if len(content) < 20:
        print(f"[QUALITY GATE FAIL] Content too short: {document_dict.get('document_id')}")
        return False

    # Gate 2: Reject documents containing toxic/error strings
    content_lower = content.lower()
    for toxic in TOXIC_STRINGS:
        if toxic.lower() in content_lower:
            print(f"[QUALITY GATE FAIL] Toxic string detected in: {document_dict.get('document_id')}")
            return False

    # Gate 3: Flag discrepancies (tax calculation comment vs code)
    metadata = document_dict.get('source_metadata', {})
    if metadata.get('tax_discrepancy_detected'):
        print(f"[QUALITY GATE WARNING] Tax discrepancy detected in: {document_dict.get('document_id')}")

    return True
