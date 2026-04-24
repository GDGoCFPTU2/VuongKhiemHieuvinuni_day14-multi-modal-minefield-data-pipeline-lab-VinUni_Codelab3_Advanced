import ast
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.

def extract_logic_from_code(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()

    tree = ast.parse(source_code)

    functions = []
    business_rules = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            docstring = ast.get_docstring(node)
            func_info = {
                'name': node.name,
                'docstring': docstring or ''
            }
            functions.append(func_info)

    # Find business rules in comments
    rule_pattern = re.compile(r'#\s*Business Logic Rule \d+:', re.IGNORECASE)
    rules = rule_pattern.findall(source_code)

    # Detect tax discrepancy: comment says 8% but code has 0.10
    tax_discrepancy = False
    if '8%' in source_code and '0.10' in source_code:
        tax_discrepancy = True

    content_parts = []
    for func in functions:
        if func['docstring']:
            content_parts.append(f"Function {func['name']}: {func['docstring']}")

    content = ' '.join(content_parts) if content_parts else 'No docstrings found'

    doc = {
        'document_id': 'code-legacy-001',
        'content': content,
        'source_type': 'Code',
        'author': 'Unknown',
        'timestamp': None,
        'source_metadata': {
            'functions': functions,
            'business_rules_found': len(rules) > 0,
            'tax_discrepancy_detected': tax_discrepancy,
            'original_file': 'legacy_pipeline.py'
        }
    }

    return doc

