import pandas as pd
import re
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

def parse_price(price_str):
    """Convert price string to float. Handle '$1200', '250000', 'five dollars', 'N/A', 'NULL', etc."""
    if pd.isna(price_str) or price_str in ['N/A', 'NULL', 'Liên hệ', '']:
        return None
    price_str = str(price_str).strip()
    # Remove currency symbols and commas
    price_str = re.sub(r'[$€£¥,]', '', price_str)
    price_str = price_str.replace('VND', '').replace('USD', '').strip()
    # Handle word numbers
    word_to_num = {
        'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
        'eleven': 11, 'twelve': 12, 'thirteen': 13, 'fourteen': 14, 'fifteen': 15,
        'a': 1
    }
    words = price_str.lower().split()
    if all(w in word_to_num or w.isdigit() for w in words):
        try:
            return float(price_str)
        except ValueError:
            pass
    # Handle "five dollars" pattern
    match = re.search(r'(\w+)\s*dollars?', price_str.lower())
    if match:
        word = match.group(1)
        if word in word_to_num:
            return float(word_to_num[word])
    try:
        return float(price_str)
    except ValueError:
        return None

def parse_date(date_str):
    """Normalize date to YYYY-MM-DD format."""
    if pd.isna(date_str) or date_str == '':
        return None
    date_str = str(date_str).strip()
    formats = [
        '%Y-%m-%d',      # 2026-01-15
        '%d/%m/%Y',      # 15/01/2026
        '%d-%m-%Y',      # 17-01-2026
        '%Y/%m/%d',      # 2026/01/19
        '%d %B %Y',      # 19 Jan 2026
        '%B %dth %Y',    # January 16th 2026
        '%B %d, %Y',     # January 22nd 2026
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None

def process_sales_csv(file_path):
    df = pd.read_csv(file_path)

    # Remove duplicate rows based on 'id', keep first
    df = df.drop_duplicates(subset=['id'], keep='first')

    # Clean price column
    df['price_cleaned'] = df['price'].apply(parse_price)

    # Normalize date
    df['date_normalized'] = df['date_of_sale'].apply(parse_date)

    # Build result list for UnifiedDocument schema
    results = []
    for _, row in df.iterrows():
        doc = {
            'document_id': f"csv-{row['id']}",
            'content': f"Product: {row['product_name']}, Category: {row['category']}, Price: {row['price_cleaned']}, Date: {row['date_normalized']}",
            'source_type': 'CSV',
            'author': 'Unknown',
            'timestamp': row['date_normalized'],
            'source_metadata': {
                'product_name': row['product_name'],
                'category': row['category'],
                'price_raw': row['price'],
                'price_cleaned': row['price_cleaned'],
                'currency': row.get('currency', 'VND'),
                'seller_id': row.get('seller_id'),
                'stock_quantity': row.get('stock_quantity'),
                'original_id': row['id']
            }
        }
        results.append(doc)

    return results

