from bs4 import BeautifulSoup
import re

def parse_price_html(price_str):
    """Convert price string to float. Handle '28,500,000 VND', 'N/A', 'Liên hệ'."""
    if not price_str or price_str in ['N/A', 'Liên hệ', '']:
        return None
    price_str = str(price_str).strip()
    price_str = re.sub(r'[^\d,]', '', price_str)
    price_str = price_str.replace(',', '')
    try:
        return float(price_str)
    except ValueError:
        return None

def parse_html_catalog(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    table = soup.find('table', id='main-catalog')
    if not table:
        return []

    results = []
    rows = table.find('tbody').find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) < 6:
            continue

        product_id = cells[0].get_text(strip=True)
        product_name = cells[1].get_text(strip=True)
        category = cells[2].get_text(strip=True)
        price_raw = cells[3].get_text(strip=True)
        stock = cells[4].get_text(strip=True)
        rating = cells[5].get_text(strip=True)

        price_cleaned = parse_price_html(price_raw)

        doc = {
            'document_id': f"html-{product_id}",
            'content': f"Product: {product_name}, Category: {category}, Price: {price_cleaned}, Stock: {stock}, Rating: {rating}",
            'source_type': 'HTML',
            'author': 'Unknown',
            'timestamp': None,
            'source_metadata': {
                'product_id': product_id,
                'product_name': product_name,
                'category': category,
                'price_raw': price_raw,
                'price_cleaned': price_cleaned,
                'stock_quantity': stock,
                'rating': rating
            }
        }
        results.append(doc)

    return results

