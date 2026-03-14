import re
import requests
import json
from typing import Optional, Dict, Any, List
from bs4 import BeautifulSoup
from config import SOURCE, BRAND, SECOND_HAND, COUNTRY


class ProductScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
    
    def scrape_product(self, product_url: str) -> Optional[Dict[str, Any]]:
        try:
            handle = self._extract_handle(product_url)
            if not handle:
                return None
            
            url = f"https://wearebound.co.uk/products/{handle}.json"
            response = self.session.get(url, timeout=30)
            
            if response.status_code != 200:
                print(f"    Failed to fetch product: {product_url}")
                return None
            
            data = response.json()
            product = data.get('product', {})
            
            if not product:
                return None
            
            return self._parse_product(product, product_url)
            
        except Exception as e:
            print(f"    Error scraping {product_url}: {e}")
            return None
    
    def _extract_handle(self, url: str) -> Optional[str]:
        match = re.search(r'/products/([^/?]+)', url)
        if match:
            return match.group(1)
        return None
    
    def _parse_product(self, product: Dict[str, Any], product_url: str) -> Dict[str, Any]:
        product_id = product.get('handle')
        title = product.get('title', 'Unknown')
        
        description_html = product.get('body_html', '')
        description = self._clean_html(description_html)
        
        price, sale_price = self._extract_prices(product)
        images = self._extract_images(product)
        sizes = self._extract_sizes(product)
        category = product.get('product_type', '')
        gender = self._extract_gender(title, category)
        colors = self._extract_colors(product)
        metadata = self._build_metadata(product, title, description, price, sale_price, sizes, colors)
        
        return {
            "id": product_id,
            "source": SOURCE,
            "product_url": product_url,
            "brand": BRAND,
            "title": title,
            "description": description,
            "category": category,
            "gender": gender,
            "price": price,
            "sale": sale_price if sale_price else price,
            "second_hand": SECOND_HAND,
            "country": COUNTRY,
            "image_url": images[0] if images else None,
            "additional_images": ", ".join(images[1:]) if len(images) > 1 else None,
            "metadata": metadata,
            "size": ", ".join(sizes) if sizes else None,
        }
    
    def _clean_html(self, html: str) -> str:
        if not html:
            return ""
        soup = BeautifulSoup(html, 'html.parser')
        return soup.get_text(strip=True)
    
    def _extract_prices(self, product: Dict[str, Any]) -> tuple[str, Optional[str]]:
        variants = product.get('variants', [])
        
        if not variants:
            return "0", None
        
        prices = set()
        compare_prices = set()
        
        for variant in variants:
            price = variant.get('price')
            if price:
                prices.add(price)
            
            compare_at = variant.get('compare_at_price')
            if compare_at:
                compare_prices.add(compare_at)
        
        if prices:
            price = min(prices)
        else:
            price = "0"
        
        sale_price = None
        if compare_prices and min(compare_prices) > float(price):
            sale_price = price
            price = min(compare_prices)
        
        return f"{price}GBP", sale_price
    
    def _extract_images(self, product: Dict[str, Any]) -> List[str]:
        images = product.get('images', [])
        return [img.get('src', '') for img in images if img.get('src')]
    
    def _extract_sizes(self, product: Dict[str, Any]) -> List[str]:
        variants = product.get('variants', [])
        sizes = set()
        
        for variant in variants:
            title = variant.get('title', '')
            if title and title != 'Default Title':
                sizes.add(title)
        
        return list(sizes)
    
    def _extract_colors(self, product: Dict[str, Any]) -> List[str]:
        options = product.get('options', [])
        colors = []
        
        for option in options:
            if option.get('name', '').lower() == 'color':
                values = option.get('values', [])
                colors.extend(values)
        
        return colors
    
    def _extract_gender(self, title: str, category: str) -> str:
        combined = f"{title} {category}".lower()
        
        if any(word in combined for word in ['men', 'man', 'male', 'mens', 'boy']):
            return "man"
        elif any(word in combined for word in ['women', 'woman', 'female', 'womens', 'ladies', 'girl']):
            return "woman"
        
        return "woman"
    
    def _build_metadata(self, product: Dict[str, Any], title: str, description: str, 
                        price: str, sale_price: Optional[str], sizes: List[str], 
                        colors: List[str]) -> str:
        parts = [f"Title: {title}"]
        
        if description:
            parts.append(f"Description: {description[:500]}")
        
        if price:
            parts.append(f"Price: {price}")
        
        if sale_price:
            parts.append(f"Sale Price: {sale_price}")
        
        if sizes:
            parts.append(f"Sizes: {', '.join(sizes)}")
        
        if colors:
            parts.append(f"Colors: {', '.join(colors)}")
        
        tags = product.get('tags', [])
        if tags:
            parts.append(f"Tags: {', '.join(tags)}")
        
        return " | ".join(parts)
    
    def close(self):
        self.session.close()
