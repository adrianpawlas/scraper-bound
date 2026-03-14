import requests
from typing import Set, List, Dict, Any
from config import CATEGORIES


class CategoryScraper:
    def __init__(self):
        self.all_product_urls: Set[str] = set()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
    
    def scrape_all_categories(self) -> Set[str]:
        print("Starting product fetching via Shopify API...")
        
        page = 1
        while True:
            print(f"  Fetching page {page}...")
            
            url = f"https://wearebound.co.uk/products.json?page={page}&currency=EUR"
            response = self.session.get(url, timeout=30)
            
            if response.status_code != 200:
                print(f"    Failed to fetch page {page}")
                break
            
            data = response.json()
            products = data.get('products', [])
            
            if not products:
                print(f"    No more products on page {page}")
                break
            
            new_count = 0
            for product in products:
                product_url = f"https://wearebound.co.uk/products/{product.get('handle')}"
                if product_url not in self.all_product_urls:
                    self.all_product_urls.add(product_url)
                    new_count += 1
            
            print(f"    Found {len(products)} products ({new_count} new)")
            page += 1
        
        print(f"\nTotal unique products found: {len(self.all_product_urls)}")
        return self.all_product_urls
    
    def get_product_urls(self) -> Set[str]:
        return self.all_product_urls
    
    def close(self):
        self.session.close()
