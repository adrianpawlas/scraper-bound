import time
from datetime import datetime, timezone
from typing import Dict, Any
from category_scraper import CategoryScraper
from product_scraper import ProductScraper
from embedding_generator import EmbeddingGenerator
from supabase_uploader import SupabaseUploader


class BoundScraper:
    def __init__(self):
        print("=" * 60)
        print("Weare Bound Scraper - Starting")
        print("=" * 60)
        
        self.category_scraper = CategoryScraper()
        self.product_scraper = ProductScraper()
        self.embedding_generator = EmbeddingGenerator()
        self.supabase_uploader = SupabaseUploader()
        
        self.stats = {
            "total_products": 0,
            "scraped": 0,
            "embedded": 0,
            "uploaded": 0,
            "failed": 0,
        }
    
    def run(self):
        try:
            if not self.supabase_uploader.check_connection():
                print("Failed to connect to Supabase. Exiting.")
                return
            
            print("\n[1/4] Fetching product URLs via Shopify API...")
            product_urls = self.category_scraper.scrape_all_categories()
            self.stats["total_products"] = len(product_urls)
            print(f"Found {self.stats['total_products']} unique products")
            
            print("\n[2/4] Scraping individual products...")
            products = self._scrape_all_products(product_urls)
            
            print("\n[3/4] Generating embeddings...")
            products = self._generate_all_embeddings(products)
            
            print("\n[4/4] Uploading to Supabase...")
            self._upload_all_products(products)
            
            self._print_stats()
            
        except KeyboardInterrupt:
            print("\nScraping interrupted by user")
            self._print_stats()
        except Exception as e:
            print(f"\nError during scraping: {e}")
            raise
        finally:
            self.cleanup()
    
    def _scrape_all_products(self, product_urls: set) -> list[Dict[str, Any]]:
        products = []
        total = len(product_urls)
        
        for i, url in enumerate(product_urls, 1):
            print(f"  [{i}/{total}] {url}")
            
            product_data = self.product_scraper.scrape_product(url)
            
            if product_data:
                product_data["created_at"] = datetime.now(timezone.utc).isoformat()
                products.append(product_data)
                self.stats["scraped"] += 1
            else:
                self.stats["failed"] += 1
            
            time.sleep(0.3)
        
        return products
    
    def _generate_all_embeddings(self, products: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        total = len(products)
        
        for i, product in enumerate(products, 1):
            print(f"  [{i}/{total}] Generating embeddings for: {product.get('title', 'unknown')[:50]}")
            
            image_url = product.get("image_url")
            if image_url:
                image_embedding = self.embedding_generator.generate_image_embedding(image_url)
                if image_embedding:
                    product["image_embedding"] = image_embedding
                    self.stats["embedded"] += 1
                else:
                    print(f"    Failed to generate image embedding")
            else:
                print(f"    No image URL available")
            
            info_text = self.embedding_generator.generate_info_text(product)
            if info_text:
                info_embedding = self.embedding_generator.generate_text_embedding(info_text)
                if info_embedding:
                    product["info_embedding"] = info_embedding
                else:
                    print(f"    Failed to generate info embedding")
            
            time.sleep(0.2)
        
        return products
    
    def _upload_all_products(self, products: list[Dict[str, Any]]):
        total = len(products)
        
        for i, product in enumerate(products, 1):
            print(f"  [{i}/{total}] Uploading: {product.get('title', 'unknown')[:50]}")
            
            success = self.supabase_uploader.insert_product(product)
            
            if success:
                self.stats["uploaded"] += 1
            else:
                self.stats["failed"] += 1
            
            time.sleep(0.2)
    
    def _print_stats(self):
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETE - STATISTICS")
        print("=" * 60)
        print(f"Total products found:    {self.stats['total_products']}")
        print(f"Products scraped:        {self.stats['scraped']}")
        print(f"Embeddings generated:    {self.stats['embedded']}")
        print(f"Products uploaded:       {self.stats['uploaded']}")
        print(f"Failed:                  {self.stats['failed']}")
        print("=" * 60)
    
    def cleanup(self):
        print("\nCleaning up resources...")
        self.category_scraper.close()
        self.product_scraper.close()
        self.embedding_generator.cleanup()
        print("Done")
