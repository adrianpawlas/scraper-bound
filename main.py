import time
from datetime import datetime, timezone
from typing import Dict, Any, List
from category_scraper import CategoryScraper
from product_scraper import ProductScraper
from embedding_generator import EmbeddingGenerator
from supabase_uploader import SupabaseUploader
from config import SOURCE


class BoundScraper:
    BATCH_SIZE = 50
    EMBEDDING_DELAY = 0.5
    
    def __init__(self):
        print("=" * 60)
        print("Weare Bound Scraper - Starting")
        print("=" * 60)
        
        self.category_scraper = CategoryScraper()
        self.product_scraper = ProductScraper()
        self.embedding_generator = EmbeddingGenerator()
        self.supabase_uploader = SupabaseUploader()
        
        self.stats = {
            "new": 0,
            "updated": 0,
            "unchanged": 0,
            "failed": 0,
            "embedded": 0,
            "stale_deleted": 0,
        }
        
        self.existing_products = {}
        self.current_product_urls = []
    
    def run(self):
        try:
            if not self.supabase_uploader.check_connection():
                print("Failed to connect to Supabase. Exiting.")
                return
            
            print("\n[1/5] Fetching product URLs via Shopify API...")
            product_urls = self.category_scraper.scrape_all_categories()
            self.current_product_urls = list(product_urls)
            print(f"Found {len(product_urls)} unique products")
            
            print("\n[2/5] Fetching existing products from database...")
            self.existing_products = self.supabase_uploader.get_existing_products(
                SOURCE, self.current_product_urls
            )
            print(f"Found {len(self.existing_products)} existing products in database")
            
            print("\n[3/5] Scraping and processing products...")
            products = self._scrape_and_process_products(product_urls)
            
            print("\n[4/5] Uploading to Supabase (batched)...")
            self._upload_products_batched(products)
            
            print("\n[5/5] Cleaning up stale products...")
            self._cleanup_stale_products()
            
            self._print_stats()
            
        except KeyboardInterrupt:
            print("\nScraping interrupted by user")
            self._print_stats()
        except Exception as e:
            print(f"\nError during scraping: {e}")
            raise
        finally:
            self.cleanup()
    
    def _scrape_and_process_products(self, product_urls: set) -> List[Dict[str, Any]]:
        products = []
        total = len(product_urls)
        
        for i, url in enumerate(product_urls, 1):
            print(f"  [{i}/{total}] Processing: {url}")
            
            product_data = self.product_scraper.scrape_product(url)
            
            if not product_data:
                self.stats["failed"] += 1
                time.sleep(0.3)
                continue
            
            product_data["created_at"] = datetime.now(timezone.utc).isoformat()
            
            existing = self.existing_products.get(url)
            
            if existing:
                needs_update, needs_embedding = self._check_if_needs_update(existing, product_data)
                
                if not needs_update:
                    print(f"    Skipping (unchanged)")
                    self.stats["unchanged"] += 1
                    time.sleep(0.1)
                    continue
                
                if needs_embedding:
                    print(f"    Regenerating embeddings...")
                    self._generate_embeddings(product_data)
                else:
                    print(f"    Updating (data changed, keeping embeddings)")
                    if existing.get("image_embedding"):
                        product_data["image_embedding"] = existing["image_embedding"]
                    if existing.get("info_embedding"):
                        product_data["info_embedding"] = existing["info_embedding"]
                
                self.stats["updated"] += 1
            else:
                print(f"    New product, generating embeddings...")
                self._generate_embeddings(product_data)
                self.stats["new"] += 1
            
            products.append(product_data)
            time.sleep(0.3)
        
        return products
    
    def _check_if_needs_update(self, existing: Dict[str, Any], new_data: Dict[str, Any]) -> tuple[bool, bool]:
        needs_update = False
        needs_embedding = False
        
        if existing.get("title") != new_data.get("title"):
            needs_update = True
        
        if existing.get("price") != new_data.get("price"):
            needs_update = True
        
        if existing.get("sale") != new_data.get("sale"):
            needs_update = True
        
        if existing.get("description") != new_data.get("description"):
            needs_update = True
        
        if existing.get("category") != new_data.get("category"):
            needs_update = True
        
        if existing.get("size") != new_data.get("size"):
            needs_update = True
        
        existing_image = existing.get("image_url", "")
        new_image = new_data.get("image_url", "")
        
        if existing_image != new_image:
            needs_update = True
            needs_embedding = True
        
        if existing.get("additional_images") != new_data.get("additional_images"):
            needs_update = True
        
        if not existing.get("image_embedding") or not existing.get("info_embedding"):
            needs_embedding = True
        
        return needs_update, needs_embedding
    
    def _generate_embeddings(self, product: Dict[str, Any]):
        image_url = product.get("image_url")
        if image_url:
            image_embedding = self.embedding_generator.generate_image_embedding(image_url)
            if image_embedding:
                product["image_embedding"] = image_embedding
                self.stats["embedded"] += 1
            else:
                print(f"    Failed to generate image embedding")
        
        info_text = self.embedding_generator.generate_info_text(product)
        if info_text:
            info_embedding = self.embedding_generator.generate_text_embedding(info_text)
            if info_embedding:
                product["info_embedding"] = info_embedding
        
        time.sleep(self.EMBEDDING_DELAY)
    
    def _upload_products_batched(self, products: List[Dict[str, Any]]):
        total = len(products)
        print(f"  Uploading {total} products in batches of {self.BATCH_SIZE}...")
        
        for i in range(0, total, self.BATCH_SIZE):
            batch = products[i:i + self.BATCH_SIZE]
            batch_num = (i // self.BATCH_SIZE) + 1
            total_batches = (total + self.BATCH_SIZE - 1) // self.BATCH_SIZE
            
            print(f"  Batch {batch_num}/{total_batches}: {len(batch)} products")
            
            result = self.supabase_uploader.batch_upsert(batch)
            
            if result["failed"] > 0:
                self.stats["failed"] += result["failed"]
            
            time.sleep(0.5)
    
    def _cleanup_stale_products(self):
        deleted = self.supabase_uploader.delete_stale_products(
            SOURCE, self.current_product_urls
        )
        self.stats["stale_deleted"] = deleted
        if deleted > 0:
            print(f"  Deleted {deleted} stale products")
        else:
            print("  No stale products to delete")
    
    def _print_stats(self):
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETE - SUMMARY")
        print("=" * 60)
        print(f"New products added:        {self.stats['new']}")
        print(f"Products updated:         {self.stats['updated']}")
        print(f"Products unchanged:        {self.stats['unchanged']}")
        print(f"Products failed:           {self.stats['failed']}")
        print(f"Embeddings generated:     {self.stats['embedded']}")
        print(f"Stale products deleted:    {self.stats['stale_deleted']}")
        print("=" * 60)
    
    def cleanup(self):
        print("\nCleaning up resources...")
        self.category_scraper.close()
        self.product_scraper.close()
        self.embedding_generator.cleanup()
        print("Done")
