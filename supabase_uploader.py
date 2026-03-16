from typing import Dict, Any, List, Optional
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_ANON_KEY
import time
import json
from datetime import datetime, timezone


class SupabaseUploader:
    def __init__(self):
        print(f"Connecting to Supabase: {SUPABASE_URL}")
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        print("Connected to Supabase successfully")
    
    def check_connection(self) -> bool:
        try:
            response = self.client.table('products').select('id').limit(1).execute()
            print("Supabase connection verified")
            return True
        except Exception as e:
            print(f"Supabase connection failed: {e}")
            return False
    
    def get_existing_products(self, source: str, product_urls: List[str]) -> Dict[str, Dict[str, Any]]:
        if not product_urls:
            return {}
        
        try:
            response = self.client.table('products').select(
                "id, product_url, title, price, sale, image_url, additional_images, description, category, size, metadata"
            ).eq("source", source).in_("product_url", product_urls).execute()
            
            return {p["product_url"]: p for p in response.data}
        except Exception as e:
            print(f"    Error fetching existing products: {e}")
            return {}
    
    def batch_upsert(self, products: List[Dict[str, Any]], max_retries: int = 3) -> Dict[str, int]:
        if not products:
            return {"success": 0, "failed": 0}
        
        records = [self._prepare_record(p) for p in products]
        
        for attempt in range(max_retries):
            try:
                response = self.client.table('products').upsert(
                    records,
                    on_conflict='source, product_url'
                ).execute()
                
                if response.data:
                    return {"success": len(products), "failed": 0}
                else:
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    return {"success": 0, "failed": len(products)}
                    
            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    print(f"    Batch insert attempt {attempt + 1} failed: {e}, retrying...")
                    time.sleep(1)
                    continue
                
                self._log_failed_products(products, error_msg)
                return {"success": 0, "failed": len(products)}
        
        return {"success": 0, "failed": len(products)}
    
    def delete_stale_products(self, source: str, current_product_urls: List[str]) -> int:
        if not current_product_urls:
            return 0
        
        try:
            response = self.client.table('products').select(
                "id, product_url, updated_at"
            ).eq("source", source).execute()
            
            all_products = response.data
            current_urls_set = set(current_product_urls)
            
            stale_products = [
                p for p in all_products 
                if p.get("product_url") not in current_urls_set
            ]
            
            if not stale_products:
                return 0
            
            stale_ids = [p["id"] for p in stale_products]
            
            deleted_count = 0
            for i in range(0, len(stale_ids), 50):
                batch_ids = stale_ids[i:i+50]
                try:
                    self.client.table('products').delete().in_("id", batch_ids).execute()
                    deleted_count += len(batch_ids)
                except Exception as e:
                    print(f"    Error deleting stale products: {e}")
            
            return deleted_count
            
        except Exception as e:
            print(f"    Error finding stale products: {e}")
            return 0
    
    def _prepare_record(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        
        record = {
            "id": product_data.get("id"),
            "source": product_data.get("source"),
            "product_url": product_data.get("product_url"),
            "brand": product_data.get("brand"),
            "title": product_data.get("title"),
            "description": product_data.get("description"),
            "category": product_data.get("category"),
            "gender": product_data.get("gender"),
            "price": product_data.get("price"),
            "sale": product_data.get("sale"),
            "second_hand": product_data.get("second_hand", False),
            "country": product_data.get("country"),
            "image_url": product_data.get("image_url"),
            "additional_images": product_data.get("additional_images"),
            "metadata": product_data.get("metadata"),
            "size": product_data.get("size"),
            "created_at": product_data.get("created_at"),
            "updated_at": now,
        }
        
        if product_data.get("image_embedding"):
            record["image_embedding"] = product_data["image_embedding"]
        
        if product_data.get("info_embedding"):
            record["info_embedding"] = product_data["info_embedding"]
        
        if product_data.get("affiliate_url"):
            record["affiliate_url"] = product_data["affiliate_url"]
        
        return record
    
    def _log_failed_products(self, products: List[Dict[str, Any]], error: str):
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"failed_products_{timestamp}.log"
            
            log_data = {
                "timestamp": datetime.now().isoformat(),
                "error": error,
                "products": [{"id": p.get("id"), "product_url": p.get("product_url")} for p in products]
            }
            
            with open(filename, 'w') as f:
                json.dump(log_data, f, indent=2)
            
            print(f"    Logged {len(products)} failed products to {filename}")
        except Exception as e:
            print(f"    Error logging failed products: {e}")
    
    def close(self):
        pass
