from typing import Dict, Any, Optional
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_ANON_KEY
import json


class SupabaseUploader:
    def __init__(self):
        print(f"Connecting to Supabase: {SUPABASE_URL}")
        
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        print("Connected to Supabase successfully")
    
    def insert_product(self, product_data: Dict[str, Any]) -> bool:
        try:
            record = self._prepare_record(product_data)
            
            response = self.client.table('products').upsert(
                record,
                on_conflict='source, product_url'
            ).execute()
            
            if response.data:
                print(f"    Upserted: {product_data.get('id', 'unknown')}")
                return True
            else:
                print(f"    Failed: {product_data.get('id', 'unknown')}")
                return False
                
        except Exception as e:
            print(f"    Error upserting product: {e}")
            return False
    
    def _prepare_record(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
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
        }
        
        if product_data.get("image_embedding"):
            record["image_embedding"] = product_data["image_embedding"]
        
        if product_data.get("info_embedding"):
            record["info_embedding"] = product_data["info_embedding"]
        
        if product_data.get("affiliate_url"):
            record["affiliate_url"] = product_data["affiliate_url"]
        
        return record
    
    def check_connection(self) -> bool:
        try:
            response = self.client.table('products').select('id').limit(1).execute()
            print("Supabase connection verified")
            return True
        except Exception as e:
            print(f"Supabase connection failed: {e}")
            return False
    
    def close(self):
        pass
