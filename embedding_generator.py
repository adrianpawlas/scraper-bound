import io
import numpy as np
import requests
from typing import Optional, List
from PIL import Image
import torch
from transformers import AutoModel, AutoProcessor
from config import EMBEDDING_MODEL, EMBEDDING_DIMENSION


class EmbeddingGenerator:
    def __init__(self):
        print(f"Loading embedding model: {EMBEDDING_MODEL}")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Using device: {self.device}")
        
        self.model = AutoModel.from_pretrained(EMBEDDING_MODEL)
        self.processor = AutoProcessor.from_pretrained(EMBEDDING_MODEL)
        self.model.to(self.device)
        self.model.eval()
        
        print("Model loaded successfully")
    
    def generate_image_embedding(self, image_url: str) -> Optional[List[float]]:
        try:
            response = requests.get(image_url, timeout=30)
            if response.status_code != 200:
                print(f"    Failed to download image: {image_url}")
                return None
            
            image = Image.open(io.BytesIO(response.content)).convert("RGB")
            
            inputs = self.processor(images=image, return_tensors="pt")
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.model.get_image_features(**inputs)
                if hasattr(outputs, 'last_hidden_state'):
                    outputs = outputs.last_hidden_state
                elif hasattr(outputs, 'pooler_output'):
                    outputs = outputs.pooler_output
                else:
                    outputs = outputs
            
            embedding = outputs.cpu().numpy()[0].tolist()
            
            if len(embedding) != EMBEDDING_DIMENSION:
                print(f"    Warning: Embedding dimension mismatch. Expected {EMBEDDING_DIMENSION}, got {len(embedding)}")
            
            return embedding
            
        except Exception as e:
            print(f"    Error generating image embedding: {e}")
            return None
    
    def generate_text_embedding(self, text: str) -> Optional[List[float]]:
        try:
            truncated_text = text[:500]
            
            inputs = self.processor(text=truncated_text, return_tensors="pt", padding=True, truncation=True, max_length=64)
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.model.get_text_features(**inputs)
                if hasattr(outputs, 'last_hidden_state'):
                    outputs = outputs.last_hidden_state
                elif hasattr(outputs, 'pooler_output'):
                    outputs = outputs.pooler_output
                else:
                    outputs = outputs
            
            embedding = outputs.cpu().numpy()[0].tolist()
            
            if len(embedding) != EMBEDDING_DIMENSION:
                print(f"    Warning: Text embedding dimension mismatch. Expected {EMBEDDING_DIMENSION}, got {len(embedding)}")
            
            return embedding
            
        except Exception as e:
            print(f"    Error generating text embedding: {e}")
            return None
    
    def generate_info_text(self, product_data: dict) -> str:
        parts = []
        
        if product_data.get("title"):
            parts.append(f"Title: {product_data['title']}")
        
        if product_data.get("brand"):
            parts.append(f"Brand: {product_data['brand']}")
        
        if product_data.get("category"):
            parts.append(f"Category: {product_data['category']}")
        
        if product_data.get("gender"):
            parts.append(f"Gender: {product_data['gender']}")
        
        if product_data.get("price"):
            parts.append(f"Price: {product_data['price']}")
        
        if product_data.get("description"):
            parts.append(f"Description: {product_data['description']}")
        
        if product_data.get("metadata"):
            parts.append(f"Details: {product_data['metadata']}")
        
        if product_data.get("size"):
            parts.append(f"Sizes: {product_data['size']}")
        
        return " | ".join(parts)
    
    def cleanup(self):
        if hasattr(self, 'model'):
            del self.model
        if hasattr(self, 'processor'):
            del self.processor
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
