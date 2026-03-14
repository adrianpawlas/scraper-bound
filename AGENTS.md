# Weare Bound Scraper

Full scraper for fashion store Weare Bound that extracts products, generates embeddings, and uploads to Supabase.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. The scraper uses the following configuration (already set in config.py):
- Supabase URL: https://yqawmzggcgpeyaaynrjk.supabase.co
- Categories: all, shop-accessories, shop-sale
- Embedding model: google/siglip-base-patch16-384 (768-dim)

## Running the Scraper

### Manual Run
```bash
python run.py
```

### Automated Run (GitHub Actions)
The scraper runs automatically every day at midnight via GitHub Actions. You can also trigger it manually:

1. Go to https://github.com/adrianpawlas/scraper-bound/actions
2. Click on "Run Weare Bound Scraper"
3. Click "Run workflow"

## Process Flow

1. **Product URL Collection**: Fetches all product URLs via Shopify API (handles pagination automatically)
2. **Product Scraping**: Scrapes each product page for details (title, price, images, sizes, etc.)
3. **Embedding Generation**: Creates 768-dim embeddings for:
   - Product images (SigLIP model)
   - Product info text (title, description, price, category, etc.)
4. **Supabase Upload**: Inserts all data into the products table (updates existing products)

## Output Fields

- `id`: Product slug from URL
- `source`: "scraper-bound"
- `brand`: "Bound"
- `product_url`: Product URL
- `image_url`: Primary product image
- `additional_images`: Other product images (comma-separated)
- `title`: Product name
- `description`: Product description
- `category`: Category
- `gender`: "man" or "woman"
- `price`: Original price (GBP)
- `sale`: Sale price (same as price if no sale)
- `second_hand`: false
- `metadata`: All product details combined
- `size`: Available sizes (comma-separated)
- `image_embedding`: 768-dim image embedding
- `info_embedding`: 768-dim text embedding
- `country`: "GB"
- `created_at`: Import timestamp
