import requests
import time
from typing import Optional
from bs4 import BeautifulSoup
from config import HEADERS, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY
from tenacity import retry, stop_after_attempt, wait_fixed


class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    @retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_fixed(RETRY_DELAY))
    def get(self, url: str, timeout: int = REQUEST_TIMEOUT) -> Optional[requests.Response]:
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Request failed for {url}: {e}")
            raise
    
    def close(self):
        self.session.close()


def parse_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def extract_product_links_from_collection(soup: BeautifulSoup) -> list[str]:
    product_urls = []
    
    links = soup.find_all('a', href=True)
    for link in links:
        href = link['href']
        if '/products/' in href and href not in product_urls:
            if not href.startswith('http'):
                href = f"https://wearebound.co.uk{href}"
            if href not in product_urls:
                product_urls.append(href)
    
    return product_urls


def has_next_page(soup: BeautifulSoup) -> bool:
    next_link = soup.find('a', {'rel': 'next'})
    if next_link:
        return True
    
    pagination = soup.find('nav', class_='pagination')
    if pagination:
        links = pagination.find_all('a', href=True)
        for link in links:
            if 'page=' in link['href']:
                return True
    
    return False


def get_next_page_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    next_link = soup.find('a', {'rel': 'next'})
    if next_link and next_link.get('href'):
        href = next_link['href']
        if not href.startswith('http'):
            href = f"https://wearebound.co.uk{href}"
        return href
    
    pagination = soup.find('nav', class_='pagination')
    if pagination:
        current_page = 1
        links = pagination.find_all('a', href=True)
        for link in links:
            href = link['href']
            if 'page=' in href:
                try:
                    page_num = int(href.split('page=')[1].split('&')[0])
                    if page_num > current_page:
                        current_page = page_num
                        next_url = href
                        if not next_url.startswith('http'):
                            next_url = f"https://wearebound.co.uk{next_url}"
                        return next_url
                except (ValueError, IndexError):
                    pass
    
    return None
