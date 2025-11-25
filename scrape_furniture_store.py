"""
Product Scraper for a furniture store
------------------------------

This script scrapes product data (SKU, price, coupon message, URL)
from the website across multiple brands and categories.

Workflow:
    brand > categories > product pages

Each final product row includes:
    - SKU
    - Price
    - Coupon message (if present)
    - Product page URL

Output:
    A CSV file per brand.

Requirements:
    - requests
    - beautifulsoup4
"""

import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# A placeholder to save headaches
BASE_URL = 'https://www.example.com/'


def scrape_product(url: str) -> list:
    """
        Scrape a single product page for SKU, price, and coupon message.

        Args:
            url (str): Full URL of the product page.

        Returns:
            list: [SKU, price, coupon_message, product_url]
    """

    response = requests.get(url)
    html = response.content
    soup = BeautifulSoup(html, 'html.parser')

    # Extract SKU from the H1 heading
    heading = soup.find('h1', class_='product-details-full-content-header-title')
    sku = heading.text.split(' - ')[-1].strip()

    # Extract the price (remove "$" and commas)
    price_span_element = soup.find('span', class_='product-views-price-lead')
    price = price_span_element.text.replace('$', '').replace(',', '').strip()

    # Extract coupon message (if present)
    coupon_div_element = soup.find('div', id='special-coupon-message-container')
    coupon_b_element = coupon_div_element.find('b')
    coupon_message = coupon_b_element.text if coupon_b_element else ''
    print('Scraped product', sku)

    return [sku, price, coupon_message, url]


def scrape_category(url: str) -> list:
    """
        Scrape all product links within a category page.

        Args:
            url (str): Full URL of a category page.

        Returns:
            list: List of product rows returned by scrape_product().
    """

    response = requests.get(url)
    html = response.content
    soup = BeautifulSoup(html, 'html.parser')

    # Get all product links inside category (product list view)
    product_anchor_tags = soup.find_all('a', class_='facets-item-cell-grid-title')
    product_links = [urljoin(BASE_URL, a['href']) for a in product_anchor_tags]

    # Scrape each product page
    products = [scrape_product(l) for l in product_links]

    return products


def scrape_brand(url: str) -> list:
    """
        Scrape all categories under a brand and all products within them.

        Args:
            url (str): Full URL of the brand page.

        Returns:
            list: Combined list of all products for this brand.
    """

    response = requests.get(url)
    html = response.content
    soup = BeautifulSoup(html, 'html.parser')

    # Extract category links under this brand
    category_a_tags = soup.find_all('a', class_='facets-category-cell-anchor')
    category_links = [urljoin(BASE_URL, a['href']) for a in category_a_tags]

    products = []

    # Scrape all categories
    for category_url in category_links:
        category_products = scrape_category(category_url)
        products += category_products

    return products


def write_data(brand_name: str, products: list) -> None:
    """
       Write scraped product data into a CSV file.

       Args:
           brand_name (str): The brand name used for the CSV filename.
           products (list): List of product rows.
    """

    with open(f'{brand_name}.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['SKU', 'Price', 'Comment', 'URL'])
        writer.writerows(products)

    print('Your data was saved to', f'{brand_name}.csv')


def main():
    """
        Main workflow:
        - Build brand URLs
        - Scrape each brand
        - Write CSV output
    """

    brands = [
        'brands-legacy-classic-furniture',
        'brands-martin-furniture'
    ]
    
    for brand in brands:
        name = brand.replace('brands-', '', 1)
        url = urljoin(urljoin(BASE_URL, 'brands/'), brand)
        print('Scraping brand', name)
        products = scrape_brand(url)
        write_data(name, products)


if __name__ == '__main__':
    main()
