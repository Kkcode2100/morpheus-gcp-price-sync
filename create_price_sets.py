#!/usr/bin/env python3
"""
Script 4: create_price_sets.py

This script reads prices data and groups them into logical price sets based on
resource family and group categories. It creates price sets in Morpheus to organize
related pricing components.

Usage:
    python3 create_price_sets.py

Dependencies:
    pip install requests

Environment Variables:
    MORPHEUS_TOKEN = "9fcc4426-c89a-4430-b6d7-99d5950fc1cc"
    MORPHEUS_URL = "https://xdjmorpheapp01"
    PRICE_PREFIX = "IOH-CP"

Input Files:
    prices.json - From script 3

Output:
    price_sets.json - Contains created price set IDs
"""

import os
import sys
import json
import logging
import requests
import time
from typing import Dict, List, Any, Optional
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MorpheusAPIClient:
    """Client for interacting with Morpheus API"""
    
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        })
    
    def make_request(self, method: str, endpoint: str, data: Dict = None, params: Dict = None, max_retries: int = 3) -> requests.Response:
        """Make HTTP request with retry logic"""
        url = f"{self.base_url}/api/{endpoint.lstrip('/')}"
        
        for attempt in range(max_retries):
            try:
                response = self.session.request(method, url, json=data, params=params)
                logger.debug(f"{method} {url} - Status: {response.status_code}")
                
                if response.status_code < 500:  # Don't retry on client errors
                    return response
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait 1 second before retry
                
        raise Exception(f"Request failed after {max_retries} attempts")
    
    def get_price_sets(self, params: Dict = None) -> List[Dict]:
        """Get all price sets"""
        response = self.make_request('GET', '/price-sets', params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get('priceSets', [])
        else:
            logger.error(f"Failed to get price sets: {response.status_code} - {response.text}")
            return []
    
    def get_price_set_by_code(self, code: str) -> Optional[Dict]:
        """Get price set by code"""
        price_sets = self.get_price_sets()
        for price_set in price_sets:
            if price_set.get('code') == code:
                return price_set
        return None
    
    def create_price_set(self, price_set_data: Dict) -> Optional[Dict]:
        """Create a new price set"""
        response = self.make_request('POST', '/price-sets', data=price_set_data)
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return data.get('priceSet')
            else:
                logger.error(f"Price set creation failed: {data}")
                return None
        else:
            logger.error(f"Failed to create price set: {response.status_code} - {response.text}")
            return None

def load_environment() -> Dict[str, str]:
    """Load and validate environment variables"""
    required_vars = ['MORPHEUS_TOKEN', 'MORPHEUS_URL', 'PRICE_PREFIX']
    env_vars = {}
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            logger.error(f"Missing required environment variable: {var}")
            sys.exit(1)
        env_vars[var] = value
    
    logger.info("Environment variables loaded successfully")
    return env_vars

def load_json_file(filename: str) -> Dict:
    """Load data from JSON file"""
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
        logger.info(f"Loaded data from {filename}")
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {filename}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {filename}: {e}")
        sys.exit(1)

def group_prices_by_category(prices: List[Dict]) -> Dict[str, List[Dict]]:
    """Group prices by resource family and group"""
    categories = defaultdict(list)
    
    for price in prices:
        config = price.get('config', {})
        resource_family = config.get('gcpResourceFamily', 'Unknown')
        resource_group = config.get('gcpResourceGroup', 'Unknown')
        
        # Clean up category names
        if not resource_family or resource_family == 'None':
            resource_family = 'General'
        if not resource_group or resource_group == 'None':
            resource_group = 'Standard'
        
        # Create category key
        category_key = f"{resource_family}-{resource_group}".replace(' ', '-').replace('_', '-').lower()
        categories[category_key].append(price)
    
    return dict(categories)

def create_price_set_data(category_name: str, prices: List[Dict], prefix: str) -> Dict[str, Any]:
    """Create price set data structure for Morpheus API"""
    # Create readable name from category
    name_parts = category_name.split('-')
    display_name = ' '.join(word.capitalize() for word in name_parts)
    
    # Create unique code
    code = f"{prefix.lower()}-gcp-{category_name}".replace('_', '-').replace(' ', '-').lower()
    
    # Extract price IDs
    price_ids = [price['id'] for price in prices]
    
    # Create description based on resource types
    sample_price = prices[0] if prices else {}
    config = sample_price.get('config', {})
    resource_family = config.get('gcpResourceFamily', 'General')
    
    description = f"GCP {display_name} pricing for {resource_family} resources"
    
    return {
        "priceSet": {
            "name": f"{prefix} GCP {display_name}",
            "code": code,
            "description": description,
            "priceUnit": "hour",
            "type": "fixed",
            "regionCode": None,  # Global price set
            "zonePool": None,
            "active": True,
            "systemCreated": False,
            "prices": [{"id": price_id} for price_id in price_ids],
            "config": {
                "gcpResourceFamily": resource_family,
                "category": category_name,
                "priceCount": len(prices)
            }
        }
    }

def save_price_sets(price_sets: List[Dict], filename: str = 'price_sets.json') -> None:
    """Save price sets data to JSON file"""
    try:
        with open(filename, 'w') as f:
            json.dump({
                'priceSets': price_sets,
                'metadata': {
                    'timestamp': time.time(),
                    'count': len(price_sets)
                }
            }, f, indent=2)
        logger.info(f"Price sets data saved to {filename}")
    except Exception as e:
        logger.error(f"Failed to save price sets data: {e}")
        raise

def main():
    """Main function to create price sets from grouped prices"""
    logger.info("Starting price set creation from grouped prices")
    
    # Load environment variables
    env_vars = load_environment()
    
    # Load input data
    prices_data = load_json_file('prices.json')
    prices = prices_data.get('prices', [])
    
    if not prices:
        logger.error("No prices found in prices.json")
        sys.exit(1)
    
    logger.info(f"Processing {len(prices)} prices")
    
    # Group prices by category
    price_categories = group_prices_by_category(prices)
    
    logger.info(f"Grouped prices into {len(price_categories)} categories:")
    for category, price_list in price_categories.items():
        logger.info(f"  {category}: {len(price_list)} prices")
    
    # Initialize Morpheus API client
    client = MorpheusAPIClient(env_vars['MORPHEUS_URL'], env_vars['MORPHEUS_TOKEN'])
    
    created_price_sets = []
    skipped_count = 0
    error_count = 0
    
    try:
        for category_name, category_prices in price_categories.items():
            logger.info(f"Processing category: {category_name} ({len(category_prices)} prices)")
            
            # Create price set code
            price_set_code = f"{env_vars['PRICE_PREFIX'].lower()}-gcp-{category_name}".replace('_', '-').replace(' ', '-').lower()
            
            # Check if price set already exists
            existing_price_set = client.get_price_set_by_code(price_set_code)
            if existing_price_set:
                logger.info(f"Price set already exists for category {category_name}: {existing_price_set['name']} (ID: {existing_price_set['id']})")
                created_price_sets.append(existing_price_set)
                continue
            
            # Skip categories with no prices
            if not category_prices:
                logger.warning(f"Skipping category {category_name}: No prices")
                skipped_count += 1
                continue
            
            # Create price set data
            price_set_data = create_price_set_data(category_name, category_prices, env_vars['PRICE_PREFIX'])
            
            # Create price set in Morpheus
            try:
                created_price_set = client.create_price_set(price_set_data)
                if created_price_set:
                    logger.info(f"Created price set for category {category_name}: {created_price_set['name']} (ID: {created_price_set['id']})")
                    created_price_sets.append(created_price_set)
                else:
                    logger.error(f"Failed to create price set for category {category_name}")
                    error_count += 1
            except Exception as e:
                logger.error(f"Error creating price set for category {category_name}: {e}")
                error_count += 1
                continue
        
        # Save created price sets
        save_price_sets(created_price_sets)
        
        logger.info(f"Price set creation completed:")
        logger.info(f"  Created: {len(created_price_sets)}")
        logger.info(f"  Skipped: {skipped_count}")
        logger.info(f"  Errors: {error_count}")
        
        return created_price_sets
        
    except Exception as e:
        logger.error(f"Error during price set creation: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()