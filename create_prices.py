#!/usr/bin/env python3
"""
Script 3: create_prices.py

This script reads service plans and GCP SKUs data, then creates corresponding prices
in Morpheus. It maps GCP SKU categories to appropriate Morpheus price types and volume types.

Usage:
    python3 create_prices.py

Dependencies:
    pip install requests

Environment Variables:
    MORPHEUS_TOKEN = "9fcc4426-c89a-4430-b6d7-99d5950fc1cc"
    MORPHEUS_URL = "https://xdjmorpheapp01"
    PRICE_PREFIX = "IOH-CP"

Input Files:
    service_plans.json - From script 1
    gcp_skus.json - From script 2

Output:
    prices.json - Contains created price IDs
"""

import os
import sys
import json
import logging
import requests
import time
from typing import Dict, List, Any, Optional, Tuple

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
    
    def get_price_by_code(self, code: str) -> Optional[Dict]:
        """Get price by code"""
        params = {'code': code}
        response = self.make_request('GET', '/prices', params=params)
        if response.status_code == 200:
            data = response.json()
            prices = data.get('prices', [])
            for price in prices:
                if price.get('code') == code:
                    return price
        return None
    
    def create_price(self, price_data: Dict) -> Optional[Dict]:
        """Create a new price"""
        response = self.make_request('POST', '/prices', data=price_data)
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return data.get('price')
            else:
                logger.error(f"Price creation failed: {data}")
                return None
        else:
            logger.error(f"Failed to create price: {response.status_code} - {response.text}")
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

def determine_price_type_and_volume_type(sku: Dict) -> Tuple[str, str]:
    """
    Determine priceType and volumeType based on GCP SKU category
    
    Maps GCP resource types to Morpheus pricing model:
    - priceType: hourly, monthly, quantity, compute, memory, storage, dataTransfer, software
    - volumeType: hourly, monthly, yearly, fixed, usage
    """
    category = sku.get('category', {})
    resource_family = category.get('resourceFamily', '').lower()
    resource_group = category.get('resourceGroup', '').lower()
    usage_type = category.get('usageType', '').lower()
    description = sku.get('description', '').lower()
    
    # Default values
    price_type = 'compute'
    volume_type = 'hourly'
    
    # Determine price type based on resource family and group
    if 'compute' in resource_family:
        if 'memory' in resource_group or 'ram' in resource_group:
            price_type = 'memory'
        elif 'core' in resource_group or 'cpu' in resource_group or 'vcpu' in resource_group:
            price_type = 'compute'
        elif 'gpu' in resource_group:
            price_type = 'compute'  # GPUs are typically compute resources
        elif 'license' in resource_group or 'software' in resource_group:
            price_type = 'software'
        else:
            price_type = 'compute'
    
    elif 'storage' in resource_family:
        price_type = 'storage'
    
    elif 'network' in resource_family:
        if 'transfer' in resource_group or 'egress' in resource_group or 'ingress' in resource_group:
            price_type = 'dataTransfer'
        else:
            price_type = 'compute'  # Network resources like load balancers
    
    elif 'bigquery' in resource_family or 'data' in resource_family:
        if 'storage' in resource_group:
            price_type = 'storage'
        else:
            price_type = 'quantity'
    
    # Determine volume type based on usage type
    if 'preemptible' in usage_type or 'spot' in usage_type:
        volume_type = 'hourly'
    elif 'commit' in usage_type:
        if '1yr' in usage_type or 'year' in usage_type:
            volume_type = 'yearly'
        elif '1mo' in usage_type or 'month' in usage_type:
            volume_type = 'monthly'
        else:
            volume_type = 'monthly'
    elif 'ondemand' in usage_type:
        volume_type = 'hourly'
    else:
        # Analyze description for time-based patterns
        if any(term in description for term in ['per hour', 'hourly', '/hour']):
            volume_type = 'hourly'
        elif any(term in description for term in ['per month', 'monthly', '/month']):
            volume_type = 'monthly'
        elif any(term in description for term in ['per year', 'yearly', '/year']):
            volume_type = 'yearly'
        elif any(term in description for term in ['per gb', 'per request', 'per operation']):
            volume_type = 'usage'
        else:
            volume_type = 'hourly'  # Default
    
    return price_type, volume_type

def extract_price_value(sku: Dict) -> float:
    """Extract price value from GCP SKU pricing info"""
    pricing_info = sku.get('pricingInfo', [])
    if not pricing_info:
        return 0.0
    
    # Use the first pricing info entry
    price_info = pricing_info[0]
    pricing_expression = price_info.get('pricingExpression', {})
    tiered_rates = pricing_expression.get('tieredRates', [])
    
    if not tiered_rates:
        return 0.0
    
    # Use the first tier rate
    first_tier = tiered_rates[0]
    unit_price = first_tier.get('unitPrice', {})
    
    # Convert from nanos to dollars
    nanos = unit_price.get('nanos', 0)
    units = int(unit_price.get('units', 0))
    
    price_value = units + (nanos / 1000000000.0)
    return price_value

def create_price_data(sku: Dict, service_plan_id: int, prefix: str) -> Dict[str, Any]:
    """Create price data structure for Morpheus API"""
    price_type, volume_type = determine_price_type_and_volume_type(sku)
    price_value = extract_price_value(sku)
    
    # Create unique code for the price
    sku_id = sku.get('skuId', '')
    code = f"{prefix.lower()}-gcp-{sku_id}".replace('_', '-').replace(' ', '-').lower()
    
    # Create name from description
    description = sku.get('description', '')
    name = f"{prefix} {description}"[:100]  # Limit length
    
    return {
        "price": {
            "name": name,
            "code": code,
            "account": None,  # Global price
            "priceType": price_type,
            "priceUnit": "hour",  # Default unit
            "incurCharges": "running",
            "currency": "USD",
            "cost": price_value,
            "markupType": "fixed",
            "markup": 0.0,
            "markupPercent": 0.0,
            "customPrice": price_value,
            "volumeType": volume_type,
            "datastore": None,
            "crossCloudApply": False,
            "servicePlan": {
                "id": service_plan_id
            },
            "config": {
                "gcpSkuId": sku.get('skuId'),
                "gcpResourceFamily": sku.get('resourceFamily'),
                "gcpResourceGroup": sku.get('resourceGroup'),
                "gcpUsageType": sku.get('usageType'),
                "gcpDescription": description
            }
        }
    }

def save_prices(prices: List[Dict], filename: str = 'prices.json') -> None:
    """Save prices data to JSON file"""
    try:
        with open(filename, 'w') as f:
            json.dump({
                'prices': prices,
                'metadata': {
                    'timestamp': time.time(),
                    'count': len(prices)
                }
            }, f, indent=2)
        logger.info(f"Prices data saved to {filename}")
    except Exception as e:
        logger.error(f"Failed to save prices data: {e}")
        raise

def main():
    """Main function to create prices from GCP SKUs"""
    logger.info("Starting price creation from GCP SKUs")
    
    # Load environment variables
    env_vars = load_environment()
    
    # Load input data
    service_plans_data = load_json_file('service_plans.json')
    gcp_skus_data = load_json_file('gcp_skus.json')
    
    service_plans = service_plans_data.get('servicePlans', [])
    gcp_skus = gcp_skus_data.get('skus', [])
    
    if not service_plans:
        logger.error("No service plans found in service_plans.json")
        sys.exit(1)
    
    if not gcp_skus:
        logger.error("No GCP SKUs found in gcp_skus.json")
        sys.exit(1)
    
    # Use the first service plan
    service_plan = service_plans[0]
    service_plan_id = service_plan['id']
    
    logger.info(f"Using service plan: {service_plan['name']} (ID: {service_plan_id})")
    logger.info(f"Processing {len(gcp_skus)} GCP SKUs")
    
    # Initialize Morpheus API client
    client = MorpheusAPIClient(env_vars['MORPHEUS_URL'], env_vars['MORPHEUS_TOKEN'])
    
    created_prices = []
    skipped_count = 0
    error_count = 0
    
    try:
        for i, sku in enumerate(gcp_skus):
            sku_id = sku.get('skuId', '')
            logger.info(f"Processing SKU {i+1}/{len(gcp_skus)}: {sku_id}")
            
            # Create price code
            price_code = f"{env_vars['PRICE_PREFIX'].lower()}-gcp-{sku_id}".replace('_', '-').replace(' ', '-').lower()
            
            # Check if price already exists
            existing_price = client.get_price_by_code(price_code)
            if existing_price:
                logger.info(f"Price already exists for SKU {sku_id}: {existing_price['name']} (ID: {existing_price['id']})")
                created_prices.append(existing_price)
                continue
            
            # Skip SKUs with no pricing info
            if not sku.get('pricingInfo'):
                logger.warning(f"Skipping SKU {sku_id}: No pricing information")
                skipped_count += 1
                continue
            
            # Create price data
            price_data = create_price_data(sku, service_plan_id, env_vars['PRICE_PREFIX'])
            
            # Create price in Morpheus
            try:
                created_price = client.create_price(price_data)
                if created_price:
                    logger.info(f"Created price for SKU {sku_id}: {created_price['name']} (ID: {created_price['id']})")
                    created_prices.append(created_price)
                else:
                    logger.error(f"Failed to create price for SKU {sku_id}")
                    error_count += 1
            except Exception as e:
                logger.error(f"Error creating price for SKU {sku_id}: {e}")
                error_count += 1
                continue
        
        # Save created prices
        save_prices(created_prices)
        
        logger.info(f"Price creation completed:")
        logger.info(f"  Created: {len(created_prices)}")
        logger.info(f"  Skipped: {skipped_count}")
        logger.info(f"  Errors: {error_count}")
        
        return created_prices
        
    except Exception as e:
        logger.error(f"Error during price creation: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()