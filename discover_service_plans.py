#!/usr/bin/env python3
"""
Script 1: discover_service_plans.py

This script discovers existing service plans from HPE Morpheus v8.0.7.
It retrieves all service plans via GET /api/service-plans and stores them in service_plans.json.

Usage:
    python3 discover_service_plans.py

Dependencies:
    pip install requests

Environment Variables:
    MORPHEUS_TOKEN = "9fcc4426-c89a-4430-b6d7-99d5950fc1cc"
    MORPHEUS_URL = "https://xdjmorpheapp01"
    PRICE_PREFIX = "IOH-CP"

Output:
    service_plans.json - Contains discovered service plan data
"""

import os
import sys
import json
import logging
import requests
import time
import urllib3
from typing import Optional, Dict, Any, List

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        # Disable SSL verification for self-signed certificates
        self.session.verify = False
        logger.warning("SSL certificate verification disabled - this is acceptable for self-signed certificates but should be used with caution in production")
    
    def make_request(self, method: str, endpoint: str, data: Dict = None, params: Dict = None, max_retries: int = 3) -> requests.Response:
        """Make HTTP request with retry logic"""
        url = f"{self.base_url}/api/{endpoint.lstrip('/')}"
        
        for attempt in range(max_retries):
            try:
                response = self.session.request(method, url, json=data, params=params, verify=False)
                logger.debug(f"{method} {url} - Status: {response.status_code}")
                
                if response.status_code < 500:  # Don't retry on client errors
                    return response
                    
            except requests.exceptions.SSLError as e:
                if "CERTIFICATE_VERIFY_FAILED" in str(e):
                    logger.warning(f"SSL certificate verification failed (attempt {attempt + 1}/{max_retries}): {e}")
                    logger.warning("This is expected behavior with self-signed certificates - continuing with verification disabled")
                else:
                    logger.warning(f"SSL error (attempt {attempt + 1}/{max_retries}): {e}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait 1 second before retry
                
        raise Exception(f"Request failed after {max_retries} attempts")
    
    def get_all_service_plans(self, max_results: int = 10000) -> List[Dict]:
        """Get all service plans without filtering"""
        all_plans = []
        offset = 0
        page_size = 100  # Reasonable page size
        
        while True:
            params = {
                'max': page_size,
                'offset': offset
            }
            
            response = self.make_request('GET', '/service-plans', params=params)
            if response.status_code == 200:
                data = response.json()
                plans = data.get('servicePlans', [])
                
                if not plans:
                    break  # No more results
                    
                all_plans.extend(plans)
                logger.info(f"Retrieved {len(plans)} service plans (offset: {offset}, total so far: {len(all_plans)})")
                
                # Check if we've reached the end
                if len(plans) < page_size:
                    break
                    
                offset += page_size
                
                # Safety check to prevent infinite loops
                if len(all_plans) >= max_results:
                    logger.warning(f"Reached maximum results limit ({max_results})")
                    break
                    
            else:
                logger.error(f"Failed to get service plans: {response.status_code} - {response.text}")
                break
        
        return all_plans

def load_environment() -> Dict[str, str]:
    """Load and validate environment variables"""
    required_vars = ['MORPHEUS_TOKEN', 'MORPHEUS_URL']
    env_vars = {}
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            logger.error(f"Missing required environment variable: {var}")
            sys.exit(1)
        env_vars[var] = value
    
    # PRICE_PREFIX is optional for discovery
    env_vars['PRICE_PREFIX'] = os.getenv('PRICE_PREFIX', 'IOH-CP')
    
    logger.info("Environment variables loaded successfully")
    return env_vars

def save_service_plans(service_plans: List[Dict], filename: str = 'service_plans.json') -> None:
    """Save service plans data to JSON file"""
    try:
        # Check if file exists for idempotency logging
        file_exists = os.path.exists(filename)
        if file_exists:
            logger.info(f"File {filename} exists, will overwrite with new data")
        
        with open(filename, 'w') as f:
            json.dump({
                'servicePlans': service_plans,
                'metadata': {
                    'timestamp': time.time(),
                    'count': len(service_plans),
                    'discovery_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'source': 'morpheus-api'
                }
            }, f, indent=2)
        logger.info(f"Service plans data saved to {filename}")
    except Exception as e:
        logger.error(f"Failed to save service plans data: {e}")
        raise

def main():
    """Main function to discover all service plans from Morpheus"""
    logger.info("Starting service plan discovery from Morpheus")
    
    # Load environment variables
    env_vars = load_environment()
    
    # Initialize Morpheus API client
    client = MorpheusAPIClient(env_vars['MORPHEUS_URL'], env_vars['MORPHEUS_TOKEN'])
    
    try:
        # Get all service plans
        logger.info("Retrieving all service plans from Morpheus...")
        service_plans = client.get_all_service_plans()
        
        if service_plans:
            logger.info(f"Successfully discovered {len(service_plans)} service plans")
            
            # Log some basic statistics
            provision_types = {}
            for plan in service_plans:
                prov_type = plan.get('provisionType', {}).get('name', 'Unknown')
                provision_types[prov_type] = provision_types.get(prov_type, 0) + 1
            
            logger.info("Service plans by provision type:")
            for prov_type, count in sorted(provision_types.items()):
                logger.info(f"  {prov_type}: {count}")
                
        else:
            logger.warning("No service plans found in Morpheus")
        
        # Save service plans data (even if empty, for idempotency)
        save_service_plans(service_plans)
        
        logger.info("Service plan discovery completed successfully")
        return service_plans
        
    except Exception as e:
        logger.error(f"Error during service plan discovery: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()