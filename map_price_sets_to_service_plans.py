#!/usr/bin/env python3
"""
Script 5: map_price_sets_to_service_plans.py

This script maps created price sets to service plans in Morpheus, completing the
integration between Google Cloud pricing and Morpheus costing.

Usage:
    python3 map_price_sets_to_service_plans.py

Dependencies:
    pip install requests

Environment Variables:
    MORPHEUS_TOKEN = "9fcc4426-c89a-4430-b6d7-99d5950fc1cc"
    MORPHEUS_URL = "https://xdjmorpheapp01"
    PRICE_PREFIX = "IOH-CP"

Input Files:
    service_plans.json - From script 1
    price_sets.json - From script 4

Output:
    Integration completion status and updated service plan mappings
"""

import os
import sys
import json
import logging
import requests
import time
from typing import Dict, List, Any, Optional

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
    
    def get_service_plan(self, plan_id: int) -> Optional[Dict]:
        """Get service plan by ID"""
        response = self.make_request('GET', f'/service-plans/{plan_id}')
        if response.status_code == 200:
            data = response.json()
            return data.get('servicePlan')
        else:
            logger.error(f"Failed to get service plan {plan_id}: {response.status_code} - {response.text}")
            return None
    
    def update_service_plan(self, plan_id: int, service_plan_data: Dict) -> Optional[Dict]:
        """Update service plan"""
        response = self.make_request('PUT', f'/service-plans/{plan_id}', data=service_plan_data)
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return data.get('servicePlan')
            else:
                logger.error(f"Service plan update failed: {data}")
                return None
        else:
            logger.error(f"Failed to update service plan {plan_id}: {response.status_code} - {response.text}")
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

def get_existing_price_set_mappings(service_plan: Dict) -> List[Dict]:
    """Extract existing price set mappings from service plan"""
    existing_mappings = []
    
    # Check for existing priceSets in service plan
    price_sets = service_plan.get('priceSets', [])
    for price_set in price_sets:
        existing_mappings.append({
            'id': price_set.get('id'),
            'name': price_set.get('name'),
            'code': price_set.get('code')
        })
    
    return existing_mappings

def create_price_set_mappings(price_sets: List[Dict]) -> List[Dict]:
    """Create price set mapping data for service plan"""
    mappings = []
    
    for price_set in price_sets:
        mappings.append({
            'id': price_set['id']
        })
    
    return mappings

def update_service_plan_with_price_sets(service_plan: Dict, new_price_sets: List[Dict]) -> Dict[str, Any]:
    """Update service plan data with new price set mappings"""
    # Get existing price set mappings
    existing_mappings = get_existing_price_set_mappings(service_plan)
    existing_ids = {mapping['id'] for mapping in existing_mappings}
    
    # Create new mappings for price sets that aren't already mapped
    new_mappings = []
    for price_set in new_price_sets:
        if price_set['id'] not in existing_ids:
            new_mappings.append({'id': price_set['id']})
    
    # Combine existing and new mappings
    all_mappings = existing_mappings + new_mappings
    
    # Create updated service plan data
    updated_service_plan = {
        "servicePlan": {
            "id": service_plan['id'],
            "name": service_plan['name'],
            "code": service_plan['code'],
            "description": service_plan.get('description', ''),
            "editable": service_plan.get('editable', True),
            "provisionType": service_plan.get('provisionType'),
            "visibility": service_plan.get('visibility', 'public'),
            "active": service_plan.get('active', True),
            "sortOrder": service_plan.get('sortOrder', 100),
            "config": service_plan.get('config', {}),
            "priceSets": [{'id': mapping['id']} for mapping in all_mappings]
        }
    }
    
    return updated_service_plan

def save_integration_summary(summary: Dict, filename: str = 'integration_summary.json') -> None:
    """Save integration summary to JSON file"""
    try:
        with open(filename, 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Integration summary saved to {filename}")
    except Exception as e:
        logger.error(f"Failed to save integration summary: {e}")
        raise

def main():
    """Main function to map price sets to service plans"""
    logger.info("Starting price set to service plan mapping")
    
    # Load environment variables
    env_vars = load_environment()
    
    # Load input data
    service_plans_data = load_json_file('service_plans.json')
    price_sets_data = load_json_file('price_sets.json')
    
    service_plans = service_plans_data.get('servicePlans', [])
    price_sets = price_sets_data.get('priceSets', [])
    
    if not service_plans:
        logger.error("No service plans found in service_plans.json")
        sys.exit(1)
    
    if not price_sets:
        logger.error("No price sets found in price_sets.json")
        sys.exit(1)
    
    # Use the first service plan
    service_plan = service_plans[0]
    service_plan_id = service_plan['id']
    
    logger.info(f"Mapping {len(price_sets)} price sets to service plan: {service_plan['name']} (ID: {service_plan_id})")
    
    # Initialize Morpheus API client
    client = MorpheusAPIClient(env_vars['MORPHEUS_URL'], env_vars['MORPHEUS_TOKEN'])
    
    try:
        # Get current service plan details
        logger.info("Retrieving current service plan details...")
        current_service_plan = client.get_service_plan(service_plan_id)
        
        if not current_service_plan:
            logger.error(f"Could not retrieve service plan {service_plan_id}")
            sys.exit(1)
        
        # Get existing price set mappings
        existing_mappings = get_existing_price_set_mappings(current_service_plan)
        logger.info(f"Found {len(existing_mappings)} existing price set mappings")
        
        # Determine which price sets need to be added
        existing_ids = {mapping['id'] for mapping in existing_mappings}
        new_price_sets = [ps for ps in price_sets if ps['id'] not in existing_ids]
        
        if not new_price_sets:
            logger.info("All price sets are already mapped to the service plan")
            integration_summary = {
                'status': 'completed',
                'service_plan': {
                    'id': service_plan_id,
                    'name': current_service_plan['name'],
                    'total_price_sets': len(existing_mappings)
                },
                'changes_made': False,
                'timestamp': time.time()
            }
        else:
            logger.info(f"Adding {len(new_price_sets)} new price set mappings")
            
            # Update service plan with new price sets
            updated_plan_data = update_service_plan_with_price_sets(current_service_plan, new_price_sets)
            
            # Update service plan via API
            updated_service_plan = client.update_service_plan(service_plan_id, updated_plan_data)
            
            if updated_service_plan:
                logger.info(f"Successfully updated service plan with {len(new_price_sets)} new price set mappings")
                
                # Create integration summary
                integration_summary = {
                    'status': 'completed',
                    'service_plan': {
                        'id': service_plan_id,
                        'name': updated_service_plan['name'],
                        'total_price_sets': len(existing_mappings) + len(new_price_sets)
                    },
                    'changes_made': True,
                    'new_mappings_added': len(new_price_sets),
                    'existing_mappings': len(existing_mappings),
                    'price_sets_mapped': [
                        {
                            'id': ps['id'],
                            'name': ps['name'],
                            'code': ps['code']
                        } for ps in new_price_sets
                    ],
                    'timestamp': time.time()
                }
            else:
                logger.error("Failed to update service plan with price set mappings")
                sys.exit(1)
        
        # Save integration summary
        save_integration_summary(integration_summary)
        
        # Log completion summary
        logger.info("=== INTEGRATION COMPLETED SUCCESSFULLY ===")
        logger.info(f"Service Plan: {integration_summary['service_plan']['name']}")
        logger.info(f"Total Price Sets Mapped: {integration_summary['service_plan']['total_price_sets']}")
        logger.info(f"Changes Made: {integration_summary['changes_made']}")
        
        if integration_summary['changes_made']:
            logger.info(f"New Mappings Added: {integration_summary['new_mappings_added']}")
            logger.info("New Price Set Mappings:")
            for ps in integration_summary['price_sets_mapped']:
                logger.info(f"  - {ps['name']} (ID: {ps['id']})")
        
        logger.info("Google Cloud pricing integration with Morpheus is now complete!")
        logger.info("GCP costing data will now appear in Morpheus for provisioned resources.")
        
        return integration_summary
        
    except Exception as e:
        logger.error(f"Error during price set mapping: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()