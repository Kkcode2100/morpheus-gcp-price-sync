#!/usr/bin/env python3
"""
Script 1: discover_service_plans.py

This script discovers and manages service plans for Google Cloud in HPE Morpheus v8.0.7.
It checks if a Google Cloud service plan exists, and creates one if missing.

Usage:
    python3 discover_service_plans.py

Dependencies:
    pip install requests

Environment Variables:
    MORPHEUS_TOKEN = "9fcc4426-c89a-4430-b6d7-99d5950fc1cc"
    MORPHEUS_URL = "https://xdjmorpheapp01"
    PRICE_PREFIX = "IOH-CP"

Output:
    service_plans.json - Contains discovered/created service plan data
"""

import os
import sys
import json
import logging
import requests
import time
from typing import Optional, Dict, Any, List

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
    
    def get_service_plans(self, params: Dict = None) -> List[Dict]:
        """Get all service plans"""
        response = self.make_request('GET', '/service-plans', params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get('servicePlans', [])
        else:
            logger.error(f"Failed to get service plans: {response.status_code} - {response.text}")
            return []
    
    def get_service_plan_by_code(self, code: str) -> Optional[Dict]:
        """Get service plan by code"""
        params = {'code': code}
        response = self.make_request('GET', '/service-plans', params=params)
        if response.status_code == 200:
            data = response.json()
            plans = data.get('servicePlans', [])
            for plan in plans:
                if plan.get('code') == code:
                    return plan
        return None
    
    def create_service_plan(self, service_plan_data: Dict) -> Optional[Dict]:
        """Create a new service plan"""
        response = self.make_request('POST', '/service-plans', data=service_plan_data)
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return data.get('servicePlan')
            else:
                logger.error(f"Service plan creation failed: {data}")
                return None
        else:
            logger.error(f"Failed to create service plan: {response.status_code} - {response.text}")
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

def create_google_cloud_service_plan(prefix: str) -> Dict[str, Any]:
    """Create service plan data structure for Google Cloud"""
    return {
        "servicePlan": {
            "name": f"{prefix} Google Cloud Service Plan",
            "code": f"{prefix.lower()}-google-cloud-plan",
            "description": "Service plan for Google Cloud pricing integration",
            "editable": True,
            "provisionType": {
                "code": "googlecloud"
            },
            "visibility": "public",
            "active": True,
            "sortOrder": 100,
            "config": {
                "storageSizeType": "GB",
                "memorySizeType": "MB",
                "ranges": {
                    "minStorage": 0,
                    "maxStorage": 0,
                    "minMemory": 0,
                    "maxMemory": 0,
                    "minCores": 0,
                    "maxCores": 0,
                    "minSockets": 0,
                    "maxSockets": 0,
                    "minCoresPerSocket": 0,
                    "maxCoresPerSocket": 0
                }
            }
        }
    }

def save_service_plans(service_plans: List[Dict], filename: str = 'service_plans.json') -> None:
    """Save service plans data to JSON file"""
    try:
        with open(filename, 'w') as f:
            json.dump({
                'servicePlans': service_plans,
                'metadata': {
                    'timestamp': time.time(),
                    'count': len(service_plans)
                }
            }, f, indent=2)
        logger.info(f"Service plans data saved to {filename}")
    except Exception as e:
        logger.error(f"Failed to save service plans data: {e}")
        raise

def main():
    """Main function to discover/create Google Cloud service plans"""
    logger.info("Starting service plan discovery for Google Cloud")
    
    # Load environment variables
    env_vars = load_environment()
    
    # Initialize Morpheus API client
    client = MorpheusAPIClient(env_vars['MORPHEUS_URL'], env_vars['MORPHEUS_TOKEN'])
    
    # Define service plan code
    plan_code = f"{env_vars['PRICE_PREFIX'].lower()}-google-cloud-plan"
    
    try:
        # Check if Google Cloud service plan already exists
        logger.info(f"Checking for existing service plan with code: {plan_code}")
        existing_plan = client.get_service_plan_by_code(plan_code)
        
        if existing_plan:
            logger.info(f"Found existing Google Cloud service plan: {existing_plan['name']} (ID: {existing_plan['id']})")
            service_plans = [existing_plan]
        else:
            logger.info("Google Cloud service plan not found, creating new one")
            
            # Create new service plan
            plan_data = create_google_cloud_service_plan(env_vars['PRICE_PREFIX'])
            created_plan = client.create_service_plan(plan_data)
            
            if created_plan:
                logger.info(f"Successfully created Google Cloud service plan: {created_plan['name']} (ID: {created_plan['id']})")
                service_plans = [created_plan]
            else:
                logger.error("Failed to create Google Cloud service plan")
                sys.exit(1)
        
        # Save service plans data
        save_service_plans(service_plans)
        
        logger.info("Service plan discovery completed successfully")
        return service_plans
        
    except Exception as e:
        logger.error(f"Error during service plan discovery: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()