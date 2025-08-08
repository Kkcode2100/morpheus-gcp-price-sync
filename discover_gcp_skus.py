#!/usr/bin/env python3
"""
Script 2: discover_gcp_skus.py

This script authenticates to Google Cloud Billing API and retrieves SKUs for Google Compute Engine
service for a specific region.

Usage:
    python3 discover_gcp_skus.py

Dependencies:
    pip install google-auth google-api-python-client requests

Environment Variables:
    GOOGLE_APPLICATION_CREDENTIALS = "/root/gcloud_offline_rpms/auth.json"
    GCP_REGION = "asia-southeast2"

Output:
    gcp_skus.json - Contains all GCP SKUs for the specified region
"""

import os
import sys
import json
import logging
import time
from typing import Dict, List, Any, Optional

# Google Cloud imports
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GCPBillingClient:
    """Client for interacting with Google Cloud Billing API"""
    
    def __init__(self, credentials_path: str):
        self.credentials_path = credentials_path
        self.service = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate and build the service client"""
        try:
            # Set credentials path
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path
            
            # Get default credentials
            credentials, project = google.auth.default(
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            
            # Build the service
            self.service = build('cloudbilling', 'v1', credentials=credentials)
            logger.info("Successfully authenticated with Google Cloud Billing API")
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Google Cloud: {e}")
            raise
    
    def list_services(self) -> List[Dict]:
        """List all available billing services"""
        try:
            request = self.service.services().list()
            response = request.execute()
            return response.get('services', [])
        except HttpError as e:
            logger.error(f"Error listing services: {e}")
            return []
    
    def find_compute_engine_service(self) -> Optional[str]:
        """Find the Google Compute Engine service ID"""
        services = self.list_services()
        
        for service in services:
            display_name = service.get('displayName', '').lower()
            if 'compute engine' in display_name:
                service_id = service.get('serviceId')
                logger.info(f"Found Compute Engine service: {service['displayName']} (ID: {service_id})")
                return service_id
        
        logger.error("Google Compute Engine service not found")
        return None
    
    def list_skus(self, service_id: str, region: str = None) -> List[Dict]:
        """List SKUs for a given service and optional region"""
        all_skus = []
        page_token = None
        
        try:
            while True:
                request = self.service.services().skus().list(
                    parent=f'services/{service_id}',
                    pageSize=5000,
                    pageToken=page_token
                )
                
                response = request.execute()
                skus = response.get('skus', [])
                
                # Filter by region if specified
                if region:
                    filtered_skus = []
                    for sku in skus:
                        service_regions = sku.get('serviceRegions', [])
                        geo_taxonomy = sku.get('geoTaxonomy', {})
                        
                        # Check if SKU is available in the specified region
                        if (region in service_regions or 
                            not service_regions or  # Global SKUs
                            geo_taxonomy.get('type') == 'GLOBAL'):
                            filtered_skus.append(sku)
                    
                    all_skus.extend(filtered_skus)
                    logger.info(f"Retrieved {len(filtered_skus)} SKUs for region {region} (page size: {len(skus)})")
                else:
                    all_skus.extend(skus)
                    logger.info(f"Retrieved {len(skus)} SKUs (total: {len(all_skus)})")
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                    
        except HttpError as e:
            logger.error(f"Error listing SKUs: {e}")
            return []
        
        logger.info(f"Total SKUs retrieved: {len(all_skus)}")
        return all_skus
    
    def get_sku_details(self, sku: Dict) -> Dict[str, Any]:
        """Extract relevant details from a SKU"""
        return {
            'skuId': sku.get('skuId'),
            'name': sku.get('name'),
            'description': sku.get('description'),
            'category': sku.get('category', {}),
            'serviceRegions': sku.get('serviceRegions', []),
            'pricingInfo': sku.get('pricingInfo', []),
            'serviceProviderName': sku.get('serviceProviderName'),
            'geoTaxonomy': sku.get('geoTaxonomy', {}),
            'resourceFamily': sku.get('category', {}).get('resourceFamily'),
            'resourceGroup': sku.get('category', {}).get('resourceGroup'),
            'usageType': sku.get('category', {}).get('usageType')
        }

def load_environment() -> Dict[str, str]:
    """Load and validate environment variables"""
    required_vars = ['GOOGLE_APPLICATION_CREDENTIALS', 'GCP_REGION']
    env_vars = {}
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            logger.error(f"Missing required environment variable: {var}")
            sys.exit(1)
        env_vars[var] = value
    
    # Validate credentials file exists
    if not os.path.exists(env_vars['GOOGLE_APPLICATION_CREDENTIALS']):
        logger.error(f"Credentials file not found: {env_vars['GOOGLE_APPLICATION_CREDENTIALS']}")
        sys.exit(1)
    
    logger.info("Environment variables loaded and validated successfully")
    return env_vars

def save_gcp_skus(skus: List[Dict], filename: str = 'gcp_skus.json') -> None:
    """Save GCP SKUs data to JSON file"""
    try:
        with open(filename, 'w') as f:
            json.dump({
                'skus': skus,
                'metadata': {
                    'timestamp': time.time(),
                    'count': len(skus),
                    'source': 'Google Cloud Billing API'
                }
            }, f, indent=2)
        logger.info(f"GCP SKUs data saved to {filename}")
    except Exception as e:
        logger.error(f"Failed to save GCP SKUs data: {e}")
        raise

def categorize_skus(skus: List[Dict]) -> Dict[str, List[Dict]]:
    """Categorize SKUs by resource family and type"""
    categories = {}
    
    for sku in skus:
        category = sku.get('category', {})
        resource_family = category.get('resourceFamily', 'Unknown')
        resource_group = category.get('resourceGroup', 'Unknown')
        usage_type = category.get('usageType', 'Unknown')
        
        key = f"{resource_family}/{resource_group}/{usage_type}"
        
        if key not in categories:
            categories[key] = []
        
        categories[key].append(sku)
    
    # Log category summary
    logger.info("SKU categorization summary:")
    for category, sku_list in sorted(categories.items()):
        logger.info(f"  {category}: {len(sku_list)} SKUs")
    
    return categories

def main():
    """Main function to discover GCP SKUs"""
    logger.info("Starting GCP SKU discovery")
    
    # Load environment variables
    env_vars = load_environment()
    
    try:
        # Initialize GCP Billing client
        client = GCPBillingClient(env_vars['GOOGLE_APPLICATION_CREDENTIALS'])
        
        # Find Google Compute Engine service
        logger.info("Finding Google Compute Engine service...")
        compute_service_id = client.find_compute_engine_service()
        
        if not compute_service_id:
            logger.error("Could not find Google Compute Engine service")
            sys.exit(1)
        
        # List SKUs for the specified region
        region = env_vars['GCP_REGION']
        logger.info(f"Retrieving SKUs for region: {region}")
        
        raw_skus = client.list_skus(compute_service_id, region)
        
        if not raw_skus:
            logger.error("No SKUs found for the specified service and region")
            sys.exit(1)
        
        # Process and extract relevant SKU details
        processed_skus = []
        for sku in raw_skus:
            sku_details = client.get_sku_details(sku)
            processed_skus.append(sku_details)
        
        logger.info(f"Processed {len(processed_skus)} SKUs")
        
        # Categorize SKUs for analysis
        categorize_skus(processed_skus)
        
        # Save SKUs data
        save_gcp_skus(processed_skus)
        
        logger.info("GCP SKU discovery completed successfully")
        return processed_skus
        
    except Exception as e:
        logger.error(f"Error during GCP SKU discovery: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()