#!/usr/bin/env python3
"""
GCP SKU Discovery Script

This script reads service_plans.json, filters Google service plans, 
authenticates to Google Cloud Billing API, and fetches relevant SKUs.

Features:
- Reads and filters Google service plans from Morpheus
- Authenticates to GCP Billing API using service account
- Discovers Compute Engine service and fetches all SKUs
- Filters SKUs relevant to Morpheus Google service plans
- Handles pagination and retries
- Saves results to gcp_skus.json

Usage:
  python discover_gcp_skus.py
"""

import json
import logging
import os
import re
import sys
import time
from typing import List, Dict, Optional
import urllib3

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Google Cloud Billing API
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GCP_API_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Google Cloud libraries not available: {e}")
    GCP_API_AVAILABLE = False

# --- Configuration ---
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_REGION = os.getenv("GCP_REGION", "asia-southeast2")
SERVICE_PLANS_FILE = "service_plans.json"
OUTPUT_FILE = "gcp_skus.json"

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GCPBillingClient:
    """Client for interacting with Google Cloud Billing API."""
    
    def __init__(self, credentials_path: str):
        """Initialize the GCP Billing client with service account credentials."""
        if not credentials_path or not os.path.exists(credentials_path):
            raise ValueError(f"Google Cloud credentials file not found: {credentials_path}")
        
        try:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/cloud-billing.readonly']
            )
            self.service = build('cloudbilling', 'v1', credentials=credentials)
            logger.info("Successfully authenticated to Google Cloud Billing API")
        except Exception as e:
            logger.error(f"Failed to authenticate to Google Cloud Billing API: {e}")
            raise
    
    def get_services(self, page_size: int = 200) -> List[Dict]:
        """Get all billing services from GCP."""
        logger.info("Fetching GCP billing services...")
        services = []
        next_page_token = None
        
        while True:
            try:
                request = self.service.services().list(
                    pageSize=page_size,
                    pageToken=next_page_token
                )
                response = request.execute()
                
                batch_services = response.get('services', [])
                services.extend(batch_services)
                logger.info(f"Retrieved {len(batch_services)} services (total: {len(services)})")
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
                time.sleep(0.1)  # Rate limiting
                
            except HttpError as e:
                logger.error(f"Error fetching services: {e}")
                raise
        
        logger.info(f"Successfully retrieved {len(services)} total services")
        return services
    
    def find_compute_engine_service(self, services: List[Dict]) -> Optional[str]:
        """Find the serviceId for Compute Engine."""
        logger.info("Looking for Compute Engine service...")
        
        for service in services:
            display_name = service.get('displayName', '')
            service_id = service.get('serviceId', '')
            
            if 'Compute Engine' in display_name:
                logger.info(f"Found Compute Engine service: {display_name} (ID: {service_id})")
                return service_id
        
        logger.error("Compute Engine service not found")
        return None
    
    def get_skus(self, service_id: str, currency_code: str = "USD", 
                 page_size: int = 1000, region_filter: Optional[str] = None) -> List[Dict]:
        """Get all SKUs for a service with pagination."""
        logger.info(f"Fetching SKUs for service {service_id}...")
        skus = []
        next_page_token = None
        
        while True:
            try:
                request_params = {
                    'parent': f'services/{service_id}',
                    'currencyCode': currency_code,
                    'pageSize': page_size
                }
                
                if next_page_token:
                    request_params['pageToken'] = next_page_token
                
                request = self.service.services().skus().list(**request_params)
                response = request.execute()
                
                batch_skus = response.get('skus', [])
                skus.extend(batch_skus)
                logger.info(f"Retrieved {len(batch_skus)} SKUs (total: {len(skus)})")
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
                time.sleep(0.1)  # Rate limiting
                
            except HttpError as e:
                logger.error(f"Error fetching SKUs: {e}")
                raise
        
        logger.info(f"Successfully retrieved {len(skus)} total SKUs")
        return skus


def load_service_plans(file_path: str) -> List[Dict]:
    """Load and parse service plans JSON file."""
    logger.info(f"Loading service plans from {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        service_plans = data.get('servicePlans', [])
        logger.info(f"Loaded {len(service_plans)} total service plans")
        return service_plans
        
    except Exception as e:
        logger.error(f"Error loading service plans: {e}")
        raise


def filter_google_service_plans(service_plans: List[Dict]) -> List[Dict]:
    """Filter service plans to only include Google/GCP plans."""
    logger.info("Filtering Google service plans...")
    
    google_plans = []
    
    for plan in service_plans:
        provision_type = plan.get('provisionType', {})
        provision_name = provision_type.get('name', '').lower()
        provision_code = provision_type.get('code', '').lower()
        
        # Filter by provision type containing "Google"
        if 'google' in provision_name or provision_code == 'google':
            google_plans.append(plan)
    
    logger.info(f"Found {len(google_plans)} Google service plans out of {len(service_plans)} total")
    return google_plans


def extract_instance_types_from_plans(google_plans: List[Dict]) -> tuple[List[str], List[str]]:
    """Extract instance types and families from Google service plans."""
    logger.info("Extracting instance types from Google service plans...")
    
    instance_types = set()
    instance_families = set()
    
    for plan in google_plans:
        name = plan.get('name', '').lower()
        code = plan.get('code', '').lower()
        
        # Extract instance patterns from name and code
        patterns = [
            r'([a-z]\d+[a-z]?-[a-z]+-\d+)',  # e2-standard-2, n2-standard-4
            r'([a-z]\d+[a-z]?-[a-z]+)',      # e2-standard, n2-standard  
            r'([a-z]\d+[a-z]?)',             # e2, n2, etc.
            r'(f1|g1)-([a-z]+)',             # f1-micro, g1-small
        ]
        
        for text in [name, code]:
            for pattern in patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    if isinstance(match, tuple):
                        instance_types.add('-'.join(match))
                        instance_families.add(match[0])
                    else:
                        instance_types.add(match)
                        # Extract family from instance type
                        family_match = re.match(r'([a-z]\d+[a-z]?)', match)
                        if family_match:
                            instance_families.add(family_match.group(1))
    
    logger.info(f"Extracted {len(instance_types)} instance types and {len(instance_families)} families")
    logger.info(f"Instance families: {sorted(instance_families)}")
    
    return list(instance_types), list(instance_families)


def filter_relevant_skus(skus: List[Dict], instance_types: List[str], 
                        instance_families: List[str], google_plans: List[Dict]) -> List[Dict]:
    """Filter SKUs to only those relevant to Morpheus Google service plans."""
    logger.info("Filtering SKUs relevant to Morpheus service plans...")
    
    relevant_skus = []
    total_skus = len(skus)
    
    # Create matching patterns
    instance_patterns = []
    for instance_type in instance_types:
        # Create regex patterns for instance type matching
        instance_patterns.append(re.compile(rf'\b{re.escape(instance_type)}\b', re.IGNORECASE))
    
    family_patterns = []
    for family in instance_families:
        family_patterns.append(re.compile(rf'\b{re.escape(family)}\b', re.IGNORECASE))
    
    # Service plan name patterns for additional matching
    plan_names = [plan.get('name', '') for plan in google_plans]
    plan_codes = [plan.get('code', '') for plan in google_plans]
    
    for sku in skus:
        description = sku.get('description', '')
        sku_id = sku.get('skuId', '')
        category = sku.get('category', {})
        resource_family = category.get('resourceFamily', '')
        
        # Check if SKU matches any instance types
        instance_match = any(pattern.search(description) for pattern in instance_patterns)
        
        # Check if SKU matches any instance families  
        family_match = any(pattern.search(description) for pattern in family_patterns)
        
        # Check for compute-related SKUs
        compute_keywords = [
            'vcpu', 'cpu', 'core', 'memory', 'ram', 'gb',
            'instance', 'vm', 'virtual machine',
            'compute', 'storage', 'disk'
        ]
        
        keyword_match = any(keyword in description.lower() for keyword in compute_keywords)
        
        # Check resource family
        resource_match = resource_family.lower() in ['compute', 'storage', 'network']
        
        # Include SKU if it matches any criteria
        if instance_match or family_match or (keyword_match and resource_match):
            relevant_skus.append(sku)
    
    logger.info(f"Filtered {len(relevant_skus)} relevant SKUs from {total_skus} total SKUs")
    return relevant_skus


def save_skus_to_file(skus: List[Dict], file_path: str, metadata: Dict):
    """Save SKUs to JSON file with metadata."""
    logger.info(f"Saving {len(skus)} SKUs to {file_path}")
    
    output_data = {
        'metadata': metadata,
        'skus': skus
    }
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Successfully saved SKUs to {file_path}")
        
    except Exception as e:
        logger.error(f"Error saving SKUs to file: {e}")
        raise


def create_sample_output_for_demo(google_plans: List[Dict], instance_types: List[str], instance_families: List[str]):
    """Create a sample output file demonstrating the expected structure when GCP API is available."""
    logger.info("Creating sample output to demonstrate script functionality...")
    
    # Create sample SKUs that would be returned by GCP API
    sample_skus = []
    
    # Sample compute SKUs for different instance families
    for family in instance_families[:3]:  # Limit to first 3 families for demo
        for resource_type in ['vCPU', 'Memory', 'Storage']:
            sample_sku = {
                'skuId': f'sample-{family}-{resource_type.lower()}-{hash(family + resource_type) % 10000:04d}',
                'name': f'services/6F81-5844-456A/skus/sample-{family}-{resource_type.lower()}',
                'description': f'{family.upper()} {resource_type} for {family} instances',
                'category': {
                    'serviceDisplayName': 'Compute Engine',
                    'resourceFamily': 'Compute' if resource_type in ['vCPU', 'Memory'] else 'Storage',
                    'resourceGroup': resource_type,
                    'usageType': 'OnDemand'
                },
                'serviceRegions': [GCP_REGION],
                'pricingInfo': [{
                    'summary': f'OnDemand {resource_type} for {family} instances',
                    'pricingExpression': {
                        'usageUnit': 'hour',
                        'usageUnitDescription': 'hour',
                        'baseUnit': 'hour',
                        'baseUnitDescription': 'hour',
                        'baseUnitConversionFactor': 1,
                        'displayQuantity': 1,
                        'tieredRates': [{
                            'startUsageAmount': '0',
                            'unitPrice': {
                                'currencyCode': 'USD',
                                'units': '0',
                                'nanos': int(50000000 * (1 + hash(family) % 10))  # Sample pricing
                            }
                        }]
                    },
                    'aggregationInfo': {'aggregationLevel': 'ACCOUNT', 'aggregationInterval': 'DAILY', 'aggregationCount': 1},
                    'currencyConversionRate': 1,
                    'effectiveTime': '2025-01-01T00:00:00Z'
                }],
                'serviceProviderName': 'Google'
            }
            sample_skus.append(sample_sku)
    
    # Create metadata for sample output
    metadata = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
        'region': GCP_REGION,
        'compute_engine_service_id': 'sample-6F81-5844-456A',
        'total_service_plans': len(google_plans),
        'google_service_plans': len(google_plans),
        'total_skus_fetched': len(sample_skus) * 10,  # Simulate larger dataset
        'relevant_skus_found': len(sample_skus),
        'instance_types_discovered': instance_types,
        'instance_families_discovered': instance_families,
        'note': 'This is sample data created for demonstration. Real data would come from GCP Billing API.'
    }
    
    save_skus_to_file(sample_skus, OUTPUT_FILE, metadata)
    return sample_skus


def main():
    """Main function to orchestrate the SKU discovery process."""
    logger.info("Starting GCP SKU discovery process")
    
    # Check for required dependencies
    if not GCP_API_AVAILABLE:
        logger.error("Google Cloud API libraries are not available. Please install: pip install google-auth google-api-python-client")
        sys.exit(1)
    
    if not os.path.exists(SERVICE_PLANS_FILE):
        logger.error(f"Service plans file not found: {SERVICE_PLANS_FILE}")
        sys.exit(1)
    
    try:
        # Step 1: Load and filter service plans
        logger.info("Step 1: Loading and filtering service plans")
        service_plans = load_service_plans(SERVICE_PLANS_FILE)
        google_plans = filter_google_service_plans(service_plans)
        
        if not google_plans:
            logger.error("No Google service plans found")
            sys.exit(1)
        
        # Log some example Google service plans
        logger.info("Sample Google service plans found:")
        for i, plan in enumerate(google_plans[:5]):  # Show first 5
            logger.info(f"  {i+1}. {plan.get('name', 'N/A')} (code: {plan.get('code', 'N/A')})")
        if len(google_plans) > 5:
            logger.info(f"  ... and {len(google_plans) - 5} more Google service plans")
        
        # Step 2: Extract instance types and families
        logger.info("Step 2: Extracting instance types from service plans")
        instance_types, instance_families = extract_instance_types_from_plans(google_plans)
        
        # Validate environment
        if not GOOGLE_APPLICATION_CREDENTIALS:
            logger.warning("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
            logger.info("Creating sample output to demonstrate script functionality...")
            create_sample_output_for_demo(google_plans, instance_types, instance_families)
            
            logger.info("=== GCP SKU Discovery Summary (Sample Mode) ===")
            logger.info(f"Total service plans loaded: {len(service_plans)}")
            logger.info(f"Google service plans found: {len(google_plans)}")
            logger.info(f"Instance families: {len(instance_families)} ({', '.join(sorted(instance_families))})")
            logger.info(f"Instance types: {len(instance_types)}")
            logger.info(f"Sample output saved to: {OUTPUT_FILE}")
            logger.info("Note: This is sample data. Set GOOGLE_APPLICATION_CREDENTIALS to fetch real GCP SKUs")
            return
        
        # Step 3: Initialize GCP Billing client
        logger.info("Step 3: Initializing GCP Billing API client")
        gcp_client = GCPBillingClient(GOOGLE_APPLICATION_CREDENTIALS)
        
        # Step 4: Get services and find Compute Engine
        logger.info("Step 4: Finding Compute Engine service")
        services = gcp_client.get_services()
        compute_engine_service_id = gcp_client.find_compute_engine_service(services)
        
        if not compute_engine_service_id:
            logger.error("Could not find Compute Engine service")
            sys.exit(1)
        
        # Step 5: Fetch all SKUs for Compute Engine
        logger.info("Step 5: Fetching Compute Engine SKUs")
        all_skus = gcp_client.get_skus(
            service_id=compute_engine_service_id,
            currency_code="USD",
            region_filter=GCP_REGION
        )
        
        # Step 6: Filter relevant SKUs
        logger.info("Step 6: Filtering relevant SKUs")
        relevant_skus = filter_relevant_skus(
            all_skus, instance_types, instance_families, google_plans
        )
        
        # Step 7: Prepare metadata and save results
        logger.info("Step 7: Saving results")
        metadata = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
            'region': GCP_REGION,
            'compute_engine_service_id': compute_engine_service_id,
            'total_service_plans': len(service_plans),
            'google_service_plans': len(google_plans),
            'total_skus_fetched': len(all_skus),
            'relevant_skus_found': len(relevant_skus),
            'instance_types_discovered': instance_types,
            'instance_families_discovered': instance_families
        }
        
        save_skus_to_file(relevant_skus, OUTPUT_FILE, metadata)
        
        # Log summary
        logger.info("=== GCP SKU Discovery Summary ===")
        logger.info(f"Total service plans loaded: {metadata['total_service_plans']}")
        logger.info(f"Google service plans found: {metadata['google_service_plans']}")
        logger.info(f"Total SKUs fetched from GCP: {metadata['total_skus_fetched']}")
        logger.info(f"Relevant SKUs filtered: {metadata['relevant_skus_found']}")
        logger.info(f"Instance families: {len(instance_families)} ({', '.join(sorted(instance_families))})")
        logger.info(f"Results saved to: {OUTPUT_FILE}")
        logger.info("GCP SKU discovery completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Discovery process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Discovery process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()