#!/usr/bin/env python3
"""
GCP SKU Discovery Script

This script reads service_plans.json, filters Google service plans, 
authenticates to Google Cloud Billing API, and fetches relevant SKUs
aligned to Morpheus service plans.

Features:
- Loads service_plans.json and filters plans with provisionType.name containing "Google"
- Extracts plan names and resource types (machine, storage, kubernetes)
- Maintains mapping table for Compute Engine, Persistent Disk, and Kubernetes Engine SKUs
- Authenticates to GCP Billing API using GOOGLE_APPLICATION_CREDENTIALS
- Handles region filtering (GLOBAL or specified region)
- Implements exponential backoff retry logic (max 3 tries per API call)
- Comprehensive logging and error handling
- Outputs gcp_skus.json in original API schema

Usage:
  python discover_gcp_skus.py --region us-central1
  python discover_gcp_skus.py  # defaults to asia-southeast2
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from typing import List, Dict, Optional, Set, Tuple
import urllib3
import subprocess
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError as e:
    REQUESTS_AVAILABLE = False
    print(f"Requests library not available: {e}")
    print("Please install: pip install requests")

# Google Cloud Billing API
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GCP_API_AVAILABLE = True
except ImportError as e:
    GCP_API_AVAILABLE = False
    print(f"Google Cloud libraries not available: {e}")
    print("Please install: pip install google-auth google-api-python-client")

# --- Configuration ---
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SERVICE_PLANS_FILE = "service_plans.json"
OUTPUT_FILE = "gcp_skus.json"
OUTPUT_SERVICES_FILE = "gcp_services.json"

# Service mapping for different GCP services
SERVICE_MAPPING = {
    'compute_engine': {
        'service_names': ['Compute Engine'],
        'machine_families': ['n1', 'n2', 'e2', 'c2', 't2d', 'm1', 'm2', 'm3', 'a2', 'c3', 'c3d'],
        'resource_types': ['machine', 'vm', 'instance']
    },
    'persistent_disk': {
        'service_names': ['Compute Engine'],  # PD SKUs are under Compute Engine
        'disk_types': ['pd-ssd', 'pd-balanced', 'pd-standard', 'pd-extreme'],
        'resource_types': ['storage', 'disk']
    },
    'kubernetes_engine': {
        'service_names': ['Kubernetes Engine'],
        'cluster_types': ['gke', 'autopilot'],
        'resource_types': ['kubernetes', 'container']
    }
}

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GCPBillingClient:
    """Client for interacting with Google Cloud Billing API with retry logic."""
    
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
    
    def _retry_api_call(self, api_call_func, max_retries: int = 3):
        """Execute API call with exponential backoff retry logic."""
        for attempt in range(max_retries):
            try:
                return api_call_func()
            except HttpError as e:
                if attempt == max_retries - 1:
                    logger.error(f"API call failed after {max_retries} attempts: {e}")
                    raise
                
                # Exponential backoff: 1, 2, 4 seconds
                wait_time = 2 ** attempt
                logger.warning(f"API call failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            except Exception as e:
                logger.error(f"Unexpected error in API call: {e}")
                raise
    
    def get_services(self, page_size: int = 200) -> List[Dict]:
        """Get all billing services from GCP with pagination and retry logic."""
        logger.info("Fetching GCP billing services...")
        services = []
        next_page_token = None
        
        while True:
            def api_call():
                return self.service.services().list(
                    pageSize=page_size,
                    pageToken=next_page_token
                ).execute()
            
            response = self._retry_api_call(api_call)
            
            batch_services = response.get('services', [])
            services.extend(batch_services)
            logger.info(f"Retrieved {len(batch_services)} services (total: {len(services)})")
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
                
            time.sleep(0.1)  # Rate limiting
        
        logger.info(f"Successfully retrieved {len(services)} total services")
        return services
    
    def find_services_by_names(self, services: List[Dict], service_names: List[str]) -> Dict[str, str]:
        """Find service IDs for given service names."""
        logger.info(f"Looking for services: {service_names}")
        found_services = {}
        
        for service in services:
            display_name = service.get('displayName', '')
            service_id = service.get('serviceId', '')
            
            for target_name in service_names:
                if target_name in display_name:
                    found_services[target_name] = service_id
                    logger.info(f"Found service: {display_name} (ID: {service_id})")
        
        # Log missing services
        missing_services = set(service_names) - set(found_services.keys())
        if missing_services:
            logger.warning(f"Services not found: {missing_services}")
        
        return found_services
    
    def get_skus(self, service_id: str, currency_code: str = "USD", 
                 page_size: int = 1000, region_filter: Optional[str] = None) -> List[Dict]:
        """Get all SKUs for a service with pagination, retry logic, and region filtering."""
        logger.info(f"Fetching SKUs for service {service_id} (region filter: {region_filter})")
        skus = []
        filtered_skus = []
        next_page_token = None
        
        while True:
            def api_call():
                request_params = {
                    'parent': f'services/{service_id}',
                    'currencyCode': currency_code,
                    'pageSize': page_size
                }
                
                if next_page_token:
                    request_params['pageToken'] = next_page_token
                
                return self.service.services().skus().list(**request_params).execute()
            
            response = self._retry_api_call(api_call)
            
            batch_skus = response.get('skus', [])
            skus.extend(batch_skus)
            
            # Apply region filtering
            for sku in batch_skus:
                geo_taxonomy = sku.get('geoTaxonomy', {})
                geo_type = geo_taxonomy.get('type', '')
                regions = geo_taxonomy.get('regions', [])
                
                # Include if GLOBAL or region matches
                if geo_type == 'GLOBAL' or (region_filter and region_filter in regions):
                    filtered_skus.append(sku)
            
            logger.info(f"Retrieved {len(batch_skus)} SKUs, {len(filtered_skus)} after region filtering (total: {len(skus)} fetched, {len(filtered_skus)} filtered)")
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
                
            time.sleep(0.1)  # Rate limiting
        
        logger.info(f"Successfully retrieved {len(skus)} total SKUs, {len(filtered_skus)} after region filtering")
        return filtered_skus


# --- REST Fallback using gcloud access token ---
API_BASE_URL = "https://cloudbilling.googleapis.com/v1"


def obtain_access_token_from_gcloud() -> Optional[str]:
    """Obtain an OAuth2 access token using gcloud. Returns None if gcloud is not available."""
    try:
        completed = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )
        if completed.returncode == 0:
            token = completed.stdout.strip()
            if token:
                return token
        logger.error(f"Failed to get access token from gcloud (exit {completed.returncode}): {completed.stderr.strip()}")
        return None
    except FileNotFoundError:
        logger.error("gcloud CLI not found. Install Google Cloud SDK or install google-auth and google-api-python-client Python packages.")
        return None


class GCPBillingRestClient:
    """REST client for Cloud Billing Catalog using requests and optional OAuth token."""

    def __init__(self, access_token: Optional[str]):
        self.access_token = access_token
        if access_token:
            logger.info("Initialized REST client with gcloud access token")
        else:
            logger.info("Initialized REST client without token; attempting unauthenticated public catalog access")

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers

    def _retry_request(self, method: str, url: str, params: Optional[Dict] = None, max_retries: int = 3) -> Dict:
        last_error: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                response = requests.request(method, url, headers=self._headers(), params=params, timeout=60)
                if response.status_code == 401 and self.access_token is None:
                    # Unauthorized without token; propagate error
                    response.raise_for_status()
                if response.status_code >= 500:
                    raise Exception(f"Server error {response.status_code}: {response.text[:200]}")
                response.raise_for_status()
                return response.json()
            except Exception as e:
                last_error = e
                if attempt == max_retries - 1:
                    logger.error(f"Request failed after {max_retries} attempts: {e}")
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
        raise last_error  # type: ignore

    def get_services(self, page_size: int = 200) -> List[Dict]:
        logger.info("Fetching GCP billing services via REST...")
        services: List[Dict] = []
        next_page_token: Optional[str] = None
        
        while True:
            params: Dict[str, object] = {"pageSize": page_size}
            if next_page_token:
                params["pageToken"] = next_page_token
            url = f"{API_BASE_URL}/services"
            data = self._retry_request("GET", url, params=params)
            batch = data.get("services", [])
            services.extend(batch)
            logger.info(f"Retrieved {len(batch)} services (total: {len(services)})")
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            time.sleep(0.1)
        logger.info(f"Successfully retrieved {len(services)} total services")
        return services

    def find_services_by_names(self, services: List[Dict], service_names: List[str]) -> Dict[str, str]:
        # Reuse same logic as SDK client
        found: Dict[str, str] = {}
        for service in services:
            display_name = service.get('displayName', '')
            service_id = service.get('name', '').split('/')[-1] or service.get('serviceId', '')
            for target in service_names:
                if target in display_name:
                    found[target] = service_id
                    logger.info(f"Found service: {display_name} (ID: {service_id})")
        missing = set(service_names) - set(found.keys())
        if missing:
            logger.warning(f"Services not found: {missing}")
        return found

    def get_skus(self, service_id: str, currency_code: str = "USD", page_size: int = 1000, region_filter: Optional[str] = None) -> List[Dict]:
        logger.info(f"Fetching SKUs for service {service_id} via REST (region filter: {region_filter})")
        skus: List[Dict] = []
        filtered: List[Dict] = []
        next_page_token: Optional[str] = None
        
        while True:
            params: Dict[str, object] = {
                "currencyCode": currency_code,
                "pageSize": page_size,
            }
            if next_page_token:
                params["pageToken"] = next_page_token
            url = f"{API_BASE_URL}/services/{service_id}/skus"
            data = self._retry_request("GET", url, params=params)
            batch = data.get("skus", [])
            skus.extend(batch)
            for sku in batch:
                geo_taxonomy = sku.get('geoTaxonomy', {})
                geo_type = geo_taxonomy.get('type', '')
                regions = geo_taxonomy.get('regions', [])
                if geo_type == 'GLOBAL' or (region_filter and region_filter in regions):
                    filtered.append(sku)
            logger.info(f"Retrieved {len(batch)} SKUs, {len(filtered)} after region filtering (total: {len(skus)} fetched, {len(filtered)} filtered)")
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            time.sleep(0.1)
        logger.info(f"Successfully retrieved {len(skus)} total SKUs, {len(filtered)} after region filtering")
        return filtered


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
        provision_name = provision_type.get('name', '')
        
        # Filter by provision type containing "Google"
        if 'Google' in provision_name:
            google_plans.append(plan)
    
    logger.info(f"Found {len(google_plans)} Google service plans out of {len(service_plans)} total")
    return google_plans


def extract_plan_details(google_plans: List[Dict]) -> Dict[str, List[Dict]]:
    """Extract plan names and classify by resource type (machine, storage, kubernetes)."""
    logger.info("Extracting plan details and classifying by resource type...")
    
    classified_plans = {
        'machine': [],
        'storage': [], 
        'kubernetes': []
    }
    
    for plan in google_plans:
        name = plan.get('name', '').lower()
        code = plan.get('code', '').lower()
        
        plan_info = {
            'id': plan.get('id'),
            'name': plan.get('name', ''),
            'code': plan.get('code', ''),
            'description': plan.get('description', ''),
            'provisionType': plan.get('provisionType', {})
        }
        
        # Classify by resource type based on name and code
        if any(keyword in name or keyword in code for keyword in ['vm', 'instance', 'compute', 'machine']):
            classified_plans['machine'].append(plan_info)
        elif any(keyword in name or keyword in code for keyword in ['disk', 'storage', 'volume']):
            classified_plans['storage'].append(plan_info)
        elif any(keyword in name or keyword in code for keyword in ['gke', 'kubernetes', 'container', 'k8s']):
            classified_plans['kubernetes'].append(plan_info)
        else:
            # Default to machine for unclassified Google plans
            classified_plans['machine'].append(plan_info)
    
    for resource_type, plans in classified_plans.items():
        logger.info(f"Classified {len(plans)} plans as {resource_type} type")
        if plans:
            logger.info(f"  Sample {resource_type} plans: {[p['name'] for p in plans[:3]]}")
    
    return classified_plans


def create_sku_mapping_patterns(classified_plans: Dict[str, List[Dict]]) -> Dict[str, List[str]]:
    """Create regex patterns for matching SKUs to service plans."""
    logger.info("Creating SKU mapping patterns...")
    
    patterns = {
        'compute_engine': [],
        'persistent_disk': [],
        'kubernetes_engine': []
    }
    
    # Extract machine family patterns from machine plans
    for plan in classified_plans['machine']:
        name = plan['name'].lower()
        code = plan['code'].lower()
        
        # Extract machine family patterns (n1, n2, e2, etc.)
        for family in SERVICE_MAPPING['compute_engine']['machine_families']:
            if family in name or family in code:
                patterns['compute_engine'].append(family)
        
        # Extract specific instance type patterns
        instance_patterns = re.findall(r'([a-z]\d+[a-z]?)', name + ' ' + code)
        patterns['compute_engine'].extend(instance_patterns)
    
    # Extract disk type patterns from storage plans
    for plan in classified_plans['storage']:
        name = plan['name'].lower()
        code = plan['code'].lower()
        
        for disk_type in SERVICE_MAPPING['persistent_disk']['disk_types']:
            if disk_type in name or disk_type in code:
                patterns['persistent_disk'].append(disk_type)
    
    # Extract kubernetes patterns from kubernetes plans
    for plan in classified_plans['kubernetes']:
        name = plan['name'].lower()
        code = plan['code'].lower()
        
        for cluster_type in SERVICE_MAPPING['kubernetes_engine']['cluster_types']:
            if cluster_type in name or cluster_type in code:
                patterns['kubernetes_engine'].append(cluster_type)
    
    # Add default patterns if none found
    if not patterns['compute_engine']:
        patterns['compute_engine'] = SERVICE_MAPPING['compute_engine']['machine_families'][:5]  # Use first 5 families
    
    if not patterns['persistent_disk']:
        patterns['persistent_disk'] = SERVICE_MAPPING['persistent_disk']['disk_types']
    
    if not patterns['kubernetes_engine']:
        patterns['kubernetes_engine'] = SERVICE_MAPPING['kubernetes_engine']['cluster_types']
    
    # Remove duplicates
    for service, pattern_list in patterns.items():
        patterns[service] = list(set(pattern_list))
        logger.info(f"{service} patterns: {patterns[service]}")
    
    return patterns


def filter_relevant_skus(skus: List[Dict], sku_patterns: Dict[str, List[str]], 
                        classified_plans: Dict[str, List[Dict]]) -> Tuple[List[Dict], Dict[str, int]]:
    """Filter SKUs to only those relevant to Morpheus service plans with fuzzy matching."""
    logger.info("Filtering SKUs relevant to Morpheus service plans...")
    
    relevant_skus = []
    service_sku_counts = {'compute_engine': 0, 'persistent_disk': 0, 'kubernetes_engine': 0, 'other': 0}
    
    for sku in skus:
        description = sku.get('description', '').lower()
        sku_id = sku.get('skuId', '')
        category = sku.get('category', {})
        resource_family = category.get('resourceFamily', '').lower()
        service_display_name = category.get('serviceDisplayName', '').lower()
        
        sku_matched = False
        
        # Check Compute Engine patterns
        for pattern in sku_patterns['compute_engine']:
            if pattern in description:
                relevant_skus.append(sku)
                service_sku_counts['compute_engine'] += 1
                sku_matched = True
                break
        
        if not sku_matched:
            # Check Persistent Disk patterns
            for pattern in sku_patterns['persistent_disk']:
                if pattern in description or 'disk' in description:
                    relevant_skus.append(sku)
                    service_sku_counts['persistent_disk'] += 1
                    sku_matched = True
                    break
        
        if not sku_matched:
            # Check Kubernetes Engine patterns
            for pattern in sku_patterns['kubernetes_engine']:
                if pattern in description or 'kubernetes' in service_display_name:
                    relevant_skus.append(sku)
                    service_sku_counts['kubernetes_engine'] += 1
                    sku_matched = True
                    break
        
        if not sku_matched:
            # Include general compute/storage SKUs that might be relevant
            compute_keywords = ['vcpu', 'cpu', 'core', 'memory', 'ram', 'gb']
            storage_keywords = ['storage', 'disk', 'volume']
            
            if (any(keyword in description for keyword in compute_keywords) and 
                resource_family in ['compute', 'storage']) or \
               (any(keyword in description for keyword in storage_keywords) and 
                'compute' in service_display_name):
                relevant_skus.append(sku)
                service_sku_counts['other'] += 1
    
    logger.info(f"Filtered {len(relevant_skus)} relevant SKUs from {len(skus)} total SKUs")
    logger.info(f"SKU breakdown: Compute Engine: {service_sku_counts['compute_engine']}, "
                f"Persistent Disk: {service_sku_counts['persistent_disk']}, "
                f"Kubernetes Engine: {service_sku_counts['kubernetes_engine']}, "
                f"Other: {service_sku_counts['other']}")
    
    return relevant_skus, service_sku_counts


def identify_plans_without_skus(classified_plans: Dict[str, List[Dict]], 
                                service_sku_counts: Dict[str, int]) -> List[str]:
    """Identify plans that have no matching SKUs."""
    plans_without_skus = []
    
    # Check if each plan category has matching SKUs
    if classified_plans['machine'] and service_sku_counts['compute_engine'] == 0:
        plans_without_skus.extend([plan['name'] for plan in classified_plans['machine']])
    
    if classified_plans['storage'] and service_sku_counts['persistent_disk'] == 0:
        plans_without_skus.extend([plan['name'] for plan in classified_plans['storage']])
    
    if classified_plans['kubernetes'] and service_sku_counts['kubernetes_engine'] == 0:
        plans_without_skus.extend([plan['name'] for plan in classified_plans['kubernetes']])
    
    if plans_without_skus:
        logger.warning(f"Plans with no matching SKUs: {plans_without_skus}")
    else:
        logger.info("All plan categories have matching SKUs")
    
    return plans_without_skus


def save_skus_to_file(skus: List[Dict], file_path: str, metadata: Dict):
    """Save SKUs to JSON file in original API schema format."""
    logger.info(f"Saving {len(skus)} SKUs to {file_path}")
    
    # Use original GCP API schema format
    output_data = {
        'skus': skus,
        'metadata': metadata
    }
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Successfully saved SKUs to {file_path}")
        
    except Exception as e:
        logger.error(f"Error saving SKUs to file: {e}")
        raise


def save_services_to_file(services: List[Dict], file_path: str):
    """Save GCP billing services to JSON file for human review."""
    logger.info(f"Saving {len(services)} services to {file_path}")
    try:
        minimal_services = []
        for s in services:
            display_name = s.get('displayName') or s.get('name', '')
            name = s.get('name') or ''
            service_id = s.get('serviceId') or name.split('/')[-1]
            minimal_services.append({
                'displayName': display_name,
                'name': name,
                'serviceId': service_id
            })
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump({
                'services': minimal_services,
                'metadata': {
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                    'note': 'Dumped from GCP Billing Catalog prior to SKU/pricing fetch'
                }
            }, f, indent=2, ensure_ascii=False)
        logger.info(f"Successfully saved services to {file_path}")
    except Exception as e:
        logger.error(f"Error saving services to file: {e}")
        raise


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Discover GCP SKUs aligned to Morpheus service plans')
    parser.add_argument('--region', default='asia-southeast2', 
                        help='GCP region to filter SKUs (default: asia-southeast2)')
    parser.add_argument('--dump-services-only', action='store_true',
                        help='Only fetch and dump the list of GCP billing services, then exit')
    return parser.parse_args()


def main():
    """Main function to orchestrate the SKU discovery process."""
    logger.info("Starting GCP SKU discovery process")
    
    # Parse command line arguments
    args = parse_arguments()
    region = args.region
    dump_services_only = args.dump_services_only
    logger.info(f"Configured region: {region}")
    if dump_services_only:
        logger.info("Running in dump-services-only mode")
    
    # Google Cloud API libraries missing will trigger REST fallback via gcloud if available
    
    if not os.path.exists(SERVICE_PLANS_FILE):
        logger.error(f"Service plans file not found: {SERVICE_PLANS_FILE}")
        sys.exit(1)
    
    if GCP_API_AVAILABLE and not GOOGLE_APPLICATION_CREDENTIALS:
        logger.error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
        logger.error("Please set the path to your Google Cloud service account JSON file or rely on gcloud fallback by installing the CLI and running 'gcloud auth activate-service-account'")
        sys.exit(1)
    
    try:
        # Step 1: Load and filter service plans
        logger.info("Step 1: Loading and filtering service plans")
        service_plans = load_service_plans(SERVICE_PLANS_FILE)
        google_plans = filter_google_service_plans(service_plans)
        
        if not google_plans:
            logger.error("No Google service plans found")
            sys.exit(1)
        
        # Log sample Google service plans
        logger.info("Sample Google service plans found:")
        for i, plan in enumerate(google_plans[:5]):
            logger.info(f"  {i+1}. {plan.get('name', 'N/A')} (code: {plan.get('code', 'N/A')})")
        if len(google_plans) > 5:
            logger.info(f"  ... and {len(google_plans) - 5} more Google service plans")
        
        # Step 2: Extract and classify plan details
        logger.info("Step 2: Extracting and classifying plan details")
        classified_plans = extract_plan_details(google_plans)
        
        # Step 3: Create SKU mapping patterns
        logger.info("Step 3: Creating SKU mapping patterns")
        sku_patterns = create_sku_mapping_patterns(classified_plans)
        
        # Step 4: Initialize GCP Billing client
        logger.info("Step 4: Initializing GCP Billing API client")
        if GCP_API_AVAILABLE:
            gcp_client = GCPBillingClient(GOOGLE_APPLICATION_CREDENTIALS)
        else:
            if not REQUESTS_AVAILABLE:
                logger.error("requests library not available for REST fallback. Please install: pip install requests")
                sys.exit(1)
            logger.info("Falling back to REST client using gcloud access token")
            access_token = obtain_access_token_from_gcloud()
            if not access_token:
                logger.error("gcloud access token unavailable. Install Google Cloud SDK and run 'gcloud auth activate-service-account' or install google-auth libraries.")
                sys.exit(1)
            gcp_client = GCPBillingRestClient(access_token)
        
        # Step 5: Get services and find relevant service IDs
        logger.info("Step 5: Finding relevant GCP services")
        services = gcp_client.get_services()
        
        # Save the full services list for human review
        save_services_to_file(services, OUTPUT_SERVICES_FILE)
        if dump_services_only:
            logger.info("Services dumped successfully. Exiting as requested by --dump-services-only")
            return
        
        # Get service IDs for all relevant services
        all_service_names = set()
        for service_config in SERVICE_MAPPING.values():
            all_service_names.update(service_config['service_names'])
        
        found_services = gcp_client.find_services_by_names(services, list(all_service_names))
        
        if not found_services:
            logger.error("Could not find any relevant GCP services")
            sys.exit(1)
        
        # Step 6: Fetch SKUs from all relevant services
        logger.info("Step 6: Fetching SKUs from relevant services")
        all_skus = []
        service_fetch_counts = {}
        
        for service_name, service_id in found_services.items():
            logger.info(f"Fetching SKUs for {service_name} (ID: {service_id})")
            service_skus = gcp_client.get_skus(
                service_id=service_id,
                currency_code="USD",
                region_filter=region
            )
            all_skus.extend(service_skus)
            service_fetch_counts[service_name] = len(service_skus)
            logger.info(f"Fetched {len(service_skus)} SKUs from {service_name}")
        
        logger.info(f"Total SKUs fetched across all services: {len(all_skus)}")
        
        # Step 7: Filter relevant SKUs
        logger.info("Step 7: Filtering relevant SKUs")
        relevant_skus, service_sku_counts = filter_relevant_skus(
            all_skus, sku_patterns, classified_plans
        )
        
        # Step 8: Identify plans without matching SKUs
        logger.info("Step 8: Identifying plans without matching SKUs")
        plans_without_skus = identify_plans_without_skus(classified_plans, service_sku_counts)
        
        # Step 9: Prepare metadata and save results
        logger.info("Step 9: Saving results")
        metadata = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
            'region': region,
            'services_found': found_services,
            'total_service_plans': len(service_plans),
            'google_service_plans': len(google_plans),
            'classified_plans': {k: len(v) for k, v in classified_plans.items()},
            'total_skus_fetched': len(all_skus),
            'skus_fetched_per_service': service_fetch_counts,
            'relevant_skus_found': len(relevant_skus),
            'skus_by_service_type': service_sku_counts,
            'sku_patterns_used': sku_patterns,
            'plans_without_matching_skus': plans_without_skus
        }
        
        save_skus_to_file(relevant_skus, OUTPUT_FILE, metadata)
        
        # Log summary
        logger.info("=== GCP SKU Discovery Summary ===")
        logger.info(f"Configured region: {region}")
        logger.info(f"Total service plans loaded: {metadata['total_service_plans']}")
        logger.info(f"Google service plans found: {metadata['google_service_plans']}")
        logger.info(f"Services discovered: {list(found_services.keys())}")
        logger.info(f"Total SKUs fetched from GCP: {metadata['total_skus_fetched']}")
        for service_name, count in service_fetch_counts.items():
            logger.info(f"  {service_name}: {count} SKUs")
        logger.info(f"Relevant SKUs after filtering: {metadata['relevant_skus_found']}")
        logger.info(f"SKU breakdown by type: {service_sku_counts}")
        if plans_without_skus:
            logger.warning(f"Plans with no matching SKUs ({len(plans_without_skus)}): {plans_without_skus}")
        logger.info(f"Results saved to: {OUTPUT_FILE}")
        logger.info(f"Services saved to: {OUTPUT_SERVICES_FILE}")
        logger.info("GCP SKU discovery completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Discovery process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Discovery process failed: {e}")
        raise


if __name__ == "__main__":
    main()