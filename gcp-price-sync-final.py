#!/usr/bin/env python3
"""
GCP Price Sync - Final Unified Script

This consolidated script uses the comprehensive SKU catalog produced by gcp-sku-downloader.py
to discover existing GCP service plans and create comprehensive prices and price sets in Morpheus.
Optionally, it can also create service plans based on Compute Engine SKUs.

Features:
- Uses downloaded SKU catalog (full catalog JSON from gcp-sku-downloader.py)
- Discovers existing GCP service plans in Morpheus
- Creates comprehensive Prices from SKUs (with units and costs)
- Creates Price Sets by category and a comprehensive set
- Optionally creates Service Plans based on compute instance families/types
- Dry-run and validation modes with concise summaries

Usage:
  python gcp-price-sync-final.py --sku-catalog gcp_skus_YYYYMMDD_HHMMSS.json --dry-run
  python gcp-price-sync-final.py --sku-catalog gcp_skus_YYYYMMDD_HHMMSS.json --create-service-plans
  python gcp-price-sync-final.py --sku-catalog gcp_skus_YYYYMMDD_HHMMSS.json --validate-only
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Union, Optional, Tuple, List, Dict

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuration ---
MORPHEUS_URL = os.getenv("MORPHEUS_URL", "https://localhost")
MORPHEUS_TOKEN = os.getenv("MORPHEUS_TOKEN", "9fcc4426-c89a-4430-b6d7-99d5950fc1cc")
GCP_REGION = os.getenv("GCP_REGION", "asia-southeast2")
PRICE_PREFIX = os.getenv("PRICE_PREFIX", "IOH-CP")

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MorpheusApiClient:
    """Client for interacting with the Morpheus API."""

    def __init__(self, base_url: str, api_token: str, max_retries: int = 5, backoff_factor: float = 2,
                 status_forcelist=(429, 500, 502, 503, 504)):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"BEARER {api_token}",
            "Content-Type": "application/json",
        }
        self.session = requests.Session()
        retry_strategy = Retry(total=max_retries, backoff_factor=backoff_factor,
                               status_forcelist=list(status_forcelist))
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _request(self, method: str, endpoint: str, payload=None, params=None):
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(method, url, json=payload, headers=self.headers,
                                             params=params, verify=False)
            response.raise_for_status()
            return response.json() if response.content else None
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection Error: Cannot connect to Morpheus server at {self.base_url}")
            logger.error("Please verify Morpheus availability, URL correctness, and network connectivity.")
            raise
        except requests.exceptions.HTTPError as e:
            if response.status_code != 404:
                logger.error(f"HTTP Error for {method.upper()} {endpoint}: "
                             f"{e.response.status_code} - {e.response.text}")
                try:
                    error_detail = e.response.json()
                    logger.error(f"Error details: {json.dumps(error_detail, indent=2)}")
                    # If this is a price creation error, log additional context
                    if endpoint == "prices" and method.upper() == "POST":
                        logger.error(f"Price creation failed with payload type. This might be due to missing required fields or incorrect priceType value.")
                except Exception:
                    logger.error(f"Raw error response: {e.response.text}")
                raise
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"An unexpected request error occurred: {e}")
            raise

    def get(self, endpoint: str, params=None):
        return self._request('get', endpoint, params=params)

    def post(self, endpoint: str, payload):
        return self._request('post', endpoint, payload=payload)

    def put(self, endpoint: str, payload):
        return self._request('put', endpoint, payload=payload)


class SKUCatalogProcessor:
    """Process and analyze the comprehensive SKU catalog (full catalog from downloader)."""

    def __init__(self, catalog_file: str):
        self.catalog_file = catalog_file
        self.catalog = self._load_catalog()
        self.metadata_region = (self.catalog.get('metadata') or {}).get('region') or GCP_REGION
        self.processed_skus = self._process_skus()
        self.compute_skus = self._extract_compute_skus()

    def _load_catalog(self):
        """Load the SKU catalog from file. Requires full catalog with 'services'."""
        try:
            with open(self.catalog_file, 'r', encoding='utf-8') as f:
                catalog = json.load(f)
            if 'services' not in catalog:
                raise ValueError("SKU catalog must be the full output from gcp-sku-downloader.py (missing 'services').")
            meta = catalog.get('metadata', {})
            logger.info(f"Loaded SKU catalog: {meta.get('total_services', '?')} services, "
                        f"{meta.get('total_skus', '?')} SKUs")
            return catalog
        except Exception as e:
            logger.error(f"Error loading SKU catalog: {e}")
            raise

    def _process_skus(self):
        """Process and normalize SKUs for pricing sync grouped by broad categories."""
        processed = {
            'compute': [],
            'storage': [],
            'network': [],
            'database': [],
            'ai_ml': [],
            'other': [],
        }
        for service_id, service_data in self.catalog['services'].items():
            service_name = service_data['service_info']['display_name']
            for sku in service_data.get('skus', []):
                normalized_sku = self._normalize_sku(sku, service_name, service_id)
                if normalized_sku:
                    category_key = self._categorize_sku(normalized_sku)
                    processed[category_key].append(normalized_sku)
        for category, skus in processed.items():
            logger.info(f"Processed {len(skus)} {category} SKUs")
        return processed

    def _normalize_sku(self, sku: dict, service_name: str, service_id: str):
        """Normalize SKU data for pricing sync."""
        try:
            pricing_info = sku.get('pricingInfo', [])
            if not pricing_info:
                return None
            tiered_rates = pricing_info[0].get('pricingExpression', {}).get('tieredRates', [])
            if not tiered_rates:
                return None
            rate = tiered_rates[0].get('unitPrice', {})
            if not rate:
                return None
            pricing_unit = pricing_info[0].get('pricingExpression', {}).get('usageUnit', 'hour')
            normalized = {
                'sku_id': sku.get('skuId', ''),
                'description': sku.get('description', ''),
                'service_name': service_name,
                'service_id': service_id,
                'category': sku.get('category', {}),
                'pricing_unit': pricing_unit,
                'rate': rate,
                'tiered_rates': tiered_rates,
                'pricing_info': pricing_info,
                'original_sku': sku,
            }
            return normalized
        except Exception as e:
            logger.warning(f"Error normalizing SKU {sku.get('skuId', 'unknown')}: {e}")
            return None

    def _categorize_sku(self, sku: dict) -> str:
        # unchanged categorization for summary reporting
        service_name = sku['service_name'].lower()
        description = sku['description'].lower()
        category = sku['category']
        resource_family = category.get('resourceFamily', '').lower()
        if resource_family == 'storage':
            return 'storage'
        if resource_family == 'compute':
            return 'compute'
        if resource_family == 'network':
            return 'network'
        if resource_family == 'database':
            return 'database'
        if resource_family in ['ai/ml', 'ai', 'ml']:
            return 'ai_ml'
        if any(k in service_name for k in ['storage', 'cloud storage', 'filestore', 'memorystore']):
            return 'storage'
        if any(k in service_name for k in ['compute', 'vm', 'instance', 'gke', 'kubernetes', 'run', 'functions']):
            return 'compute'
        if any(k in service_name for k in ['network', 'vpc', 'load balancer', 'cdn', 'gateway']):
            return 'network'
        if any(k in service_name for k in ['sql', 'database', 'firestore', 'bigtable', 'spanner', 'alloydb']):
            return 'database'
        if any(k in service_name for k in ['ai', 'ml', 'vertex', 'notebooks', 'composer', 'dataflow']):
            return 'ai_ml'
        if any(k in description for k in ['storage', 'gb', 'tb']):
            return 'storage'
        if any(k in description for k in ['cpu', 'ram', 'memory', 'core']):
            return 'compute'
        if any(k in description for k in ['network', 'bandwidth', 'transfer']):
            return 'network'
        if any(k in description for k in ['database', 'sql', 'query']):
            return 'database'
        if any(k in description for k in ['ai', 'ml', 'machine learning', 'tensorflow']):
            return 'ai_ml'
        return 'other'

    def extract_machine_family(self, text: str) -> Optional[str]:
        name = (text or '').lower()
        patterns = [r'^([a-z]\d+[a-z]?)-', r'\b([a-z]\d+[a-z]?)-']
        for pat in patterns:
            m = re.search(pat, name)
            if m:
                return m.group(1)
        return None

    def classify_price_type(self, sku: dict) -> Tuple[str, Optional[str]]:
        """Return (priceTypeCode, machine_family) for SKU."""
        service_name = (sku.get('service_name') or '').lower()
        description = (sku.get('description') or '').lower()
        category = sku.get('category') or {}
        resource_family = (category.get('resourceFamily') or '').lower()
        resource_group = (category.get('resourceGroup') or '').lower()

        # Storage
        storage_keywords = ['persistent disk', 'pd-', 'hyperdisk', 'local ssd', 'ssd', 'hdd', 'filestore']
        if resource_family == 'storage' or any(k in description for k in storage_keywords):
            return 'storage', None

        # Compute cores
        core_keywords = ['vcpu', 'core', 'cpu']
        if resource_family == 'compute' or resource_group == 'cpu' or any(k in description for k in core_keywords):
            fam = self.extract_machine_family(description)
            return 'cores', fam

        # Memory
        mem_keywords = ['ram', 'memory']
        if resource_group == 'ram' or any(k in description for k in mem_keywords):
            fam = self.extract_machine_family(description)
            return 'memory', fam

        # Default
        return 'software', None

    def _extract_compute_skus(self):
        """Extract compute SKUs for service plan creation (instance families/types)."""
        compute_skus: List[dict] = []
        for service_id, service_data in self.catalog['services'].items():
            if service_data['service_info']['display_name'] == 'Compute Engine':
                for sku in service_data.get('skus', []):
                    description = sku.get('description', '').lower()
                    sku_id = sku.get('skuId', '')
                    instance_patterns = [
                        r'(\w+\d+[a-z]?-\w+-\d+)',  # e2-standard-2, n2-standard-4
                        r'(\w+\d+[a-z]?-\w+)',      # e2-standard, n2-standard
                        r'(\w+\d+[a-z]?-\d+)',      # e2-2, n2-4
                    ]
                    matched = False
                    for pattern in instance_patterns:
                        matches = re.findall(pattern, description)
                        if matches:
                            for match in matches:
                                compute_skus.append({
                                    'instance_type': match,
                                    'sku_id': sku_id,
                                    'description': sku.get('description', ''),
                                    'pricing_info': sku.get('pricingInfo', []),
                                    'original_sku': sku,
                                })
                                matched = True
                                break
                        if matched:
                            break
                    if not matched:
                        compute_skus.append({
                            'instance_type': 'general',
                            'sku_id': sku_id,
                            'description': sku.get('description', ''),
                            'pricing_info': sku.get('pricingInfo', []),
                            'original_sku': sku,
                        })
        logger.info(f"Extracted {len(compute_skus)} compute SKUs for service plan creation")
        return compute_skus

    def get_sku_summary(self):
        summary = {}
        for category, skus in self.processed_skus.items():
            summary[category] = {
                'count': len(skus),
                'services': list(set(sku['service_name'] for sku in skus)),
            }
        return summary

    def get_all_skus(self):
        all_skus = []
        for category_skus in self.processed_skus.values():
            all_skus.extend(category_skus)
        return all_skus


def discover_morpheus_plans(morpheus_api: MorpheusApiClient):
    """Discover existing plans in Morpheus (filters for GCP)."""
    logger.info("Discovering existing Morpheus service plans...")
    try:
        # Query the proper endpoint and scope to Google
        resp = morpheus_api.get("service-plans?provisionTypeCode=google&max=1000")
        all_plans = resp.get("servicePlans", []) if resp else []

        # Include patterns for GCP families and legacy types
        include_patterns = [
            r'^[a-z]\d+[a-z]?-',   # e2-, n2-, c2-, n2d-, c2d-, etc.
            r'^(f1|g1)-',           # legacy types
        ]
        # Exclude obvious non-GCP/noise
        exclude_fragments = [
            'azure', 'rds db.', 'aks ', 'eks ', 'gke controller', 'hyper-v',
            'default', 'discovered', 'terraform', 'workflow', 'controller',
            'stack', 'external', 'manual', 'kubernetes', 'dtus', 'ioh vm',
            ' cpu,', ' memory,', ' storage'
        ]

        gcp_plans = []
        for plan in all_plans:
            name = (plan.get('name') or '').lower()
            if any(frag in name for frag in exclude_fragments):
                continue
            if any(re.match(pat, name) for pat in include_patterns):
                gcp_plans.append(plan)
                continue
            # Fallback on explicit metadata if present
            provision_code = ((plan.get('provisionType') or {}).get('code') or '').lower()
            if provision_code == 'google':
                gcp_plans.append(plan)
                continue
            if plan.get('zone', {}).get('cloud', {}).get('type') == 'gcp':
                gcp_plans.append(plan)

        logger.info(f"Found {len(gcp_plans)} actual GCP Service Plans (excluded {len(all_plans) - len(gcp_plans)} non-GCP plans)")
        return gcp_plans
    except Exception as e:
        logger.error(f"Error discovering plans: {e}")
        return []


def create_comprehensive_pricing_data(sku_processor: SKUCatalogProcessor):
    """Create comprehensive pricing entries from SKU catalog, with type/family/region tags."""
    logger.info("Creating comprehensive pricing data from SKU catalog...")
    all_skus = sku_processor.get_all_skus()
    logger.info(f"Processing {len(all_skus)} SKUs for pricing data creation")
    pricing_data = []
    region = sku_processor.metadata_region
    region_key = region.replace('-', '_')
    for sku in all_skus:
        try:
            price_type, machine_family = sku_processor.classify_price_type(sku)
            rate = sku['rate']
            price_value = 0.0
            if 'units' in rate and 'nanos' in rate:
                units_val = int(rate.get('units') or 0)
                nanos_val = int(rate.get('nanos') or 0)
                price_value = units_val + nanos_val / 1_000_000_000

            # Build a stable code; include region, type, and family if applicable
            base_code_parts = [PRICE_PREFIX.lower(), 'gcp', price_type]
            if machine_family:
                base_code_parts.append(machine_family)
            base_code_parts.append(region_key)
            base_code_parts.append(sku['sku_id'])
            morpheus_code = '.'.join(base_code_parts)

            pricing_entry = {
                'name': f"{PRICE_PREFIX} - {sku['description']}",
                'morpheus_code': morpheus_code,
                'priceTypeCode': price_type,
                'priceUnit': 'hour',
                'price': price_value,
                'cost': price_value,
                'currency': 'USD',
                'incurCharges': True,
                'active': True,
                'region': region,
                'machine_family': machine_family or 'software' if price_type == 'software' else (machine_family or 'unknown'),
                'sku_id': sku['sku_id'],
                'service_name': sku['service_name'],
                'category': sku['category'],
                'description': sku['description'],
            }
            pricing_data.append(pricing_entry)
        except Exception as e:
            logger.warning(f"Error processing SKU {sku.get('sku_id', 'unknown')} for pricing: {e}")
            continue
    logger.info(f"Created {len(pricing_data)} pricing entries")
    return pricing_data


def create_enhanced_price_sets(sku_processor: SKUCatalogProcessor):
    """Create enhanced price sets grouped by category plus a comprehensive set."""
    logger.info("Creating enhanced price sets...")
    sku_summary = sku_processor.get_sku_summary()
    price_sets = []
    for category, summary in sku_summary.items():
        if summary['count'] > 0:
            price_set = {
                'name': f"{PRICE_PREFIX}-{category.upper()}-PRICES",
                'code': f"gcp-{category}-prices",
                'priceUnit': 'month',
                'priceType': 'fixed',
                'incurCharges': True,
                'currency': 'USD',
                'refType': 'ComputeZone',
                'refId': None,
                'volumeType': None,
                'datastore': None,
                'crossCloudApply': False,
                'category': category,
                'sku_count': summary['count'],
                'services': summary['services'],
            }
            price_sets.append(price_set)
    comprehensive_set = {
        'name': f"{PRICE_PREFIX}-COMPREHENSIVE-PRICES",
        'code': "gcp-comprehensive-prices",
        'priceUnit': 'month',
        'priceType': 'fixed',
        'incurCharges': True,
        'currency': 'USD',
        'refType': 'ComputeZone',
        'refId': None,
        'volumeType': None,
        'datastore': None,
        'crossCloudApply': False,
        'category': 'comprehensive',
        'sku_count': sum(summary['count'] for summary in sku_summary.values()),
        'services': list(set(service for summary in sku_summary.values() for service in summary['services'])),
    }
    price_sets.append(comprehensive_set)
    logger.info(f"Created {len(price_sets)} price sets")
    return price_sets


def create_service_plans_from_skus(sku_processor: SKUCatalogProcessor):
    """Create service plans based on compute instance families/types (optional)."""
    logger.info("Creating service plans from compute SKUs...")
    compute_skus = sku_processor.compute_skus
    if not compute_skus:
        logger.warning("No compute SKUs found for service plan creation")
        return []
    instance_families: Dict[str, list] = defaultdict(list)
    for sku in compute_skus:
        instance_type = sku['instance_type']
        if instance_type != 'general':
            family_match = re.match(r'(\w+\d+[a-z]?)', instance_type)
            if family_match:
                family = family_match.group(1)
                instance_families[family].append(sku)
    service_plans = []
    for family, skus in instance_families.items():
        instance_types = list(set(sku['instance_type'] for sku in skus))
        for instance_type in instance_types[:10]:  # Limit to first 10 per family
            service_plan = {
                'name': f"GCP {instance_type.upper()}",
                'code': f"gcp-{instance_type.lower()}",
                'description': f"Google Cloud Platform {instance_type.upper()} instance",
                'editable': True,
                'provisionType': {'id': 1},
                'zone': {'id': 1},  # Adjust to your Morpheus zone
                'priceSets': [],
                'config': {
                    'instanceType': instance_type,
                    'family': family,
                    'region': GCP_REGION,
                },
            }
            service_plans.append(service_plan)
    logger.info(f"Created {len(service_plans)} service plans from {len(compute_skus)} compute SKUs")
    return service_plans


def create_component_price_sets(morpheus_api: MorpheusApiClient, sku_processor: SKUCatalogProcessor, pricing_data: List[dict]):
    """Create component price sets (cores + memory + storage) per machine family and region, with regionCode."""
    logger.info("Creating component price sets per family and region...")

    # Fetch all prices to map code -> id
    all_prices_resp = morpheus_api.get(f"prices?max=5000&phrase={PRICE_PREFIX}")
    if not all_prices_resp or not all_prices_resp.get('prices'):
        logger.error("No prices found with the required prefix. Please run with --create-prices first.")
        return []
    price_id_map = {p['code']: p['id'] for p in all_prices_resp['prices']}

    # Group storage prices by region; cores/memory by (family, region)
    storage_prices_by_region: Dict[str, set] = defaultdict(set)
    family_prices: Dict[Tuple[str, str], dict] = {}

    region = sku_processor.metadata_region
    region_key = region.replace('-', '_')

    storage_types = ['pd-standard', 'pd-ssd', 'pd-balanced', 'pd-extreme', 'local-ssd',
                     'hyperdisk-balanced', 'hyperdisk-extreme', 'regional-pd-standard', 'regional-pd-ssd']

    for p in pricing_data:
        code = p['morpheus_code']
        price_id = price_id_map.get(code)
        if not price_id:
            continue
        if p['priceTypeCode'] == 'storage' or any(t in p['description'].lower() for t in storage_types):
            storage_prices_by_region[region_key].add(price_id)
        elif p['priceTypeCode'] in ('cores', 'memory') and p.get('machine_family') and p['machine_family'] not in ('software', 'unknown'):
            key = (p['machine_family'], region_key)
            if key not in family_prices:
                family_prices[key] = {
                    'name': f"{PRICE_PREFIX} - GCP - {p['machine_family'].upper()} ({region})",
                    'code': f"{PRICE_PREFIX.lower()}.gcp-{p['machine_family']}-{region_key}",
                    'prices': set(),
                    'price_types': set(),
                    'region': region,
                    'region_key': region_key,
                }
            family_prices[key]['prices'].add(price_id)
            family_prices[key]['price_types'].add(p['priceTypeCode'])

    # Attach storage to each family set
    for key, data in family_prices.items():
        rk = data['region_key']
        if rk in storage_prices_by_region:
            data['prices'].update(storage_prices_by_region[rk])
            data['price_types'].add('storage')

    # Create or update price sets
    created_or_updated = []
    for (_fam, _rk), data in family_prices.items():
        if not data['prices']:
            logger.warning(f"Skipping price set '{data['name']}' - no prices found")
            continue
        # Validate required components
        required = {'cores', 'memory', 'storage'}
        if not required.issubset(data['price_types']):
            logger.warning(f"Missing required components for '{data['name']}': {required - data['price_types']}")
            # still proceed if cores+memory exist; storage may be added later

        payload = {
            'priceSet': {
                'name': data['name'],
                'code': data['code'],
                'type': 'component',
                'priceUnit': 'hour',
                'regionCode': data['region'],  # ensure actual region
                'prices': [{'id': pid} for pid in sorted(data['prices'])],
            }
        }
        try:
            existing = morpheus_api.get(f"price-sets?code={data['code']}")
            if existing and existing.get('priceSets'):
                ps_id = existing['priceSets'][0]['id']
                resp = morpheus_api.put(f"price-sets/{ps_id}", payload)
            else:
                resp = morpheus_api.post("price-sets", payload)
            if resp and (resp.get('success') or resp.get('priceSet')):
                created_or_updated.append(data['code'])
                logger.info(f"Processed price set: {data['name']}")
            else:
                logger.error(f"Failed to process price set '{data['name']}': {resp}")
        except Exception as e:
            logger.error(f"Error creating/updating price set '{data['name']}': {e}")
    logger.info(f"Created/updated {len(created_or_updated)} component price sets")
    return created_or_updated


def validate_price_payload(payload: dict) -> bool:
    """Validate that a price payload has all required fields and correct data types."""
    try:
        price = payload.get('price', {})
        required_fields = ['name', 'code', 'priceType', 'priceUnit', 'price', 'cost', 'currency']
        
        for field in required_fields:
            if field not in price:
                logger.error(f"Missing required field '{field}' in price payload")
                return False
            if price[field] is None and field in ['name', 'code', 'priceType', 'priceUnit', 'currency']:
                logger.error(f"Required field '{field}' cannot be None")
                return False
        
        # Validate numeric fields
        numeric_fields = ['price', 'cost']
        for field in numeric_fields:
            if field in price and price[field] is not None:
                try:
                    float(price[field])
                except (ValueError, TypeError):
                    logger.error(f"Field '{field}' must be numeric, got: {price[field]}")
                    return False
        
        # Validate boolean fields
        boolean_fields = ['incurCharges', 'active']
        for field in boolean_fields:
            if field in price and price[field] is not None:
                if not isinstance(price[field], bool):
                    logger.error(f"Field '{field}' must be boolean, got: {price[field]}")
                    return False
        
        # Validate priceType (from official Morpheus API documentation)
        valid_price_types = [
            'fixed', 'compute', 'memory', 'cores', 'storage', 'datastore', 
            'platform', 'software', 'load_balancer', 'load_balancer_virtual_server'
        ]
        if price.get('priceType') not in valid_price_types:
            logger.error(f"Invalid priceType: {price.get('priceType')}. Valid types: {valid_price_types}")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Error validating price payload: {e}")
        return False


def sync_data(morpheus_api: MorpheusApiClient, sku_processor: SKUCatalogProcessor,
              dry_run: bool = False, create_service_plans: bool = False):
    """Sync prices and price sets (and optionally service plans) into Morpheus."""
    logger.info("Starting sync from SKU catalog...")
    if dry_run:
        logger.info("DRY RUN MODE - No changes will be made")
    pricing_data = create_comprehensive_pricing_data(sku_processor)
    # replace old set creator with component sets signature (needs API)
    # price_sets here will hold codes of created sets
    price_sets = []
    service_plans = create_service_plans_from_skus(sku_processor) if create_service_plans else []
    if not dry_run:
        created_prices = []
        for pricing_entry in pricing_data:
            try:
                # Idempotency: skip if exists
                existing = morpheus_api.get(f"prices?code={pricing_entry['morpheus_code']}")
                if existing and existing.get('prices'):
                    continue
                payload = { 'price': {
                    'name': pricing_entry['name'],
                    'code': pricing_entry['morpheus_code'],
                    'priceType': pricing_entry['priceTypeCode'],
                    'priceUnit': pricing_entry['priceUnit'],
                    'price': float(pricing_entry['price']),
                    'cost': float(pricing_entry['cost']),
                    'incurCharges': bool(pricing_entry['incurCharges']),
                    'currency': pricing_entry['currency'],
                    'active': bool(pricing_entry['active'])
                }}
                
                # Validate payload before sending
                if not validate_price_payload(payload):
                    logger.error(f"Skipping invalid price payload for: {pricing_entry['name']}")
                    continue
                    
                response = morpheus_api.post("prices", payload)
                if response:
                    created_prices.append(response)
                    logger.info(f"Successfully created price: {pricing_entry['name']}")
                else:
                    logger.error(f"Failed to create price {pricing_entry['name']}: No response from API")
                time.sleep(0.02)
            except Exception as e:
                logger.error(f"Error creating price {pricing_entry['name']}: {e}")
                logger.debug(f"Failed payload: {json.dumps(payload, indent=2)}")
        # Create component price sets (needs current Morpheus price IDs)
        try:
            created_set_codes = create_component_price_sets(morpheus_api, sku_processor, pricing_data)
            price_sets = created_set_codes
        except Exception as e:
            logger.error(f"Failed creating component price sets: {e}")
        # Service plans optional (unchanged)
        created_service_plans = []
        if create_service_plans:
            for service_plan in service_plans:
                try:
                    response = morpheus_api.post("service-plans", service_plan)
                    if response:
                        created_service_plans.append(response)
                    time.sleep(0.02)
                except Exception as e:
                    logger.error(f"Error creating service plan {service_plan['name']}: {e}")
        logger.info(
            f"Sync completed: {len(created_prices)} prices, {len(price_sets)} price sets, "
            f"{len(created_service_plans)} service plans created"
        )
    else:
        logger.info(
            f"DRY RUN: Would create {len(pricing_data)} prices, component price sets per family/region, "
            f"{len(service_plans)} service plans"
        )
    return {
        'pricing_data': pricing_data,
        'price_sets': price_sets,
        'service_plans': service_plans,
        'sku_summary': sku_processor.get_sku_summary(),
    }


def validate_sync(morpheus_api: MorpheusApiClient, sku_processor: SKUCatalogProcessor):
    """Validate existing prices/price sets against catalog size and provide coverage."""
    logger.info("Validating sync results in Morpheus...")
    try:
        prices_response = morpheus_api.get("prices")
        price_sets_response = morpheus_api.get("price-sets")
        service_plans_response = morpheus_api.get("service-plans")
        existing_prices = prices_response.get("prices", []) if prices_response else []
        existing_price_sets = price_sets_response.get("priceSets", []) if price_sets_response else []
        existing_service_plans = service_plans_response.get("servicePlans", []) if service_plans_response else []
        gcp_prices = [p for p in existing_prices if p.get("code", "").startswith("gcp-")]
        gcp_price_sets = [ps for ps in existing_price_sets if ps.get("code", "").startswith("gcp-")]
        gcp_service_plans = [sp for sp in existing_service_plans if sp.get("code", "").startswith("gcp-")]
        sku_summary = sku_processor.get_sku_summary()
        total_skus = sum(summary['count'] for summary in sku_summary.values())
        coverage = (len(gcp_prices) / total_skus * 100) if total_skus > 0 else 0
        logger.info("Validation Results:")
        logger.info(f"  Total prices in Morpheus: {len(existing_prices)} (GCP: {len(gcp_prices)})")
        logger.info(f"  Total price sets in Morpheus: {len(existing_price_sets)} (GCP: {len(gcp_price_sets)})")
        logger.info(f"  Total service plans in Morpheus: {len(existing_service_plans)} (GCP: {len(gcp_service_plans)})")
        logger.info(f"  Total SKUs in catalog: {total_skus}")
        logger.info(f"  Price coverage: {len(gcp_prices)}/{total_skus} ({coverage:.1f}%)")
        return {
            'total_prices': len(existing_prices),
            'gcp_prices': len(gcp_prices),
            'total_price_sets': len(existing_price_sets),
            'gcp_price_sets': len(gcp_price_sets),
            'total_service_plans': len(existing_service_plans),
            'gcp_service_plans': len(gcp_service_plans),
            'catalog_skus': total_skus,
            'coverage_percentage': coverage,
        }
    except Exception as e:
        logger.error(f"Error during validation: {e}")
        return None


def _print_plans_summary(plans: List[dict]):
    """Print grouped summary of GCP plans by machine family with examples."""
    from collections import defaultdict
    family_groups: Dict[str, List[str]] = defaultdict(list)
    for p in plans:
        name = (p.get('name') or '').lower()
        family = 'unknown'
        for pattern in [r'^([a-z]\d+[a-z]?)-', r'^(f1|g1)-']:
            m = re.match(pattern, name)
            if m:
                family = m.group(1)
                break
        family_groups[family].append(p.get('name') or '')
    logger.info(f"Found {len(plans)} actual GCP Service Plans (grouped by family):")
    for family in sorted(family_groups.keys()):
        items = sorted(family_groups[family])
        logger.info(f"  {family.upper()} family: {len(items)} plans")
        for example in items[:3]:
            print(f"   - {example}")
        if len(items) > 3:
            print(f"   - ... and {len(items) - 3} more {family} plans")


def main():
    parser = argparse.ArgumentParser(
        description="Final unified GCP price sync using downloaded SKU catalog",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python gcp-price-sync-final.py --sku-catalog gcp_skus_YYYYMMDD_HHMMSS.json --dry-run\n"
            "  python gcp-price-sync-final.py --sku-catalog gcp_skus_YYYYMMDD_HHMMSS.json --create-service-plans\n"
            "  python gcp-price-sync-final.py --sku-catalog gcp_skus_YYYYMMDD_HHMMSS.json --validate-only\n"
        ),
    )
    parser.add_argument('--sku-catalog', required=True,
                        help='Path to the full SKU catalog JSON (output of gcp-sku-downloader.py)')
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no changes made)')
    parser.add_argument('--create-service-plans', action='store_true', help='Create service plans from compute SKUs')
    parser.add_argument('--validate-only', action='store_true', help='Only validate existing sync results')
    parser.add_argument('--create-prices', action='store_true', help='Create prices from SKU catalog')
    parser.add_argument('--create-price-sets', action='store_true', help='Create price sets from SKU catalog summary')
    parser.add_argument('--map-to-plans', action='store_true', help='Map created price sets to discovered GCP service plans')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--discover-morpheus-plans', action='store_true', help='Discover and print GCP service plans, then exit')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        morpheus_api = MorpheusApiClient(MORPHEUS_URL, MORPHEUS_TOKEN)
        sku_processor = SKUCatalogProcessor(args.sku_catalog)

        # Discover existing GCP service plans
        discovered_plans = discover_morpheus_plans(morpheus_api)

        if args.discover_morpheus_plans:
            # Print grouped summary and exit
            _print_plans_summary(discovered_plans)
            logger.info("Discovery-only run complete.")
            return
        if not discovered_plans:
            logger.warning("No GCP service plans discovered in Morpheus.")
            if not args.validate_only:
                print("\nNo GCP service plans found. Prices and price sets will still be created; mapping to plans will be skipped. You can create plans in Morpheus or run again with --create-service-plans.")
            
        print("\n=== GCP SKU Catalog Information ===")
        metadata = sku_processor.catalog.get('metadata', {})
        print(f"Region: {metadata.get('region')}")
        print(f"Download Time: {metadata.get('download_timestamp')}")
        print(f"Total Services: {metadata.get('total_services')}")
        print(f"Total SKUs: {metadata.get('total_skus')}")

        processed_summary = sku_processor.get_sku_summary()
        print("\nProcessed SKU Summary:")
        for category, summary in processed_summary.items():
            services_display = ', '.join(summary['services'][:3])
            ellipsis = '...' if len(summary['services']) > 3 else ''
            print(f"  {category}: {summary['count']} SKUs")
            if summary['services']:
                print(f"    Services: {services_display}{ellipsis}")

        if args.validate_only:
            results = validate_sync(morpheus_api, sku_processor)
            if results:
                print("\n=== Validation Summary ===")
                print(f"GCP Prices in Morpheus: {results['gcp_prices']}")
                print(f"GCP Price Sets in Morpheus: {results['gcp_price_sets']}")
                print(f"GCP Service Plans in Morpheus: {results['gcp_service_plans']}")
                print(f"Catalog SKUs: {results['catalog_skus']}")
                print(f"Coverage: {results['coverage_percentage']:.1f}%")
        else:
            print("\n=== Starting Sync ===")

            # Decide what to create based on flags; default is to create both if neither is specified
            create_prices_flag = args.create_prices or (not args.create_prices and not args.create_price_sets)
            create_price_sets_flag = args.create_price_sets or (not args.create_prices and not args.create_price_sets)

            pricing_data = []
            price_sets = []
            service_plans_payloads = []

            if create_prices_flag:
                pricing_data = create_comprehensive_pricing_data(sku_processor)
                if not args.dry_run:
                    for pricing_entry in pricing_data:
                        try:
                            existing = morpheus_api.get(f"prices?code={pricing_entry['morpheus_code']}")
                            if existing and existing.get('prices'):
                                logger.debug(f"Skipping existing price: {pricing_entry['morpheus_code']}")
                                continue
                            payload = { 'price': {
                                'name': pricing_entry['name'],
                                'code': pricing_entry['morpheus_code'],
                                'priceType': pricing_entry['priceTypeCode'],
                                'priceUnit': pricing_entry['priceUnit'],
                                'price': float(pricing_entry['price']),
                                'cost': float(pricing_entry['cost']),
                                'incurCharges': bool(pricing_entry['incurCharges']),
                                'currency': pricing_entry['currency'],
                                'active': bool(pricing_entry['active'])
                            }}
                            
                            # Validate payload before sending
                            if not validate_price_payload(payload):
                                logger.error(f"Skipping invalid price payload for: {pricing_entry['name']}")
                                continue
                                
                            response = morpheus_api.post("prices", payload)
                            if response:
                                logger.info(f"Successfully created price: {pricing_entry['name']}")
                            else:
                                logger.error(f"Failed to create price {pricing_entry['name']}: No response from API")
                            time.sleep(0.02)
                        except Exception as e:
                            logger.error(f"Error creating price {pricing_entry['name']}: {e}")
                            logger.debug(f"Failed payload: {json.dumps(payload, indent=2)}")
                else:
                    logger.info(f"DRY RUN: Would create {len(pricing_data)} prices")

            if create_price_sets_flag:
                # Build component price sets using current pricing data
                if not args.dry_run:
                    try:
                        created_codes = create_component_price_sets(morpheus_api, sku_processor, pricing_data)
                        logger.info(f"Created/updated {len(created_codes)} component price sets")
                    except Exception as e:
                        logger.error(f"Error creating component price sets: {e}")
                else:
                    logger.info("DRY RUN: Would create component price sets per family and region")

            # Optionally map created price sets to discovered plans
            if args.map_to_plans and not args.dry_run and discovered_plans:
                try:
                    # Refresh to get IDs
                    ps_resp = morpheus_api.get(f"price-sets?max=1000&phrase={PRICE_PREFIX}")
                    price_sets_list = ps_resp.get('priceSets', []) if ps_resp else []
                    price_set_map = {ps['code']: ps for ps in price_sets_list}
                    updated = 0
                    for plan in discovered_plans:
                        # Extract region
                        plan_region = None
                        config = plan.get('config') or {}
                        if config:
                            plan_region = (config.get('zoneRegion') or config.get('region') or None)
                            if not plan_region and config.get('availabilityZone'):
                                parts = config['availabilityZone'].split('-')
                                if len(parts) >= 2:
                                    plan_region = '-'.join(parts[0:2])
                        if not plan_region:
                            logger.debug(f"Skipping plan '{plan.get('name','')}' - no region found")
                            continue
                        # Extract family
                        name_lower = (plan.get('name') or '').lower()
                        m = re.search(r'^(?:google-)?([a-z]\d+[a-z]?)-', name_lower)
                        if not m:
                            logger.debug(f"Skipping plan '{plan.get('name','')}' - no machine family parsed")
                            continue
                        family = m.group(1)
                        expected_code = f"{PRICE_PREFIX.lower()}.gcp-{family}-{plan_region.replace('-', '_')}"
                        ps = price_set_map.get(expected_code)
                        if not ps:
                            logger.debug(f"No matching price set for plan '{plan.get('name','')}' expecting '{expected_code}'")
                            continue
                        current_ids = {ps['id'] for ps in (plan.get('priceSets') or []) if ps and 'id' in ps}
                        final_ids = current_ids.union({ps['id']})
                        payload = {"servicePlan": {"priceSets": [{"id": pid} for pid in final_ids]}}
                        resp = morpheus_api.put(f"service-plans/{plan['id']}", payload)
                        if resp and (resp.get('success') or resp.get('servicePlan')):
                            updated += 1
                    logger.info(f"Mapped price sets to {updated}/{len(discovered_plans)} plans")
                except Exception as e:
                    logger.error(f"Failed to map price sets to plans: {e}")

            validation_results = validate_sync(morpheus_api, sku_processor)

            print("\n=== Final Sync Summary ===")
            print(f"SKU Categories Processed: {list(processed_summary.keys())}")
            for category, summary in processed_summary.items():
                print(f"  {category}: {summary['count']} SKUs")
            if validation_results:
                print(f"\nCoverage Achieved: {validation_results['coverage_percentage']:.1f}%")
                print(f"Total GCP Prices in Morpheus: {validation_results['gcp_prices']}")
                print(f"Total GCP Price Sets in Morpheus: {validation_results['gcp_price_sets']}")
                print(f"Total GCP Service Plans in Morpheus: {validation_results['gcp_service_plans']}")

        logger.info("Final unified price sync completed successfully!")
    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()