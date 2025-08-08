#!/usr/bin/env python3
"""
render_tables.py

Reads service_plans.json and gcp_skus.json (and optionally gcp_services.json)
and produces human-readable CSV tables to support manual mapping between
Morpheus Service Plans and GCP services/SKUs.

Outputs:
- service_plans_table.csv: key fields of Morpheus service plans (filtered to Google)
- gcp_services_table.csv: GCP Billing service catalog (displayName, serviceId)
- gcp_skus_table.csv: flattened key fields of GCP SKUs
- mapping_suggestions.csv: heuristic suggestions linking plans to potential services/SKUs

Usage:
  python render_tables.py
"""

import csv
import json
import os
import sys
from typing import Dict, List, Any

SERVICE_PLANS_FILE = "service_plans.json"
GCP_SKUS_FILE = "gcp_skus.json"
GCP_SERVICES_FILE = "gcp_services.json"

OUT_PLANS_CSV = "service_plans_table.csv"
OUT_SERVICES_CSV = "gcp_services_table.csv"
OUT_SKUS_CSV = "gcp_skus_table.csv"
OUT_MAP_CSV = "mapping_suggestions.csv"


def load_json(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_csv(path: str, header: List[str], rows: List[List[Any]]):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


def extract_google_service_plans(plans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    google_plans: List[Dict[str, Any]] = []
    for p in plans:
        prov_name = (p.get('provisionType') or {}).get('name', '')
        if 'Google' in prov_name or 'GCP' in prov_name or 'GKE' in prov_name:
            google_plans.append(p)
    return google_plans


def build_plans_table(service_plans_json: Dict[str, Any]) -> List[List[Any]]:
    plans = service_plans_json.get('servicePlans', [])
    g_plans = extract_google_service_plans(plans)
    rows: List[List[Any]] = []
    for p in g_plans:
        rows.append([
            p.get('id'),
            p.get('name'),
            p.get('code'),
            (p.get('provisionType') or {}).get('name'),
            p.get('description', ''),
        ])
    return rows


def build_services_table(services_json: Dict[str, Any]) -> List[List[Any]]:
    services = services_json.get('services', [])
    rows: List[List[Any]] = []
    for s in services:
        rows.append([
            s.get('displayName') or s.get('name'),
            s.get('serviceId') or (s.get('name') or '').split('/')[-1]
        ])
    return rows


def build_skus_table(gcp_skus_json: Dict[str, Any]) -> List[List[Any]]:
    skus = gcp_skus_json.get('skus', [])
    rows: List[List[Any]] = []
    for sku in skus:
        category = sku.get('category', {})
        regions = sku.get('serviceRegions', [])
        price_info = (sku.get('pricingInfo') or [{}])[0]
        expr = price_info.get('pricingExpression', {})
        unit_price = (expr.get('tieredRates') or [{}])[0].get('unitPrice', {})
        rows.append([
            sku.get('skuId'),
            category.get('serviceDisplayName'),
            sku.get('description'),
            category.get('resourceFamily'),
            category.get('resourceGroup'),
            category.get('usageType'),
            ";".join(regions),
            unit_price.get('currencyCode'),
            unit_price.get('units'),
            unit_price.get('nanos'),
            expr.get('usageUnit'),
        ])
    return rows


def guess_mapping_suggestions(service_plans_json: Dict[str, Any], gcp_services_json: Dict[str, Any], gcp_skus_json: Dict[str, Any]) -> List[List[Any]]:
    # Simple heuristics: based on plan name/code keywords to service name and sample SKU
    plans = extract_google_service_plans(service_plans_json.get('servicePlans', []))
    services = gcp_services_json.get('services', [])
    skus = gcp_skus_json.get('skus', [])

    def service_for_plan(name_lc: str, code_lc: str) -> str:
        # Map keywords to likely GCP billing service
        if any(k in name_lc or k in code_lc for k in ['gke', 'kubernetes', 'autopilot']):
            return 'Kubernetes Engine'
        if any(k in name_lc or k in code_lc for k in ['disk', 'pd-', 'storage', 'volume']):
            return 'Compute Engine'  # PD billed under Compute Engine
        if any(k in name_lc or k in code_lc for k in ['vm', 'instance', 'compute', 'n1', 'n2', 'e2', 'c2', 'c3', 't2d', 'a2', 'm1', 'm2', 'm3']):
            return 'Compute Engine'
        return ''

    # Build quick lookups
    service_display_to_id: Dict[str, str] = {}
    for s in services:
        disp = s.get('displayName') or s.get('name')
        sid = s.get('serviceId') or (s.get('name') or '').split('/')[-1]
        if disp:
            service_display_to_id[disp] = sid

    # For each plan, pick a plausible service and a few example SKUs that include relevant keywords
    rows: List[List[Any]] = []
    for p in plans:
        name = p.get('name', '')
        code = p.get('code', '')
        name_lc = name.lower()
        code_lc = code.lower()
        target_service = service_for_plan(name_lc, code_lc)
        target_service_id = service_display_to_id.get(target_service, '') if target_service else ''

        # find up to 3 candidate SKUs by keyword match in description
        keywords: List[str] = []
        if target_service == 'Kubernetes Engine':
            keywords = ['kubernetes', 'autopilot', 'gke']
        elif target_service == 'Compute Engine':
            # instance families and PD hints
            keywords = ['vcpu', 'memory', 'instance', 'pd-ssd', 'pd-balanced', 'pd-standard', 'pd-extreme', 'ram']
        else:
            keywords = []

        candidate_skus: List[str] = []
        for sku in skus:
            cat = sku.get('category', {})
            if target_service and cat.get('serviceDisplayName') != target_service:
                continue
            desc = (sku.get('description') or '').lower()
            if any(k in desc for k in keywords):
                candidate_skus.append(f"{sku.get('skuId')}|{sku.get('description')}")
            if len(candidate_skus) >= 3:
                break

        rows.append([
            p.get('id'),
            name,
            code,
            (p.get('provisionType') or {}).get('name', ''),
            target_service,
            target_service_id,
            "; ".join(candidate_skus)
        ])

    return rows


def main():
    # Load inputs
    if not os.path.exists(SERVICE_PLANS_FILE):
        print(f"Missing {SERVICE_PLANS_FILE}")
        sys.exit(1)
    if not os.path.exists(GCP_SKUS_FILE):
        print(f"Missing {GCP_SKUS_FILE}")
        sys.exit(1)

    service_plans_json = load_json(SERVICE_PLANS_FILE)
    gcp_skus_json = load_json(GCP_SKUS_FILE)
    gcp_services_json = {'services': []}
    if os.path.exists(GCP_SERVICES_FILE):
        gcp_services_json = load_json(GCP_SERVICES_FILE)

    # Build and write tables
    plans_rows = build_plans_table(service_plans_json)
    write_csv(OUT_PLANS_CSV, ['planId', 'planName', 'planCode', 'provisionType', 'description'], plans_rows)

    services_rows = build_services_table(gcp_services_json)
    write_csv(OUT_SERVICES_CSV, ['serviceDisplayName', 'serviceId'], services_rows)

    skus_rows = build_skus_table(gcp_skus_json)
    write_csv(OUT_SKUS_CSV, ['skuId', 'serviceDisplayName', 'description', 'resourceFamily', 'resourceGroup', 'usageType', 'regions', 'currencyCode', 'priceUnits', 'priceNanos', 'usageUnit'], skus_rows)

    map_rows = guess_mapping_suggestions(service_plans_json, gcp_services_json, gcp_skus_json)
    write_csv(OUT_MAP_CSV, ['planId', 'planName', 'planCode', 'provisionType', 'suggestedService', 'serviceId', 'exampleSkus'], map_rows)

    print(f"Wrote: {OUT_PLANS_CSV}, {OUT_SERVICES_CSV}, {OUT_SKUS_CSV}, {OUT_MAP_CSV}")


if __name__ == '__main__':
    main()