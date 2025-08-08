# HPE Morpheus v8.0.7 Google Cloud Pricing Integration

This repository contains five Python scripts that integrate HPE Morpheus v8.0.7 with Google Cloud pricing, enabling GCP costing to appear in Morpheus for provisioned resources.

## Overview

The integration consists of five sequential scripts that:

1. **discover_service_plans.py** - Discovers or creates Google Cloud service plans in Morpheus
2. **discover_gcp_skus.py** - Retrieves Google Cloud SKUs using the Billing API
3. **create_prices.py** - Creates individual prices in Morpheus from GCP SKUs
4. **create_price_sets.py** - Groups prices into logical price sets by category
5. **map_price_sets_to_service_plans.py** - Maps price sets to service plans, completing the integration

## Prerequisites

### Software Requirements

- Python 3.x
- pip package manager
- Network access to both Morpheus API and Google Cloud Billing API

### Python Dependencies

Install the required packages:

```bash
pip install requests google-auth google-api-python-client
```

### Environment Variables

Set the following environment variables before running the scripts:

```bash
export MORPHEUS_TOKEN="9fcc4426-c89a-4430-b6d7-99d5950fc1cc"
export MORPHEUS_URL="https://xdjmorpheapp01"
export GOOGLE_APPLICATION_CREDENTIALS="/root/gcloud_offline_rpms/auth.json"
export GCP_REGION="asia-southeast2"
export PRICE_PREFIX="IOH-CP"
```

### Authentication Setup

1. **Morpheus Authentication**: Ensure the `MORPHEUS_TOKEN` has sufficient permissions to:
   - Read and create service plans
   - Read and create prices
   - Read and create price sets
   - Update service plans

2. **Google Cloud Authentication**: Ensure the service account key file at `GOOGLE_APPLICATION_CREDENTIALS` has the following permissions:
   - `cloudbilling.services.list`
   - `cloudbilling.skus.list`
   - Cloud Billing API access

## Usage Instructions

### Running the Complete Integration

Execute the scripts in sequence (1 → 5):

```bash
# Script 1: Discover/create service plans
python3 discover_service_plans.py

# Script 2: Discover GCP SKUs
python3 discover_gcp_skus.py

# Script 3: Create prices from SKUs
python3 create_prices.py

# Script 4: Create price sets
python3 create_price_sets.py

# Script 5: Map price sets to service plans
python3 map_price_sets_to_service_plans.py
```

### Individual Script Details

#### Script 1: discover_service_plans.py

**Purpose**: Discovers existing Google Cloud service plans or creates a new one if missing.

**Dependencies**: `requests`

**Input**: Environment variables

**Output**: `service_plans.json`

**Idempotent**: Yes - will not create duplicates

```bash
python3 discover_service_plans.py
```

#### Script 2: discover_gcp_skus.py

**Purpose**: Authenticates to Google Cloud Billing API and retrieves SKUs for Google Compute Engine in the specified region.

**Dependencies**: `google-auth`, `google-api-python-client`

**Input**: Environment variables, Google Cloud credentials

**Output**: `gcp_skus.json`

**Features**:
- Finds Google Compute Engine service automatically
- Filters SKUs by region
- Categorizes SKUs by resource family/group
- Handles pagination for large SKU lists

```bash
python3 discover_gcp_skus.py
```

#### Script 3: create_prices.py

**Purpose**: Creates individual prices in Morpheus from GCP SKU data.

**Dependencies**: `requests`

**Input**: `service_plans.json`, `gcp_skus.json`

**Output**: `prices.json`

**Features**:
- Maps GCP SKU categories to Morpheus price types
- Determines appropriate volume types (hourly, monthly, yearly)
- Extracts pricing information from GCP SKU data
- Handles various resource types (compute, storage, network, etc.)
- Skips SKUs without pricing information

**Idempotent**: Yes - checks for existing prices before creating

```bash
python3 create_prices.py
```

#### Script 4: create_price_sets.py

**Purpose**: Groups related prices into logical price sets based on resource categories.

**Dependencies**: `requests`

**Input**: `prices.json`

**Output**: `price_sets.json`

**Features**:
- Groups prices by resource family and group
- Creates meaningful price set names and descriptions
- Associates price sets with appropriate service plans

**Idempotent**: Yes - checks for existing price sets before creating

```bash
python3 create_price_sets.py
```

#### Script 5: map_price_sets_to_service_plans.py

**Purpose**: Maps the created price sets to service plans, completing the integration.

**Dependencies**: `requests`

**Input**: `service_plans.json`, `price_sets.json`

**Output**: `integration_summary.json`

**Features**:
- Updates service plans with price set mappings
- Preserves existing mappings
- Provides comprehensive integration summary
- Confirms successful completion

**Idempotent**: Yes - only adds new mappings

```bash
python3 map_price_sets_to_service_plans.py
```

## Data Flow

```
Environment Variables
       ↓
[Script 1] → service_plans.json
       ↓
[Script 2] → gcp_skus.json
       ↓
[Script 3] → prices.json (uses service_plans.json + gcp_skus.json)
       ↓
[Script 4] → price_sets.json (uses prices.json)
       ↓
[Script 5] → integration_summary.json (uses service_plans.json + price_sets.json)
```

## Output Files

- **service_plans.json**: Contains discovered/created service plan data
- **gcp_skus.json**: Contains all GCP SKUs for the specified region
- **prices.json**: Contains created price IDs and details
- **price_sets.json**: Contains created price set IDs and details
- **integration_summary.json**: Final integration status and summary

## Error Handling

All scripts include comprehensive error handling:

- **Retry Logic**: API calls retry up to 3 times with 1-second delays
- **Validation**: Input validation for environment variables and files
- **Graceful Failures**: Scripts continue processing remaining items if individual items fail
- **Detailed Logging**: INFO level logging with timestamps for all operations
- **Error Reporting**: Clear error messages with context for troubleshooting

## Idempotent Operations

All scripts are designed to be idempotent:

- Running scripts multiple times will not create duplicates
- Existing resources are detected and reused
- Only missing resources are created
- Safe to re-run the entire sequence

## Verification

After running all scripts, verify the integration:

1. Check the `integration_summary.json` file for completion status
2. Log into Morpheus and navigate to:
   - **Administration** → **Provisioning** → **Service Plans**
   - **Administration** → **Provisioning** → **Pricing**
3. Verify that GCP pricing appears when provisioning Google Cloud resources

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Verify `MORPHEUS_TOKEN` has correct permissions
   - Ensure `auth.json` file exists and has proper GCP permissions
   - Check network connectivity to both APIs

2. **Missing Environment Variables**:
   - All required environment variables must be set
   - Use `echo $VARIABLE_NAME` to verify values

3. **API Rate Limits**:
   - Scripts include retry logic for temporary failures
   - Large SKU datasets may take time to process

4. **File Not Found Errors**:
   - Ensure scripts are run in sequence
   - Check that output files from previous scripts exist

### Logging

All scripts log to console with INFO level by default. For more detailed debugging, modify the logging level in each script:

```python
logging.basicConfig(level=logging.DEBUG)
```

## Support

For issues with the integration scripts:

1. Check the log output for specific error messages
2. Verify all prerequisites are met
3. Ensure environment variables are correctly set
4. Run scripts individually to isolate issues

## Security Considerations

- Store API tokens and credentials securely
- Use appropriate IAM permissions with minimal required access
- Rotate credentials regularly
- Monitor API usage and access logs

## License

This integration is provided as-is for HPE Morpheus v8.0.7 and Google Cloud Platform integration purposes.