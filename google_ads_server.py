"""
Cohnen MCP Google Ads Server - Modified for Vysta Cloud Run deployment.

Changes from original:
- fastmcp library (HTTP transport support)
- Auth via env vars (refresh token, no browser login)
- Bearer token middleware
- API version updated to v23
- Health check endpoint
"""

from typing import Any, Dict, List, Optional, Union
from pydantic import Field
import os
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
import logging

# MCP - use fastmcp for HTTP transport
from fastmcp import FastMCP

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('google_ads_server')

mcp = FastMCP("google-ads-server")

# Constants and configuration
SCOPES = ['https://www.googleapis.com/auth/adwords']
API_VERSION = "v23"

# Get credentials from environment variables
GOOGLE_ADS_DEVELOPER_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")


def format_customer_id(customer_id: str) -> str:
    """Format customer ID to ensure it's 10 digits without dashes."""
    customer_id = str(customer_id)
    customer_id = customer_id.replace('\"', '').replace('"', '')
    customer_id = ''.join(char for char in customer_id if char.isdigit())
    return customer_id.zfill(10)


def get_credentials():
    """Get OAuth credentials from environment variables (refresh token)."""
    refresh_token = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN")
    client_id = os.environ.get("GOOGLE_ADS_MCP_OAUTH_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_ADS_MCP_OAUTH_CLIENT_SECRET")

    if not refresh_token or not client_id or not client_secret:
        raise ValueError(
            "Missing required env vars: GOOGLE_ADS_REFRESH_TOKEN, "
            "GOOGLE_ADS_MCP_OAUTH_CLIENT_ID, GOOGLE_ADS_MCP_OAUTH_CLIENT_SECRET"
        )

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES,
    )

    if not creds.valid:
        creds.refresh(Request())
        logger.info("Credentials refreshed successfully")

    return creds


def get_headers(creds):
    """Get headers for Google Ads API requests."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("GOOGLE_ADS_DEVELOPER_TOKEN environment variable not set")

    if not creds.valid:
        creds.refresh(Request())

    headers = {
        'Authorization': f'Bearer {creds.token}',
        'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
        'content-type': 'application/json'
    }

    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        headers['login-customer-id'] = format_customer_id(GOOGLE_ADS_LOGIN_CUSTOMER_ID)

    return headers


# ============================================================
# TOOLS (unchanged from original — all 11 tools)
# ============================================================

@mcp.tool()
async def list_accounts() -> str:
    """Lists all accessible Google Ads accounts."""
    try:
        creds = get_credentials()
        headers = get_headers(creds)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers:listAccessibleCustomers"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            return f"Error accessing accounts: {response.text}"

        customers = response.json()
        if not customers.get('resourceNames'):
            return "No accessible accounts found."

        result_lines = ["Accessible Google Ads Accounts:"]
        result_lines.append("-" * 50)

        for resource_name in customers['resourceNames']:
            customer_id = resource_name.split('/')[-1]
            formatted_id = format_customer_id(customer_id)
            result_lines.append(f"Account ID: {formatted_id}")

        return "\n".join(result_lines)

    except Exception as e:
        return f"Error listing accounts: {str(e)}"


@mcp.tool()
async def execute_gaql_query(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    query: str = Field(description="Valid GAQL query string")
) -> str:
    """Execute a custom GAQL query."""
    try:
        creds = get_credentials()
        headers = get_headers(creds)

        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"

        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            return f"Error executing query: {response.text}"

        results = response.json()
        if not results.get('results'):
            return "No results found for the query."

        result_lines = [f"Query Results for Account {formatted_customer_id}:"]
        result_lines.append("-" * 80)

        fields = []
        first_result = results['results'][0]
        for key in first_result:
            if isinstance(first_result[key], dict):
                for subkey in first_result[key]:
                    fields.append(f"{key}.{subkey}")
            else:
                fields.append(key)

        result_lines.append(" | ".join(fields))
        result_lines.append("-" * 80)

        for result in results['results']:
            row_data = []
            for field in fields:
                if "." in field:
                    parent, child = field.split(".")
                    value = str(result.get(parent, {}).get(child, ""))
                else:
                    value = str(result.get(field, ""))
                row_data.append(value)
            result_lines.append(" | ".join(row_data))

        return "\n".join(result_lines)

    except Exception as e:
        return f"Error executing GAQL query: {str(e)}"


@mcp.tool()
async def get_campaign_performance(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    days: int = Field(default=30, description="Number of days to look back")
) -> str:
    """Get campaign performance metrics for the specified time period."""
    query = f"""
        SELECT
            campaign.id, campaign.name, campaign.status,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.average_cpc
        FROM campaign
        WHERE segments.date DURING LAST_{days}_DAYS
        ORDER BY metrics.cost_micros DESC
        LIMIT 50
    """
    return await execute_gaql_query(customer_id, query)


@mcp.tool()
async def get_ad_performance(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    days: int = Field(default=30, description="Number of days to look back")
) -> str:
    """Get ad performance metrics for the specified time period."""
    query = f"""
        SELECT
            ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.status,
            campaign.name, ad_group.name,
            metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions
        FROM ad_group_ad
        WHERE segments.date DURING LAST_{days}_DAYS
        ORDER BY metrics.impressions DESC
        LIMIT 50
    """
    return await execute_gaql_query(customer_id, query)


@mcp.tool()
async def run_gaql(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    query: str = Field(description="Valid GAQL query string"),
    format: str = Field(default="table", description="Output format: 'table', 'json', or 'csv'")
) -> str:
    """Execute any GAQL query with custom formatting options."""
    try:
        creds = get_credentials()
        headers = get_headers(creds)

        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"

        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            return f"Error executing query: {response.text}"

        results = response.json()
        if not results.get('results'):
            return "No results found for the query."

        if format.lower() == "json":
            return json.dumps(results, indent=2)

        elif format.lower() == "csv":
            fields = []
            first_result = results['results'][0]
            for key, value in first_result.items():
                if isinstance(value, dict):
                    for subkey in value:
                        fields.append(f"{key}.{subkey}")
                else:
                    fields.append(key)

            csv_lines = [",".join(fields)]
            for result in results['results']:
                row_data = []
                for field in fields:
                    if "." in field:
                        parent, child = field.split(".")
                        value = str(result.get(parent, {}).get(child, "")).replace(",", ";")
                    else:
                        value = str(result.get(field, "")).replace(",", ";")
                    row_data.append(value)
                csv_lines.append(",".join(row_data))

            return "\n".join(csv_lines)

        else:
            result_lines = [f"Query Results for Account {formatted_customer_id}:"]
            result_lines.append("-" * 100)

            fields = []
            field_widths = {}
            first_result = results['results'][0]

            for key, value in first_result.items():
                if isinstance(value, dict):
                    for subkey in value:
                        field = f"{key}.{subkey}"
                        fields.append(field)
                        field_widths[field] = len(field)
                else:
                    fields.append(key)
                    field_widths[key] = len(key)

            for result in results['results']:
                for field in fields:
                    if "." in field:
                        parent, child = field.split(".")
                        value = str(result.get(parent, {}).get(child, ""))
                    else:
                        value = str(result.get(field, ""))
                    field_widths[field] = max(field_widths[field], len(value))

            header = " | ".join(f"{field:{field_widths[field]}}" for field in fields)
            result_lines.append(header)
            result_lines.append("-" * len(header))

            for result in results['results']:
                row_data = []
                for field in fields:
                    if "." in field:
                        parent, child = field.split(".")
                        value = str(result.get(parent, {}).get(child, ""))
                    else:
                        value = str(result.get(field, ""))
                    row_data.append(f"{value:{field_widths[field]}}")
                result_lines.append(" | ".join(row_data))

            return "\n".join(result_lines)

    except Exception as e:
        return f"Error executing GAQL query: {str(e)}"


@mcp.tool()
async def get_ad_creatives(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)")
) -> str:
    """Get ad creative details including headlines, descriptions, and URLs."""
    query = """
        SELECT
            ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type,
            ad_group_ad.ad.final_urls, ad_group_ad.status,
            ad_group_ad.ad.responsive_search_ad.headlines,
            ad_group_ad.ad.responsive_search_ad.descriptions,
            ad_group.name, campaign.name
        FROM ad_group_ad
        WHERE ad_group_ad.status != 'REMOVED'
        ORDER BY campaign.name, ad_group.name
        LIMIT 50
    """

    try:
        creds = get_credentials()
        headers = get_headers(creds)

        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"

        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            return f"Error retrieving ad creatives: {response.text}"

        results = response.json()
        if not results.get('results'):
            return "No ad creatives found for this customer ID."

        output_lines = [f"Ad Creatives for Customer ID {formatted_customer_id}:"]
        output_lines.append("=" * 80)

        for i, result in enumerate(results['results'], 1):
            ad = result.get('adGroupAd', {}).get('ad', {})
            ad_group = result.get('adGroup', {})
            campaign = result.get('campaign', {})

            output_lines.append(f"\n{i}. Campaign: {campaign.get('name', 'N/A')}")
            output_lines.append(f"   Ad Group: {ad_group.get('name', 'N/A')}")
            output_lines.append(f"   Ad ID: {ad.get('id', 'N/A')}")
            output_lines.append(f"   Status: {result.get('adGroupAd', {}).get('status', 'N/A')}")
            output_lines.append(f"   Type: {ad.get('type', 'N/A')}")

            rsa = ad.get('responsiveSearchAd', {})
            if rsa:
                if 'headlines' in rsa:
                    output_lines.append("   Headlines:")
                    for headline in rsa['headlines']:
                        output_lines.append(f"     - {headline.get('text', 'N/A')}")
                if 'descriptions' in rsa:
                    output_lines.append("   Descriptions:")
                    for desc in rsa['descriptions']:
                        output_lines.append(f"     - {desc.get('text', 'N/A')}")

            final_urls = ad.get('finalUrls', [])
            if final_urls:
                output_lines.append(f"   Final URLs: {', '.join(final_urls)}")

            output_lines.append("-" * 80)

        return "\n".join(output_lines)

    except Exception as e:
        return f"Error retrieving ad creatives: {str(e)}"


@mcp.tool()
async def get_account_currency(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)")
) -> str:
    """Retrieve the default currency code used by the Google Ads account."""
    query = "SELECT customer.id, customer.currency_code FROM customer LIMIT 1"

    try:
        creds = get_credentials()
        headers = get_headers(creds)

        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"

        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            return f"Error retrieving account currency: {response.text}"

        results = response.json()
        if not results.get('results'):
            return "No account information found for this customer ID."

        customer = results['results'][0].get('customer', {})
        currency_code = customer.get('currencyCode', 'Not specified')

        return f"Account {formatted_customer_id} uses currency: {currency_code}"

    except Exception as e:
        return f"Error retrieving account currency: {str(e)}"


@mcp.tool()
async def get_image_assets(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    limit: int = Field(default=50, description="Maximum number of image assets to return")
) -> str:
    """Retrieve all image assets in the account including their full-size URLs."""
    query = f"""
        SELECT
            asset.id, asset.name, asset.type,
            asset.image_asset.full_size.url,
            asset.image_asset.full_size.height_pixels,
            asset.image_asset.full_size.width_pixels,
            asset.image_asset.file_size
        FROM asset
        WHERE asset.type = 'IMAGE'
        LIMIT {limit}
    """

    try:
        creds = get_credentials()
        headers = get_headers(creds)

        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"

        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            return f"Error retrieving image assets: {response.text}"

        results = response.json()
        if not results.get('results'):
            return "No image assets found for this customer ID."

        output_lines = [f"Image Assets for Customer ID {formatted_customer_id}:"]
        output_lines.append("=" * 80)

        for i, result in enumerate(results['results'], 1):
            asset = result.get('asset', {})
            image_asset = asset.get('imageAsset', {})
            full_size = image_asset.get('fullSize', {})

            output_lines.append(f"\n{i}. Asset ID: {asset.get('id', 'N/A')}")
            output_lines.append(f"   Name: {asset.get('name', 'N/A')}")

            if full_size:
                output_lines.append(f"   Image URL: {full_size.get('url', 'N/A')}")
                output_lines.append(f"   Dimensions: {full_size.get('widthPixels', 'N/A')} x {full_size.get('heightPixels', 'N/A')} px")

            file_size = image_asset.get('fileSize', 'N/A')
            if file_size != 'N/A':
                file_size_kb = int(file_size) / 1024
                output_lines.append(f"   File Size: {file_size_kb:.2f} KB")

            output_lines.append("-" * 80)

        return "\n".join(output_lines)

    except Exception as e:
        return f"Error retrieving image assets: {str(e)}"


@mcp.tool()
async def analyze_image_assets(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    days: int = Field(default=30, description="Number of days to look back")
) -> str:
    """Analyze image assets with their performance metrics across campaigns."""
    query = """
        SELECT
            asset.id, asset.name,
            asset.image_asset.full_size.url,
            asset.image_asset.full_size.width_pixels,
            asset.image_asset.full_size.height_pixels,
            campaign.name,
            metrics.impressions, metrics.clicks,
            metrics.conversions, metrics.cost_micros
        FROM campaign_asset
        WHERE asset.type = 'IMAGE' AND segments.date DURING LAST_30_DAYS
        ORDER BY metrics.impressions DESC
        LIMIT 200
    """

    try:
        creds = get_credentials()
        headers = get_headers(creds)

        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"

        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            return f"Error analyzing image assets: {response.text}"

        results = response.json()
        if not results.get('results'):
            return "No image asset performance data found."

        assets_data = {}
        for result in results.get('results', []):
            asset = result.get('asset', {})
            asset_id = asset.get('id')

            if asset_id not in assets_data:
                assets_data[asset_id] = {
                    'name': asset.get('name', f"Asset {asset_id}"),
                    'url': asset.get('imageAsset', {}).get('fullSize', {}).get('url', 'N/A'),
                    'impressions': 0, 'clicks': 0, 'conversions': 0, 'cost_micros': 0,
                    'campaigns': set()
                }

            metrics = result.get('metrics', {})
            assets_data[asset_id]['impressions'] += int(metrics.get('impressions', 0))
            assets_data[asset_id]['clicks'] += int(metrics.get('clicks', 0))
            assets_data[asset_id]['conversions'] += float(metrics.get('conversions', 0))
            assets_data[asset_id]['cost_micros'] += int(metrics.get('costMicros', 0))

            campaign = result.get('campaign', {})
            if campaign.get('name'):
                assets_data[asset_id]['campaigns'].add(campaign.get('name'))

        output_lines = [f"Image Asset Performance (Last {days} days):"]
        output_lines.append("=" * 100)

        sorted_assets = sorted(assets_data.items(), key=lambda x: x[1]['impressions'], reverse=True)

        for asset_id, data in sorted_assets:
            ctr = (data['clicks'] / data['impressions'] * 100) if data['impressions'] > 0 else 0
            output_lines.append(f"\nAsset ID: {asset_id} | {data['name']}")
            output_lines.append(f"  Impressions: {data['impressions']:,} | Clicks: {data['clicks']:,} | CTR: {ctr:.2f}%")
            output_lines.append(f"  Conversions: {data['conversions']:.2f} | Cost: {data['cost_micros']:,} micros")
            output_lines.append(f"  Campaigns: {', '.join(list(data['campaigns'])[:5])}")
            output_lines.append("-" * 100)

        return "\n".join(output_lines)

    except Exception as e:
        return f"Error analyzing image assets: {str(e)}"


# ============================================================
# SERVER STARTUP — HTTP mode with bearer token middleware
# ============================================================

class BearerTokenMiddleware:
    """ASGI middleware: healthz + bearer token check."""

    def __init__(self, app):
        self.app = app
        self.expected_token = os.environ.get("MCP_BEARER_TOKEN")

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            path = scope.get("path", "")

            if path == "/healthz":
                body = b'{"status":"ok"}'
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        [b"content-type", b"application/json"],
                        [b"content-length", str(len(body)).encode()],
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return

            if self.expected_token:
                auth_header = ""
                for header_name, header_value in scope.get("headers", []):
                    if header_name == b"authorization":
                        auth_header = header_value.decode()
                        break

                if not auth_header.startswith("Bearer ") or auth_header[7:] != self.expected_token:
                    body = b'{"error":"Invalid or missing bearer token"}'
                    await send({
                        "type": "http.response.start",
                        "status": 401,
                        "headers": [
                            [b"content-type", b"application/json"],
                            [b"content-length", str(len(body)).encode()],
                        ],
                    })
                    await send({"type": "http.response.body", "body": body})
                    return

        await self.app(scope, receive, send)


def run_server():
    port = int(os.environ.get("PORT", "8080"))
    bearer_token = os.environ.get("MCP_BEARER_TOKEN")

    if bearer_token:
        logger.info("Bearer token authentication enabled")
    else:
        logger.warning("No MCP_BEARER_TOKEN set — endpoint is unprotected")

    app = mcp.http_app(transport="streamable-http")
    wrapped_app = BearerTokenMiddleware(app)

    import uvicorn
    uvicorn.run(wrapped_app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    run_server()
