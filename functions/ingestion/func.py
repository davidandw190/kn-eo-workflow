from parliament import Context
from cloudevents.http import CloudEvent
import os
import time
import logging
import json
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import io
import planetary_computer
from pystac_client import Client
from minio import Minio
import urllib3

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define a constant for the STAC catalog URL
PC_CATALOG_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"

def get_config():
    """Get configuration from environment variables with sensible defaults"""
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-assets'),
        'catalog_url': os.getenv('STAC_CATALOG_URL', PC_CATALOG_URL),
        'collection': os.getenv('STAC_COLLECTION', 'sentinel-2-l2a'),
        'max_cloud_cover': float(os.getenv('MAX_CLOUD_COVER', '20')),
        'max_items': int(os.getenv('MAX_ITEMS', '3')),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5')),
        'max_workers': int(os.getenv('MAX_WORKERS', '4')),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/ingestion'),
        'sink_url': os.getenv('K_SINK')
    }

def create_cloud_event(event_type, data, source):
    """Create a CloudEvent with proper attributes"""
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

def initialize_minio_client(config):
    """Initialize MinIO client with optimized connection pool"""
    max_retries = config['max_retries']
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Initializing MinIO client with endpoint: {config['minio_endpoint']}")
            
            client = Minio(
                config['minio_endpoint'],
                access_key=config['minio_access_key'],
                secret_key=config['minio_secret_key'],
                secure=config['secure_connection'],
                http_client=urllib3.PoolManager(
                    maxsize=40,  # Larger connection pool to handle concurrent operations
                    timeout=urllib3.Timeout(
                        connect=config['connection_timeout'],
                        read=config['connection_timeout']*3
                    ),
                    retries=urllib3.Retry(
                        total=3,
                        backoff_factor=0.2,
                        status_forcelist=[500, 502, 503, 504, 429]
                    )
                )
            )
            
            # Test connection
            client.list_buckets()
            logger.info("Successfully connected to MinIO")
            return client
            
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to initialize MinIO client after {max_retries} attempts: {str(e)}")
                raise
            logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
            time.sleep(config['connection_timeout'])

def ensure_bucket(minio_client, bucket_name):
    """Ensure bucket exists, create if needed"""
    try:
        if not minio_client.bucket_exists(bucket_name):
            logger.info(f"Creating bucket: {bucket_name}")
            minio_client.make_bucket(bucket_name)
            logger.info(f"Successfully created bucket: {bucket_name}")
        else:
            logger.info(f"Bucket {bucket_name} already exists and is ready")
    except Exception as e:
        logger.error(f"Failed to ensure bucket '{bucket_name}' exists: {str(e)}")
        raise

def search_scenes(client, config, bbox, time_range, cloud_cover, max_items):
    """Search for scenes with proper error handling and limits"""
    logger.info(f"Searching for {config['collection']} scenes in bbox: {bbox}, timerange: {time_range}, max_items: {max_items}")
    
    search_params = {
        "collections": [config['collection']],
        "bbox": bbox,
        "datetime": time_range,
        "query": {
            "eo:cloud_cover": {"lt": cloud_cover},
            "s2:degraded_msi_data_percentage": {"lt": 5},
            "s2:nodata_pixel_percentage": {"lt": 10}
        },
        "limit": max_items
    }
    
    max_retries = 3
    base_delay = 2
    
    for attempt in range(max_retries):
        try:
            search = client.search(**search_params)
            items = list(search.get_items())
            
            if not items:
                logger.warning("No scenes found for the specified parameters.")
                return []
            
            # Double-check limit enforcement - CRITICAL FIX
            if len(items) > max_items:
                logger.info(f"Limiting to {max_items} scenes out of {len(items)} found")
                items = items[:max_items]
            
            logger.info(f"Found {len(items)} scene(s)")
            return items
            
        except Exception as e:
            is_timeout = "exceeded the maximum allowed time" in str(e)
            
            if attempt == max_retries - 1:
                logger.error(f"Failed to search for scenes after {max_retries} attempts: {str(e)}")
                raise
                
            delay = base_delay * (2 ** attempt)
            
            if is_timeout:
                logger.warning(f"STAC API timeout on attempt {attempt + 1}. Retrying in {delay} seconds...")
            else:
                logger.warning(f"STAC search error on attempt {attempt + 1}: {str(e)}. Retrying in {delay} seconds...")
                
            time.sleep(delay)

def download_asset(item, asset_id, config):
    """Download a specific asset with token refresh and proper retries"""
    if asset_id not in item.assets:
        raise ValueError(f"Asset {asset_id} not found in item {item.id}")
    
    asset = item.assets[asset_id]
    max_retries = config['max_retries']
    base_delay = 2
    
    for attempt in range(max_retries):
        try:
            # Re-sign URL on each attempt to ensure fresh tokens
            asset_href = planetary_computer.sign(asset.href)
            
            # Increased timeouts for larger assets
            timeout = (config['connection_timeout'], config['connection_timeout'] * 5)
            
            with requests.get(
                asset_href, 
                stream=True, 
                timeout=timeout
            ) as response:
                response.raise_for_status()
                
                # Read in chunks to avoid memory issues with large assets
                data = b''
                for chunk in response.iter_content(chunk_size=8*1024*1024):
                    if chunk:
                        data += chunk
                
                metadata = {
                    "item_id": item.id,
                    "collection": item.collection_id,
                    "asset_id": asset_id,
                    "content_type": asset.media_type,
                    "original_href": asset.href,
                    "timestamp": str(int(time.time())),
                    "bbox": json.dumps(item.bbox),
                    "datetime": item.datetime.isoformat() if item.datetime else None
                }
                
                return data, metadata
                
        except requests.exceptions.RequestException as e:
            # Check for auth errors specifically
            if "403" in str(e) and "Server failed to authenticate" in str(e):
                logger.warning(f"Authentication error for asset {asset_id}, refreshing token...")
                # Force refresh the token on next attempt
                planetary_computer.SIGNED_HREFS.clear()
            elif attempt == max_retries - 1:
                logger.error(f"Failed to download asset {asset_id} after {max_retries} attempts: {str(e)}")
                raise
                
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s...")
            time.sleep(delay)

def store_asset(minio_client, bucket, item_id, asset_id, data, metadata, config):
    """Store asset in MinIO with proper error handling"""
    collection = metadata.get("collection", "unknown")
    object_name = f"{collection}/{item_id}/{asset_id}"
    
    content_type = metadata.get("content_type", "application/octet-stream")
    
    for attempt in range(config['max_retries']):
        try:
            minio_client.put_object(
                bucket,
                object_name,
                io.BytesIO(data),
                length=len(data),
                content_type=content_type,
                metadata=metadata
            )
            
            logger.info(f"Stored asset {asset_id} from item {item_id} in bucket {bucket}")
            
            return {
                "bucket": bucket,
                "object_name": object_name,
                "item_id": item_id,
                "asset_id": asset_id,
                "collection": collection,
                "size": len(data),
                "content_type": content_type
            }
        except Exception as e:
            if attempt == config['max_retries'] - 1:
                logger.error(f"Failed to store asset {asset_id} after {config['max_retries']} attempts: {str(e)}")
                raise
            logger.warning(f"Storage attempt {attempt + 1} failed: {str(e)}")
            time.sleep(config['connection_timeout'])

def process_single_asset(minio_client, item, asset_id, request_id, config):
    """Process a single asset with download and storage"""
    try:
        logger.info(f"Processing asset {asset_id} for item {item.id}")
        
        asset_data, asset_metadata = download_asset(item, asset_id, config)
        
        result = store_asset(
            minio_client,
            config['raw_bucket'],
            item.id,
            asset_id,
            asset_data,
            asset_metadata,
            config
        )
        
        result["request_id"] = request_id
        
        return result
    except Exception as e:
        logger.error(f"Error processing asset {asset_id}: {str(e)}")
        raise

def process_scene(minio_client, item, request_id, bbox, config):
    """Process all assets for a single scene"""
    scene_id = item.id
    collection_id = item.collection_id
    acquisition_date = item.datetime.strftime('%Y-%m-%d') if item.datetime else None
    cloud_cover = item.properties.get('eo:cloud_cover', 'N/A')
    
    logger.info(f"Processing scene: {scene_id} from {collection_id}, date: {acquisition_date}, cloud cover: {cloud_cover}%")
    
    # Determine which assets to ingest
    band_assets = [f"B{i:02d}" for i in range(1, 13)] + ["B8A", "SCL"]
    assets_to_ingest = [band for band in band_assets if band in item.assets]
    
    if not assets_to_ingest:
        logger.warning(f"No assets to ingest for scene {scene_id}")
        return None, []
    
    # Create registration event data
    registration_data = {
        "request_id": request_id,
        "item_id": scene_id,
        "collection": collection_id,
        "assets": assets_to_ingest,
        "bbox": bbox,
        "acquisition_date": acquisition_date,
        "cloud_cover": cloud_cover
    }
    
    # Create registration event
    registration_event = create_cloud_event(
        "eo.scene.assets.registered",
        registration_data,
        config['event_source']
    )
    
    # Ingest assets with thread pool for parallelism
    processed_assets = []
    with ThreadPoolExecutor(max_workers=config['max_workers']) as executor:
        future_to_asset = {
            executor.submit(
                process_single_asset, 
                minio_client, 
                item, 
                asset_id, 
                request_id, 
                config
            ): asset_id
            for asset_id in assets_to_ingest
        }
        
        for future in as_completed(future_to_asset):
            asset_id = future_to_asset[future]
            try:
                result = future.result()
                processed_assets.append(result)
                logger.info(f"Successfully processed asset {asset_id} for scene {scene_id}")
            except Exception as e:
                logger.error(f"Failed to process asset {asset_id} for scene {scene_id}: {str(e)}")
    
    return registration_event, processed_assets

def send_events_synchronously(events, sink_url, config):
    """Send events to the broker with retries, in a synchronous manner"""
    from cloudevents.conversion import to_structured
    
    logger.info(f"Sending {len(events)} events to sink: {sink_url}")
    
    success_count = 0
    # Process in small batches to avoid overwhelming the broker
    batch_size = 5
    
    for i in range(0, len(events), batch_size):
        batch = events[i:i+batch_size]
        logger.info(f"Sending batch {i//batch_size + 1} of {(len(events) + batch_size - 1)//batch_size}")
        
        for event in batch:
            # Convert CloudEvent to HTTP request format
            headers, body = to_structured(event)
            
            # Send with retries
            for attempt in range(3):
                try:
                    response = requests.post(
                        sink_url,
                        headers=headers,
                        data=body,
                        timeout=30
                    )
                    
                    if 200 <= response.status_code < 300:
                        success_count += 1
                        logger.info(f"Successfully sent event: {event['type']} for {event.data.get('item_id', 'unknown')}")
                        break
                    else:
                        logger.warning(f"Failed to send event, status: {response.status_code}, attempt: {attempt+1}")
                        time.sleep(2 ** attempt)  # Exponential backoff
                except Exception as e:
                    logger.error(f"Error sending event (attempt {attempt+1}): {str(e)}")
                    if attempt < 2:  # Don't sleep after the last attempt
                        time.sleep(2 ** attempt)
        
        # Brief pause between batches
        time.sleep(1)
    
    return success_count

def main(context: Context):
    """Main function for handling eo.search.requested events"""
    try:
        logger.info("STAC Ingestion Function activated")
        config = get_config()
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        # Extract request data from the cloud event
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        event_type = context.cloud_event["type"]
        
        if event_type != "eo.search.requested":
            logger.warning(f"Unexpected event type: {event_type}, expected: eo.search.requested")
            return {"error": f"Unexpected event type: {event_type}"}, 400
        
        logger.info(f"Processing search request: {event_data.get('request_id')}")
        
        # Extract search parameters
        bbox = event_data.get('bbox')
        time_range = event_data.get('time_range')
        cloud_cover = event_data.get('cloud_cover', config['max_cloud_cover'])
        max_items = event_data.get('max_items', config['max_items'])
        request_id = event_data.get('request_id', str(uuid.uuid4()))
        
        if not bbox or not time_range:
            error_msg = "Missing required parameters (bbox, time_range)"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        # Initialize MinIO client
        minio_client = initialize_minio_client(config)
        ensure_bucket(minio_client, config['raw_bucket'])
        
        # Initialize STAC client
        stac_client = Client.open(
            config['catalog_url'],
            modifier=planetary_computer.sign_inplace
        )
        
        # Search for scenes
        items = search_scenes(stac_client, config, bbox, time_range, cloud_cover, max_items)
        
        if not items:
            logger.warning("No scenes found matching the search criteria")
            return {"status": "no_scenes_found", "request_id": request_id}, 200
        
        all_events = []
        
        # Process each scene
        for item in items:
            try:
                registration_event, processed_assets = process_scene(
                    minio_client, 
                    item, 
                    request_id, 
                    bbox, 
                    config
                )
                
                if registration_event and processed_assets:
                    # Add the registration event
                    all_events.append(registration_event)
                    
                    # Create and add asset events
                    for asset_data in processed_assets:
                        asset_event = create_cloud_event(
                            "eo.asset.ingested",
                            asset_data,
                            config['event_source']
                        )
                        all_events.append(asset_event)
            except Exception as e:
                logger.error(f"Error processing scene {item.id}: {str(e)}")
        
        if not all_events:
            error_msg = "No scenes were successfully processed"
            logger.error(error_msg)
            
            error_event = create_cloud_event(
                "eo.processing.error",
                {
                    "request_id": request_id,
                    "error": error_msg,
                    "timestamp": int(time.time())
                },
                config['event_source']
            )
            
            return error_event
        
        # Get sink URL from environment
        sink_url = config['sink_url']
        if not sink_url:
            logger.error("K_SINK environment variable not found. Cannot send events.")
            return {"error": "K_SINK not configured"}, 500
        
        # Send all events directly to the broker
        success_count = send_events_synchronously(all_events, sink_url, config)
        
        logger.info(f"Successfully sent {success_count} of {len(all_events)} events to broker")
        
        # Return a simple status response
        return {
            "status": "success",
            "request_id": request_id,
            "scenes_processed": len(items),
            "events_emitted": success_count,
            "total_events": len(all_events)
        }, 200
        
    except Exception as e:
        logger.exception(f"Error in STAC ingestion function: {str(e)}")
        
        error_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "request_id": event_data.get("request_id") if "event_data" in locals() else str(uuid.uuid4())
        }
        
        error_event = create_cloud_event(
            "eo.processing.error",
            error_data,
            config['event_source']
        )
        
        return error_event