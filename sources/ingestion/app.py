import os
import time
import json
import uuid
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import io
import requests
from flask import Flask, request, Response
from minio import Minio
import urllib3
import random
import threading
import planetary_computer
from pystac_client import Client
from cloudevents.http import CloudEvent, from_http
from cloudevents.conversion import to_structured

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

processed_requests = set()
request_lock = threading.Lock()

def get_config():
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-assets'),
        'catalog_url': os.getenv('STAC_CATALOG_URL', 'https://planetarycomputer.microsoft.com/api/stac/v1'),
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
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

def initialize_minio_client(config):
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
                    maxsize=40, 
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
            timeout = (config['connection_timeout'], config['connection_timeout'] * 5)
            
            with requests.get(
                asset_href, 
                stream=True, 
                timeout=timeout
            ) as response:
                response.raise_for_status()
                
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
            if "403" in str(e) and "Server failed to authenticate" in str(e):
                logger.warning(f"Authentication error for asset {asset_id}, refreshing token...")
                planetary_computer.SIGNED_HREFS.clear()
            elif attempt == max_retries - 1:
                logger.error(f"Failed to download asset {asset_id} after {max_retries} attempts: {str(e)}")
                raise
                
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s...")
            time.sleep(delay)

def store_asset(minio_client, bucket, item_id, asset_id, data, metadata, config):
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
    
    band_assets = [f"B{i:02d}" for i in range(1, 13)] + ["B8A", "SCL"]
    assets_to_ingest = [band for band in band_assets if band in item.assets]
    
    if not assets_to_ingest:
        logger.warning(f"No assets to ingest for scene {scene_id}")
        return None, []
    
    registration_data = {
        "request_id": request_id,
        "item_id": scene_id,
        "collection": collection_id,
        "assets": assets_to_ingest,
        "bbox": bbox,
        "acquisition_date": acquisition_date,
        "cloud_cover": cloud_cover
    }
    
    registration_event = create_cloud_event(
        "eo.scene.assets.registered",
        registration_data,
        config['event_source']
    )
    
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

def send_event(event, sink_url, retries=3, base_delay=1.0):
    """Send a CloudEvent to the sink with improved reliability"""
    event_type = event['type']
    event_id = event['id']
    logger.info(f"Sending event {event_type} (ID: {event_id}) to {sink_url}")
    headers, body = to_structured(event)
    headers['X-Request-ID'] = event_id
    
    for attempt in range(retries):
        try:
            response = requests.post(
                sink_url,
                headers=headers,
                data=body,
                timeout=(5, 15)  # (connect timeout, read timeout)
            )
            
            if 200 <= response.status_code < 300:
                logger.info(f"Successfully sent event {event_type} (ID: {event_id})")
                return True
            else:
                logger.warning(f"Event delivery failed: status={response.status_code}, attempt={attempt+1}/{retries}")
                
                if 400 <= response.status_code < 500:
                    logger.error(f"Not retrying client error: {response.status_code}")
                    return False
                    
                if attempt < retries - 1:
                    delay = base_delay * (2 ** attempt) * (0.5 + 0.5 * random.random())
                    logger.info(f"Retrying in {delay:.2f} seconds")
                    time.sleep(delay)
                else:
                    return False
        except Exception as e:
            error_type = type(e).__name__
            logger.error(f"Error sending event: [{error_type}] {str(e)}")
            if attempt < retries - 1:
                delay = base_delay * (2 ** attempt) * (0.5 + 0.5 * random.random())
                logger.info(f"Retrying in {delay:.2f} seconds")
                time.sleep(delay)
            else:
                return False
    
    return False



def handle_search_requested(event_data, config):
    """Process a search request and emit appropriate events"""
    request_id = event_data.get('request_id')
    
    with request_lock:
        if request_id in processed_requests:
            logger.info(f"Request {request_id} already processed, skipping")
            return True
        processed_requests.add(request_id)
    
    try:
        logger.info(f"Processing search request: {request_id}")
        
        # Extract parameters
        bbox = event_data.get('bbox')
        time_range = event_data.get('time_range')
        cloud_cover = event_data.get('cloud_cover', config['max_cloud_cover'])
        max_items = event_data.get('max_items', config['max_items'])
        
        # Validate parameters
        if not bbox or not time_range:
            logger.error("Missing required parameters (bbox, time_range)")
            return False
        
        # Initialize clients
        minio_client = initialize_minio_client(config)
        ensure_bucket(minio_client, config['raw_bucket'])
        
        stac_client = Client.open(
            config['catalog_url'],
            modifier=planetary_computer.sign_inplace
        )
        
        # Search for scenes
        items = search_scenes(stac_client, config, bbox, time_range, cloud_cover, max_items)
        
        if not items:
            logger.warning("No scenes found matching the search criteria")
            return False
        
        # Get sink URL for events
        sink_url = config.get('sink_url')
        if not sink_url:
            logger.error("No K_SINK environment variable found. Cannot send events.")
            return False
        
        # Process scenes sequentially to avoid overwhelming the system
        success = True
        for item in items:
            try:
                # Process scene and get events
                registration_event, processed_assets = process_scene(
                    minio_client, 
                    item, 
                    request_id, 
                    bbox, 
                    config
                )
                
                if registration_event and processed_assets:
                    # Send the registration event
                    logger.info(f"Sending registration event for scene {item.id}")
                    if not send_event(registration_event, sink_url):
                        success = False
                    
                    # Send asset events
                    for asset_data in processed_assets:
                        asset_event = create_cloud_event(
                            "eo.asset.ingested",
                            asset_data,
                            config['event_source']
                        )
                        
                        logger.info(f"Sending ingestion event for asset {asset_data['asset_id']}")
                        if not send_event(asset_event, sink_url):
                            success = False
            except Exception as e:
                logger.error(f"Error processing scene {item.id}: {str(e)}")
                success = False
        
        # Send completion event
        completion_data = {
            "request_id": request_id,
            "scenes_processed": len(items),
            "success": success,
            "timestamp": int(time.time())
        }
        
        completion_event = create_cloud_event(
            "eo.search.completed",
            completion_data,
            config['event_source']
        )
        
        send_event(completion_event, sink_url)
        
        return success
        
    except Exception as e:
        logger.exception(f"Error handling search request: {str(e)}")
        return False
    
    
      
# Create Flask app
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    return {"status": "up"}

# sources/ingestion/app.py - Update the event handler
@app.route('/', methods=['POST'])
def handle_event():
    """Handle incoming CloudEvents"""
    config = get_config()
    
    try:
        # Parse the CloudEvent
        event_data = request.get_json()
        
        # For Knative events, try to parse as CloudEvent
        if 'type' in request.headers:
            try:
                cloud_event = from_http(request.headers, request.get_data())
                event_type = cloud_event['type']
                
                logger.info(f"Received CloudEvent: {event_type}")
                
                if event_type == 'eo.search.requested':
                    logger.info("Processing search request event")
                    
                    # Extract data from CloudEvent
                    event_data = cloud_event.data
                    if isinstance(event_data, str):
                        event_data = json.loads(event_data)
                    
                    # Process in background thread to return quickly
                    import threading
                    thread = threading.Thread(
                        target=handle_search_requested,
                        args=(event_data, config)
                    )
                    thread.daemon = True
                    thread.start()
                    
                    return Response(status=204)  # Success with no content
            except Exception as ce_error:
                logger.error(f"Error parsing CloudEvent: {ce_error}")
        
        # Fallback for direct JSON
        if isinstance(event_data, dict) and 'request_id' in event_data and 'bbox' in event_data:
            logger.info("Processing direct JSON request")
            handle_search_requested(event_data, config)
            return Response(status=204)
        
        logger.warning(f"Unrecognized event format")
        return Response(status=400)
        
    except Exception as e:
        logger.exception(f"Error processing event: {str(e)}")
        return Response(status=500)
    
if __name__ == '__main__':
    # Log startup info
    config = get_config()
    logger.info(f"Starting ingestion container source with:")
    logger.info(f"  MINIO_ENDPOINT: {config['minio_endpoint']}")
    logger.info(f"  RAW_BUCKET: {config['raw_bucket']}")
    logger.info(f"  MAX_ITEMS: {config['max_items']}")
    logger.info(f"  K_SINK: {config.get('sink_url', '(not set)')}")
    
    # Start Flask app
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)