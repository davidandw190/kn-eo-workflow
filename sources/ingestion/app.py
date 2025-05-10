import os
import time
import json
import uuid
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import redis
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
        'sink_url': os.getenv('K_SINK'),
        'redis_host': os.getenv('REDIS_HOST', 'redis.eo-workflow.svc.cluster.local'),
        'redis_port': int(os.getenv('REDIS_PORT', '6379')),
        'redis_key_prefix': os.getenv('REDIS_KEY_PREFIX', 'eo:ingestion:'),
        'redis_key_expiry': int(os.getenv('REDIS_KEY_EXPIRY', '86400'))  # 24 hours
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

def initialize_redis_client(config):
    """Initialize Redis client for deduplication"""
    redis_host = config.get('redis_host')
    redis_port = config.get('redis_port')
    
    for attempt in range(config['max_retries']):
        try:
            logger.info(f"Connecting to Redis at {redis_host}:{redis_port}")
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                socket_timeout=config['connection_timeout'],
                socket_connect_timeout=config['connection_timeout'],
                retry_on_timeout=True,
                decode_responses=True
            )
            client.ping()  # Test connection
            logger.info("Successfully connected to Redis")
            return client
        except Exception as e:
            if attempt == config['max_retries'] - 1:
                logger.error(f"Failed to connect to Redis after {config['max_retries']} attempts: {str(e)}")
                raise
            logger.warning(f"Redis connection attempt {attempt + 1} failed, retrying: {str(e)}")
            time.sleep(config['connection_timeout'])

def is_event_processed(redis_client, event_type, unique_id, config):
    """Check if an event has already been processed"""
    try:
        key = f"{config['redis_key_prefix']}{event_type}:{unique_id}"
        return bool(redis_client.exists(key))
    except Exception as e:
        logger.warning(f"Redis check failed for {event_type}:{unique_id}: {str(e)}")
        return False 

def mark_event_processed(redis_client, event_type, unique_id, config):
    """Mark an event as processed with TTL"""
    try:
        key = f"{config['redis_key_prefix']}{event_type}:{unique_id}"
        redis_client.set(key, datetime.now(timezone.utc).isoformat())
        redis_client.expire(key, config['redis_key_expiry'])
        logger.debug(f"Marked {event_type}:{unique_id} as processed (TTL: {config['redis_key_expiry']}s)")
    except Exception as e:
        logger.warning(f"Failed to mark event as processed {event_type}:{unique_id}: {str(e)}")
        
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

def process_scene(minio_client, item, request_id, bbox, config, redis_client=None):
    scene_id = item.id
    collection_id = item.collection_id
    
    dedup_id = f"{request_id}:{scene_id}"
    
    if redis_client:
        if is_event_processed(redis_client, "scene_registration", dedup_id, config):
            logger.info(f"Scene {scene_id} already registered for request {request_id}, skipping")
            return None, []
    
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
    
    # Mark this registration as processed if Redis is available
    if redis_client:
        mark_event_processed(redis_client, "scene_registration", dedup_id, config)
    
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
                config,
                redis_client
            ): asset_id
            for asset_id in assets_to_ingest
        }
        
        for future in as_completed(future_to_asset):
            asset_id = future_to_asset[future]
            try:
                result = future.result()
                if result:  # Only add if asset was actually processed
                    processed_assets.append(result)
                    logger.info(f"Successfully processed asset {asset_id} for scene {scene_id}")
            except Exception as e:
                logger.error(f"Failed to process asset {asset_id} for scene {scene_id}: {str(e)}")
    
    return registration_event, processed_assets

def process_single_asset(minio_client, item, asset_id, request_id, config, redis_client=None):
    """Process a single asset with deduplication"""
    try:
        scene_id = item.id
        
        # Create a unique deduplication ID
        dedup_id = f"{request_id}:{scene_id}:{asset_id}"
        
        # Check if this asset has already been processed (if Redis is available)
        if redis_client:
            if is_event_processed(redis_client, "asset_ingestion", dedup_id, config):
                logger.info(f"Asset {asset_id} already ingested for scene {scene_id}, skipping")
                return None
        
        logger.info(f"Processing asset {asset_id} for item: {scene_id}, request: {request_id}")
        
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
        
        # Mark this asset as processed if Redis is available
        if redis_client:
            mark_event_processed(redis_client, "asset_ingestion", dedup_id, config)
        
        return result
    except Exception as e:
        logger.error(f"Error processing asset {asset_id}: {str(e)}")
        raise


def process_single_asset(minio_client, item, asset_id, request_id, config, redis_client=None):
    """Process a single asset with deduplication"""
    try:
        scene_id = item.id
        
        # Create a unique deduplication ID
        dedup_id = f"{request_id}:{scene_id}:{asset_id}"
        
        # Check if this asset has already been processed (if Redis is available)
        if redis_client:
            if is_event_processed(redis_client, "asset_ingestion", dedup_id, config):
                logger.info(f"Asset {asset_id} already ingested for scene {scene_id}, skipping")
                return None
        
        logger.info(f"Processing asset {asset_id} for item: {scene_id}, request: {request_id}")
        
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
        
        # Mark this asset as processed if Redis is available
        if redis_client:
            mark_event_processed(redis_client, "asset_ingestion", dedup_id, config)
        
        return result
    except Exception as e:
        logger.error(f"Error processing asset {asset_id}: {str(e)}")
        raise
    
        
def send_event(event, sink_url, retries=3, base_delay=0.5, max_delay=10.0):
    """Send a CloudEvent to the sink with improved reliability"""
    event_type = event['type']
    event_id = event['id']
    
    logger.info(f"Sending event {event_type} (ID: {event_id}) to {sink_url}")
    headers, body = to_structured(event)
    headers['X-Request-ID'] = event_id
    
    for attempt in range(retries):
        try:
            timeout = (min(5, 3 + attempt), min(15, 10 + attempt*2))
            
            response = requests.post(
                sink_url,
                headers=headers,
                data=body,
                timeout=timeout
            )
            
            if 200 <= response.status_code < 300:
                logger.info(f"Successfully sent event {event_type} (ID: {event_id})")
                return True
            else:
                logger.warning(f"Event delivery failed: status={response.status_code}, attempt={attempt+1}/{retries}")
                if response.status_code == 400:
                    logger.error(f"Bad request error: {response.text}")
                    return False
                    
                if attempt < retries - 1:
                    # Exponential backoff with jitter
                    delay = min(base_delay * (2 ** attempt) * (0.75 + 0.5 * random.random()), max_delay)
                    logger.info(f"Retrying in {delay:.2f} seconds")
                    time.sleep(delay)
                else:
                    return False
        except Exception as e:
            error_type = type(e).__name__
            logger.error(f"Error sending event: [{error_type}] {str(e)}")
            if attempt < retries - 1:
                delay = min(base_delay * (2 ** attempt) * (0.75 + 0.5 * random.random()), max_delay)
                logger.info(f"Retrying in {delay:.2f} seconds")
                time.sleep(delay)
            else:
                return False
    
    return False

def send_error_event(error_msg, error_type, request_id, config):
    """Send an error event to the broker"""
    error_data = {
        "error": error_msg,
        "error_type": error_type,
        "request_id": request_id,
        "timestamp": int(time.time())
    }
    
    error_event = create_cloud_event(
        "eo.processing.error",
        error_data,
        config['event_source']
    )
    
    sink_url = config.get('sink_url')
    if sink_url:
        send_event(error_event, sink_url)
    else:
        logger.error(f"Cannot send error event - no sink URL: {error_msg}")

def handle_search_requested(event_data, config):
    request_id = event_data.get('request_id', str(uuid.uuid4()))
    
    try:
        logger.info(f"Processing search request: {request_id}")
        
        bbox = event_data.get('bbox')
        time_range = event_data.get('time_range')
        cloud_cover = event_data.get('cloud_cover', config['max_cloud_cover'])
        max_items = event_data.get('max_items', config['max_items'])
        
        if not bbox or not time_range:
            logger.error("Missing required parameters (bbox, time_range)")
            send_error_event("Missing required parameters", "ValidationError", request_id, config)
            return False
        
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
            
            # Send completion event even if no scenes found
            sink_url = config.get('sink_url')
            if sink_url:
                completion_data = {
                    "request_id": request_id,
                    "scenes_processed": 0,
                    "scenes_total": 0,
                    "success": True,
                    "reason": "No scenes matching criteria",
                    "timestamp": int(time.time())
                }
                
                completion_event = create_cloud_event(
                    "eo.search.completed",
                    completion_data,
                    config['event_source']
                )
                
                send_event(completion_event, sink_url)
            
            return True
        
        sink_url = config.get('sink_url')
        if not sink_url:
            logger.error("No K_SINK environment variable found. Cannot send events.")
            return False
        
        scenes_processed = 0
        scenes_total = len(items)
        
        # Process each scene sequentially to avoid overwhelming the system
        for item in items:
            try:
                scene_id = item.id
                collection_id = item.collection_id
                acquisition_date = item.datetime.strftime('%Y-%m-%d') if item.datetime else None
                cloud_cover = item.properties.get('eo:cloud_cover', 'N/A')
                
                logger.info(f"Processing scene: {scene_id} from {collection_id}, date: {acquisition_date}, cloud cover: {cloud_cover}%")
                
                band_assets = [f"B{i:02d}" for i in range(1, 13)] + ["B8A", "SCL"]
                assets_to_ingest = [band for band in band_assets if band in item.assets]
                
                if not assets_to_ingest:
                    logger.warning(f"No assets to ingest for scene {scene_id}")
                    continue
                
                # Send registration event first
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
                
                # Send and wait a bit to ensure the completion tracker has received it
                if send_event(registration_event, sink_url):
                    time.sleep(1)  # Small delay to ensure registration is processed
                    
                    # Now process assets one by one
                    for asset_id in assets_to_ingest:
                        try:
                            result = download_and_store_asset(
                                minio_client,
                                item, 
                                asset_id, 
                                request_id,
                                config
                            )
                            
                            if result:
                                # Send the ingestion event for each asset
                                ingestion_event = create_cloud_event(
                                    "eo.asset.ingested",
                                    result,
                                    config['event_source']
                                )
                                
                                logger.info(f"Sending ingestion event for asset {asset_id}, scene {scene_id}")
                                send_event(ingestion_event, sink_url)
                                # Small delay between asset events
                                time.sleep(0.2)
                        except Exception as e:
                            logger.error(f"Error processing asset {asset_id} for scene {scene_id}: {str(e)}")
                    
                    scenes_processed += 1
                else:
                    logger.error(f"Failed to send registration event for scene {scene_id}")
                
            except Exception as e:
                logger.error(f"Error processing scene {item.id}: {str(e)}")
        
        # Always send a completion event
        completion_data = {
            "request_id": request_id,
            "scenes_processed": scenes_processed,
            "scenes_total": scenes_total,
            "success": scenes_processed > 0,
            "timestamp": int(time.time())
        }
        
        completion_event = create_cloud_event(
            "eo.search.completed",
            completion_data,
            config['event_source']
        )
        
        send_event(completion_event, sink_url)
        
        return scenes_processed > 0
        
    except Exception as e:
        logger.exception(f"Error handling search request: {str(e)}")
        
        # Always send error event
        error_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "request_id": request_id,
            "timestamp": int(time.time())
        }
        
        error_event = create_cloud_event(
            "eo.processing.error",
            error_data,
            config['event_source']
        )
        
        sink_url = config.get('sink_url')
        if sink_url:
            send_event(error_event, sink_url)
        
        return False
    
def download_and_store_asset(minio_client, item, asset_id, request_id, config):
    """Process a single asset with download and storage in a single function"""
    try:
        scene_id = item.id
        collection_id = item.collection_id
        
        logger.info(f"Processing asset {asset_id} for item: {scene_id}, request: {request_id}")
        
        if asset_id not in item.assets:
            logger.warning(f"Asset {asset_id} not found in item {scene_id}")
            return None
        
        asset = item.assets[asset_id]
        max_retries = config['max_retries']
        base_delay = 2
        
        # Download the asset
        for attempt in range(max_retries):
            try:
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
                        "item_id": scene_id,
                        "collection": collection_id,
                        "asset_id": asset_id,
                        "content_type": asset.media_type,
                        "original_href": asset.href,
                        "timestamp": str(int(time.time())),
                        "bbox": json.dumps(item.bbox),
                        "datetime": item.datetime.isoformat() if item.datetime else None,
                        "request_id": request_id
                    }
                    
                    # Store the asset
                    collection = collection_id
                    object_name = f"{collection}/{scene_id}/{asset_id}"
                    content_type = metadata.get("content_type", "application/octet-stream")
                    
                    minio_client.put_object(
                        config['raw_bucket'],
                        object_name,
                        io.BytesIO(data),
                        length=len(data),
                        content_type=content_type,
                        metadata=metadata
                    )
                    
                    logger.info(f"Stored asset {asset_id} from item {scene_id} in bucket {config['raw_bucket']}")
                    
                    result = {
                        "bucket": config['raw_bucket'],
                        "object_name": object_name,
                        "item_id": scene_id,
                        "asset_id": asset_id,
                        "collection": collection,
                        "size": len(data),
                        "content_type": content_type,
                        "request_id": request_id
                    }
                    
                    return result
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
        
    except Exception as e:
        logger.error(f"Error processing asset {asset_id}: {str(e)}")
        raise
               
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    return {"status": "up"}

@app.route('/', methods=['POST'])
def handle_event():
    """Handle direct requests from webhook or CloudEvents from broker"""
    config = get_config()
    
    try:
        # Check for CloudEvent headers first
        ce_type = request.headers.get('ce-type')
        ce_source = request.headers.get('ce-source')
        ce_id = request.headers.get('ce-id')
        
        if ce_type and ce_source and ce_id:
            logger.info(f"Received CloudEvent via headers: {ce_type} from {ce_source} (ID: {ce_id})")
            
            # Parse the request body as the event data
            event_data = request.get_json(silent=True) or {}
            
            if ce_type == 'eo.search.requested':
                logger.info(f"Processing search request event: {event_data.get('request_id', 'unknown')}")
                
                thread = threading.Thread(
                    target=handle_search_requested,
                    args=(event_data, config)
                )
                thread.daemon = True
                thread.start()
                
                return Response(status=202)  # Accepted
                
            return Response(status=202)  # We accept all CloudEvents
            
        # If no CE headers, try to parse as CloudEvent
        try:
            cloud_event = from_http(request.headers, request.get_data())
            event_type = cloud_event['type']
            
            logger.info(f"Received CloudEvent object: {event_type}")
            
            event_data = cloud_event.data
            if isinstance(event_data, str):
                try:
                    event_data = json.loads(event_data)
                except:
                    logger.warning("Could not parse event data as JSON")
            
            if event_type == 'eo.search.requested':
                logger.info(f"Processing search request event: {event_data.get('request_id', 'unknown')}")
                
                thread = threading.Thread(
                    target=handle_search_requested,
                    args=(event_data, config)
                )
                thread.daemon = True
                thread.start()
                
                return Response(status=202)  # Accepted
            
            return Response(status=202)  # We accept all CloudEvents
            
        except Exception as ce_error:
            logger.warning(f"Failed to parse as CloudEvent: {str(ce_error)}")
        
        # Finally, try to handle as direct JSON
        try:
            request_data = request.get_json(silent=True)
            
            if isinstance(request_data, dict) and 'request_id' in request_data and 'bbox' in request_data:
                logger.info(f"Received direct request from webhook, request_id: {request_data.get('request_id')}")
                
                thread = threading.Thread(
                    target=handle_search_requested,
                    args=(request_data, config)
                )
                thread.daemon = True
                thread.start()
                
                return Response(status=202)  # Accepted
        except Exception as json_err:
            logger.warning(f"Failed to parse as direct JSON request: {str(json_err)}")
            
        logger.warning(f"Unrecognized request format")
        return Response(status=400)  # Bad request
        
    except Exception as e:
        logger.exception(f"Error processing request: {str(e)}")
        return Response(status=500)  # Internal server error
    
if __name__ == '__main__':
    config = get_config()
    logger.info(f"Starting ingestion container source with:")
    logger.info(f"  MINIO_ENDPOINT: {config['minio_endpoint']}")
    logger.info(f"  RAW_BUCKET: {config['raw_bucket']}")
    logger.info(f"  MAX_ITEMS: {config['max_items']}")
    logger.info(f"  K_SINK: {config.get('sink_url', '(not set)')}")
    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)