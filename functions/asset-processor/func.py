from parliament import Context
from cloudevents.http import CloudEvent
import os
import time
import logging
import json
import uuid
import io
import requests
from datetime import datetime, timezone
from minio import Minio
import planetary_computer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config():
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-assets'),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5')),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/asset-processor')
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
                secure=config['secure_connection']
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

def ensure_bucket(minio_client, bucket_name, config):
    max_retries = config['max_retries']
    
    for attempt in range(max_retries):
        try:
            if not minio_client.bucket_exists(bucket_name):
                logger.info(f"Creating bucket: {bucket_name}")
                minio_client.make_bucket(bucket_name)
                logger.info(f"Successfully created bucket: {bucket_name}")
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to ensure bucket exists after {max_retries} attempts: {str(e)}")
                raise
            logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
            time.sleep(config['connection_timeout'])

def download_asset(asset_href, config):
    max_retries = config['max_retries']
    base_delay = 2
    
    for attempt in range(max_retries):
        try:
            if 'planetarycomputer' in asset_href:
                asset_href = planetary_computer.sign(asset_href)
                
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
                        
                return data
                
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to download asset after {max_retries} attempts: {str(e)}")
                raise
                
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s...")
            time.sleep(delay)

def store_asset(minio_client, bucket, object_name, data, metadata, config):
    max_retries = config['max_retries']
    
    for attempt in range(max_retries):
        try:
            content_type = metadata.get('content_type', 'application/octet-stream')
            
            minio_client.put_object(
                bucket,
                object_name,
                io.BytesIO(data),
                length=len(data),
                content_type=content_type,
                metadata=metadata
            )
            
            logger.info(f"Successfully stored asset in bucket {bucket}, object: {object_name}")
            return True
            
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to store asset after {max_retries} attempts: {str(e)}")
                raise
            logger.warning(f"Storage attempt {attempt + 1} failed: {str(e)}")
            time.sleep(config['connection_timeout'])

def process_asset(stac_item, asset_id, request_id, config):
    logger.info(f"Processing asset {asset_id} for item {stac_item['id']}")
    
    if asset_id not in stac_item['assets']:
        raise ValueError(f"Asset {asset_id} not found in item {stac_item['id']}")
    
    asset = stac_item['assets'][asset_id]
    asset_href = asset['href']
    asset_data = download_asset(asset_href, config)
    metadata = {
        "item_id": stac_item['id'],
        "collection": stac_item['collection'],
        "asset_id": asset_id,
        "content_type": asset.get('type', 'application/octet-stream'),
        "original_href": asset_href,
        "timestamp": str(int(time.time())),
        "bbox": json.dumps(stac_item.get('bbox', [])),
        "datetime": stac_item.get('datetime', '')
    }
    
    minio_client = initialize_minio_client(config)
    ensure_bucket(minio_client, config['raw_bucket'], config)
    
    object_name = f"{stac_item['collection']}/{stac_item['id']}/{asset_id}"
    store_asset(minio_client, config['raw_bucket'], object_name, asset_data, metadata, config)
    
    return {
        "request_id": request_id,
        "item_id": stac_item['id'],
        "asset_id": asset_id,
        "collection": stac_item['collection'],
        "bucket": config['raw_bucket'],
        "object_name": object_name,
        "size": len(asset_data),
        "content_type": metadata['content_type']
    }

def main(context: Context):
    try:
        logger.info("Asset Processor function activated")
        config = get_config()
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        event_type = context.cloud_event['type']
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        request_id = event_data.get('request_id', str(uuid.uuid4()))
        
        if event_type == 'eo.scene.discovered':
            item_id = event_data.get('item_id')
            collection = event_data.get('collection')
            assets = event_data.get('assets', [])
            
            if not item_id or not collection or not assets:
                error_msg = "Missing required data in scene.discovered event"
                logger.error(error_msg)
                
                error_data = {
                    "request_id": request_id,
                    "error": error_msg,
                    "error_type": "InvalidEventData",
                    "timestamp": int(time.time())
                }
                
                error_event = create_cloud_event(
                    "eo.processing.error",
                    error_data,
                    config['event_source']
                )
                
                return error_event
            
            registration_data = {
                "request_id": request_id,
                "item_id": item_id,
                "collection": collection,
                "assets": assets,
                "bbox": event_data.get('bbox', []),
                "acquisition_date": event_data.get('acquisition_date', ''),
                "cloud_cover": event_data.get('cloud_cover', '')
            }
            
            registration_event = create_cloud_event(
                "eo.scene.assets.registered",
                registration_data,
                config['event_source']
            )
            
            logger.info(f"Registered scene {item_id} with {len(assets)} assets")
            return registration_event
            
        elif event_type == 'eo.asset.process':
            item_data = event_data.get('item')
            asset_id = event_data.get('asset_id')
            
            if not item_data or not asset_id:
                error_msg = "Missing required data in asset.process event"
                logger.error(error_msg)
                
                error_data = {
                    "request_id": request_id,
                    "error": error_msg,
                    "error_type": "InvalidEventData",
                    "timestamp": int(time.time())
                }
                
                error_event = create_cloud_event(
                    "eo.processing.error",
                    error_data,
                    config['event_source']
                )
                
                return error_event
            
            try:
                result = process_asset(item_data, asset_id, request_id, config)
                
                ingestion_event = create_cloud_event(
                    "eo.asset.ingested",
                    result,
                    config['event_source']
                )
                
                logger.info(f"Processed asset {asset_id} for item {item_data['id']}")
                return ingestion_event
                
            except Exception as e:
                logger.exception(f"Error processing asset {asset_id}: {str(e)}")
                
                error_data = {
                    "request_id": request_id,
                    "item_id": item_data.get('id'),
                    "asset_id": asset_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "timestamp": int(time.time())
                }
                
                error_event = create_cloud_event(
                    "eo.processing.error",
                    error_data,
                    config['event_source']
                )
                
                return error_event
                
        else:
            error_msg = f"Unsupported event type: {event_type}"
            logger.error(error_msg)
            
            error_data = {
                "request_id": request_id,
                "error": error_msg,
                "error_type": "UnsupportedEventType",
                "timestamp": int(time.time())
            }
            
            error_event = create_cloud_event(
                "eo.processing.error",
                error_data,
                config['event_source']
            )
            
            return error_event
            
    except Exception as e:
        logger.exception(f"Unhandled error in asset processor: {str(e)}")
        
        error_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "timestamp": int(time.time())
        }
        
        error_event = create_cloud_event(
            "eo.processing.error",
            error_data,
            config['event_source']
        )
        
        return error_event