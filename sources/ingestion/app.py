import os
import time
import json
import uuid
import logging
import threading
import io
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import signal
import sys
from minio import Minio
import planetary_computer
from pystac_client import Client
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured
import aiohttp
import asyncio
import urllib3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        'sink_url': os.getenv('K_SINK'),
        'watch_interval': int(os.getenv('WATCH_INTERVAL', '60')),  # seconds
        'request_queue': os.getenv('REQUEST_QUEUE', 'requests')    # name of queue folder in MinIO
    }

class MinioClientManager:
    def __init__(self, config):
        self.config = config
        self.endpoint = config['minio_endpoint']
        self.access_key = config['minio_access_key']
        self.secret_key = config['minio_secret_key']
        self.max_retries = config['max_retries']
        self.secure = config['secure_connection']
        self.connection_timeout = config.get('connection_timeout', 5)

    def initialize_client(self):
        """Initialize MinIO client with better connection pool settings"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Initializing MinIO client with endpoint: {self.endpoint}")
                
                # Configure with larger connection pool and better retry logic
                client = Minio(
                    self.endpoint,
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                    secure=self.secure,
                    http_client=urllib3.PoolManager(
                        maxsize=20,  # Increased from default of 10
                        timeout=urllib3.Timeout(
                            connect=self.connection_timeout,
                            read=self.connection_timeout*3
                        ),
                        retries=urllib3.Retry(
                            total=5,
                            backoff_factor=0.2,
                            status_forcelist=[500, 502, 503, 504, 429]
                        )
                    )
                )
                
                client.list_buckets()
                logger.info("Successfully connected to MinIO")
                return client
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to initialize MinIO client after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
                time.sleep(self.connection_timeout)
    
    
def create_cloud_event(event_type, data, source):
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

class STACIngestionManager:
    def __init__(self, minio_client, config):
        self.minio_client = minio_client
        self.config = config
        self.raw_bucket = config['raw_bucket']
        self.request_queue = config['request_queue']
        self.connection_timeout = config.get('connection_timeout', 5)
        self.stac_client = Client.open(
            config['catalog_url'],
            modifier=planetary_computer.sign_inplace
        )
        self.max_workers = config['max_workers']

    def ensure_buckets(self):
        """Ensure the raw assets bucket and request queue bucket exist in MinIO"""
        for bucket in [self.raw_bucket, self.request_queue]:
            try:
                if not self.minio_client.bucket_exists(bucket):
                    logger.info(f"Creating bucket: {bucket}")
                    self.minio_client.make_bucket(bucket)
                    logger.info(f"Successfully created bucket: {bucket}")
                else:
                    logger.info(f"Bucket {bucket} already exists and is ready")
            except Exception as e:
                logger.error(f"Failed to ensure bucket '{bucket}' exists: {str(e)}")
                raise

    def search_scenes(self, bbox, time_range, cloud_cover=None, max_items=None):
        """Search for scenes with retry logic for API timeouts"""
        logger.info(f"Searching for {self.config['collection']} scenes in bbox: {bbox}, timerange: {time_range}")
        
        if cloud_cover is None:
            cloud_cover = self.config['max_cloud_cover']
        
        if max_items is None:
            max_items = self.config['max_items']
        
        search_params = {
            "collections": [self.config['collection']],
            "bbox": bbox,
            "datetime": time_range,
            "query": {
                "eo:cloud_cover": {"lt": cloud_cover},
                "s2:degraded_msi_data_percentage": {"lt": 5},
                "s2:nodata_pixel_percentage": {"lt": 10}
            },
            "limit": max_items,
            "max_items": max_items,
            "page_size": 10 
        }
        
        max_retries = 3
        base_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                search = self.stac_client.search(**search_params)
                items = list(search.get_items())
                
                if not items:
                    logger.warning("No scenes found for the specified parameters.")
                    return []
                
                logger.info(f"Found {len(items)} scene(s)")
                return items
                
            except Exception as e:
                # Check if this is a timeout error from the API
                is_timeout = "exceeded the maximum allowed time" in str(e)
                
                if attempt == max_retries - 1:
                    logger.error(f"Failed to search for scenes after {max_retries} attempts: {str(e)}")
                    raise
                    
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                
                if is_timeout:
                    logger.warning(f"STAC API timeout on attempt {attempt + 1}. Retrying in {delay} seconds...")
                else:
                    logger.warning(f"STAC search error on attempt {attempt + 1}: {str(e)}. Retrying in {delay} seconds...")
                    
                time.sleep(delay)

    def download_asset(self, item, asset_id):
        """Download a specific asset with improved error handling and token refresh"""
        if asset_id not in item.assets:
            raise ValueError(f"Asset {asset_id} not found in item {item.id}")
        
        asset = item.assets[asset_id]
        max_retries = self.config['max_retries']
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Re-sign URL on each attempt to ensure fresh tokens
                asset_href = planetary_computer.sign(asset.href)
                
                # Increased timeouts for larger assets
                timeout = (self.connection_timeout, self.connection_timeout * 5)
                
                # Use streaming to handle large assets better
                with requests.get(
                    asset_href, 
                    stream=True, 
                    timeout=timeout
                ) as response:
                    response.raise_for_status()
                    
                    # Read in 8MB chunks to avoid memory issues with large assets
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
                        "timestamp": int(time.time()),
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
                    
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s...")
                time.sleep(delay)
                
    def store_asset(self, item_id, asset_id, data, metadata):
        collection = metadata.get("collection", "unknown")
        object_name = f"{collection}/{item_id}/{asset_id}"
        
        content_type = metadata.get("content_type", "application/octet-stream")
        
        for attempt in range(self.config['max_retries']):
            try:
                self.minio_client.put_object(
                    self.raw_bucket,
                    object_name,
                    io.BytesIO(data),
                    length=len(data),
                    content_type=content_type,
                    metadata=metadata
                )
                
                logger.info(f"Stored asset {asset_id} from item {item_id} in bucket {self.raw_bucket}")
                
                return {
                    "bucket": self.raw_bucket,
                    "object_name": object_name,
                    "item_id": item_id,
                    "asset_id": asset_id,
                    "collection": collection,
                    "size": len(data),
                    "content_type": content_type
                }
            except Exception as e:
                if attempt == self.config['max_retries'] - 1:
                    logger.error(f"Failed to store asset {asset_id} after {self.config['max_retries']} attempts: {str(e)}")
                    raise
                logger.warning(f"Storage attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.connection_timeout)

    def process_single_asset(self, item, asset_id, request_id):
        try:
            logger.info(f"Processing asset {asset_id} for item {item.id}")    
            asset_data, asset_metadata = self.download_asset(item, asset_id)
            result = self.store_asset(item.id, asset_id, asset_data, asset_metadata)
            result["request_id"] = request_id
            
            return result
        except Exception as e:
            logger.error(f"Error processing asset {asset_id}: {str(e)}")
            raise

    def process_scene(self, item, request_id, bbox):
        """Process a single scene's assets and prepare events"""
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
            self.config['event_source']
        )
        
        processed_assets = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_asset = {
                executor.submit(self.process_single_asset, item, asset_id, request_id): asset_id
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

    def process_scenes(self, items, request_id, bbox):
        """Process multiple scenes and manage event emission"""
        results = []
        
        for item in items:
            try:
                # Process scene - returns registration event and processed assets
                registration_event, processed_assets = self.process_scene(item, request_id, bbox)
                
                if registration_event:
                    # Create asset ingestion events for each processed asset
                    asset_events = []
                    for asset_data in processed_assets:
                        asset_event = create_cloud_event(
                            "eo.asset.ingested",
                            asset_data,
                            self.config['event_source']
                        )
                        asset_events.append(asset_event)
                    
                    results.append({
                        "scene_id": item.id,
                        "registration_event": registration_event,
                        "asset_events": asset_events,
                        "processed_count": len(processed_assets)
                    })
            except Exception as e:
                logger.error(f"Error processing scene {item.id}: {str(e)}")
        
        return results

    async def send_events(self, events, sink_url):
        """Send multiple events to the sink asynchronously with batching"""
        if not sink_url:
            logger.error("No sink URL provided. Cannot send events.")
            return False
            
        logger.info(f"Sending {len(events)} events to sink: {sink_url}")
        
        # Batch events instead of trying to send them all at once
        BATCH_SIZE = 10
        success_count = 0
        
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(events), BATCH_SIZE):
                batch = events[i:i+BATCH_SIZE]
                tasks = []
                
                for event in batch:
                    try:
                        # Convert CloudEvent to HTTP request using non-deprecated method
                        headers, body = to_structured(event)
                        
                        # Create task for sending event
                        task = asyncio.create_task(
                            session.post(
                                sink_url,
                                headers=headers,
                                data=body,
                                timeout=15  # 15 second timeout for event sending
                            )
                        )
                        tasks.append(task)
                    except Exception as e:
                        logger.error(f"Error preparing event: {str(e)}")
                
                # Wait for all tasks to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Check results
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Failed to send event: {str(result)}")
                    elif result.status >= 200 and result.status < 300:
                        success_count += 1
                    else:
                        logger.error(f"Failed to send event, status: {result.status}")
                
                # Small delay between batches
                await asyncio.sleep(0.5)
            
        logger.info(f"Successfully sent {success_count} out of {len(events)} events")
        # Consider it a success if at least one event was sent successfully
        return success_count > 0

    def process_request(self, request):
        """Process a single EO request"""
        try:
            request_data = json.loads(request) if isinstance(request, str) else request
            
            # Extract request parameters
            bbox = request_data.get('bbox')
            time_range = request_data.get('time_range')
            cloud_cover = request_data.get('cloud_cover', self.config['max_cloud_cover'])
            max_items = request_data.get('max_items', self.config['max_items'])
            request_id = request_data.get('request_id', str(uuid.uuid4()))
            
            if not bbox or not time_range:
                logger.error("Missing required parameters (bbox, time_range)")
                return False
            
            # Search for scenes
            items = self.search_scenes(bbox, time_range, cloud_cover, max_items)
            
            if not items:
                logger.warning("No scenes found matching the search criteria")
                return False
            
            # CRITICAL FIX: Enforce max_items limit here
            if len(items) > max_items:
                logger.info(f"Limiting to {max_items} scenes out of {len(items)} found")
                items = items[:max_items]  # This limits the scenes to process
            
            # Process scenes
            scene_results = self.process_scenes(items, request_id, bbox)
            
            if not scene_results:
                logger.warning("No scenes were successfully processed")
                return False
            
            # Collect all events to send
            all_events = []
            for result in scene_results:
                all_events.append(result["registration_event"])
                all_events.extend(result["asset_events"])
            
            success = self.send_events_with_batching(all_events, self.config['sink_url'])
            
            return success
            
        except Exception as e:
            logger.exception(f"Error processing request: {str(e)}")
            
            # Create and send error event
            try:
                error_data = {
                    "request_id": request_data.get('request_id', 'unknown') if 'request_data' in locals() else 'unknown',
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "timestamp": int(time.time())
                }
                
                error_event = create_cloud_event(
                    "eo.processing.error",
                    error_data,
                    self.config['event_source']
                )
                
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.send_events([error_event], self.config['sink_url']))
            except Exception as event_error:
                logger.error(f"Error sending error event: {str(event_error)}")
                
            return False
        
        
    def send_events_with_batching(self, events, sink_url):
        """Send events with batching and better error handling"""
        if not sink_url:
            logger.error("No sink URL provided. Cannot send events.")
            return False
        
        if not events:
            logger.warning("No events to send.")
            return True
        
        logger.info(f"Sending {len(events)} events to sink: {sink_url}")
        
        # Smaller batch size to avoid overwhelming the broker
        BATCH_SIZE = 5
        success_count = 0
        
        # Process in batches
        for i in range(0, len(events), BATCH_SIZE):
            batch = events[i:i+BATCH_SIZE]
            batch_success = self._send_event_batch(batch, sink_url)
            
            if batch_success:
                success_count += len(batch)
            
            # Small delay between batches to allow broker to process
            time.sleep(1)
        
        logger.info(f"Successfully sent {success_count} out of {len(events)} events")
        
        # Consider it a success if at least some events were delivered
        return success_count > 0

    def _send_event_batch(self, events, sink_url):
        """Send a batch of events, with retries for the batch"""
        max_retries = 3
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Set up async event sending
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                # Send events and get results
                results = loop.run_until_complete(self._async_send_events(events, sink_url))
                loop.close()
                
                # Check success rate
                success_count = sum(1 for r in results if r is True)
                if success_count > 0:
                    return True
                    
                if attempt == max_retries - 1:
                    logger.error(f"Failed to send event batch after {max_retries} attempts")
                    return False
                    
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Event batch sending failed. Retrying in {delay}s...")
                time.sleep(delay)
                
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to send event batch after {max_retries} attempts: {str(e)}")
                    return False
                    
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Event batch sending error: {str(e)}. Retrying in {delay}s...")
                time.sleep(delay)
        
        return False

    async def _async_send_events(self, events, sink_url):
        """Send events asynchronously with individual timeout handling"""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for event in events:
                # Use updated conversion method
                from cloudevents.conversion import to_structured
                headers, body = to_structured(event)
                
                # Create task for sending event with increased timeout
                task = self._send_single_event(session, sink_url, headers, body)
                tasks.append(task)
            
            # Wait for all tasks with timeout handling
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            processed_results = []
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Failed to send event: {str(result)}")
                    processed_results.append(False)
                elif isinstance(result, bool):
                    processed_results.append(result)
                else:
                    processed_results.append(True)
            
            return processed_results

    async def _send_single_event(self, session, sink_url, headers, body):
        """Send a single event with proper error handling"""
        try:
            # Increased timeout for event sending
            timeout = aiohttp.ClientTimeout(total=30)
            
            async with session.post(
                sink_url,
                headers=headers,
                data=body,
                timeout=timeout
            ) as response:
                if response.status >= 200 and response.status < 300:
                    return True
                else:
                    logger.error(f"Event delivery failed with status: {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Error sending event: {str(e)}")
            return False    

    def mark_request_processing(self, request_object_name):
        """Mark a request as being processed to prevent duplicate processing"""
        lock_object_name = f"{request_object_name}.lock"
        
        try:
            # Try to create a lock file
            lock_content = json.dumps({
                "timestamp": int(time.time()),
                "process_id": os.getpid()
            }).encode('utf-8')
            
            try:
                # Check if lock already exists
                self.minio_client.stat_object(self.request_queue, lock_object_name)
                logger.info(f"Request {request_object_name} is already being processed")
                return False
            except Exception:
                # Lock doesn't exist, we can create it
                pass
                
            self.minio_client.put_object(
                self.request_queue,
                lock_object_name,
                io.BytesIO(lock_content),
                length=len(lock_content)
            )
            logger.info(f"Marked request {request_object_name} as being processed")
            return True
        except Exception as e:
            logger.error(f"Error marking request as processing: {str(e)}")
            return False

    def check_for_requests(self):
        """Check for request objects in the request queue bucket"""
        try:
            objects = list(self.minio_client.list_objects(self.request_queue))
            
            # Filter out lock files
            request_objects = [obj for obj in objects if not obj.object_name.endswith('.lock')]
            
            for obj in request_objects:
                try:
                    # Check if this request is already being processed
                    if not self.mark_request_processing(obj.object_name):
                        continue
                    
                    # Get the request object
                    response = self.minio_client.get_object(self.request_queue, obj.object_name)
                    request_data = json.loads(response.read().decode('utf-8'))
                    response.close()
                    
                    logger.info(f"Processing request: {obj.object_name}")
                    
                    # Process the request
                    success = self.process_request(request_data)
                    
                    # Remove the request object and lock if processed successfully
                    if success:
                        logger.info(f"Successfully processed request {obj.object_name}")
                        self.minio_client.remove_object(self.request_queue, obj.object_name)
                        self.minio_client.remove_object(self.request_queue, f"{obj.object_name}.lock")
                    else:
                        logger.warning(f"Failed to process request {obj.object_name}")
                        # Release lock if failed
                        self.minio_client.remove_object(self.request_queue, f"{obj.object_name}.lock")
                        
                except Exception as e:
                    logger.error(f"Error processing request {obj.object_name}: {str(e)}")
                    # Release the lock to allow retry
                    try:
                        self.minio_client.remove_object(self.request_queue, f"{obj.object_name}.lock")
                    except:
                        pass
        except Exception as e:
            logger.error(f"Error checking for requests: {str(e)}")
    
    def cleanup_stale_locks(self):
        """Clean up lock files that are older than a certain threshold"""
        try:
            objects = list(self.minio_client.list_objects(self.request_queue))
            
            # Find lock files
            lock_objects = [obj for obj in objects if obj.object_name.endswith('.lock')]
            
            current_time = int(time.time())
            lock_timeout = 3600  # 1 hour
            
            for lock_obj in lock_objects:
                try:
                    response = self.minio_client.get_object(self.request_queue, lock_obj.object_name)
                    lock_data = json.loads(response.read().decode('utf-8'))
                    response.close()
                    
                    if current_time - lock_data.get('timestamp', 0) > lock_timeout:
                        logger.info(f"Removing stale lock: {lock_obj.object_name}")
                        self.minio_client.remove_object(self.request_queue, lock_obj.object_name)
                except Exception as e:
                    logger.error(f"Error cleaning up lock {lock_obj.object_name}: {str(e)}")
        except Exception as e:
            logger.error(f"Error cleaning up stale locks: {str(e)}")

    def check_for_requests(self):
        """Check for request objects in the request queue bucket"""
        try:
            objects = list(self.minio_client.list_objects(self.request_queue))
            
            # Filter out lock files
            request_objects = [obj for obj in objects if not obj.object_name.endswith('.lock')]
            
            for obj in request_objects:
                try:
                    # Check if this request is already being processed
                    if not self.mark_request_processing(obj.object_name):
                        continue
                    
                    # Get the request object
                    response = self.minio_client.get_object(self.request_queue, obj.object_name)
                    request_data = json.loads(response.read().decode('utf-8'))
                    response.close()
                    
                    logger.info(f"Processing request: {obj.object_name}")
                    
                    # Process the request
                    success = self.process_request(request_data)
                    
                    # Remove the request object and lock if processed successfully
                    if success:
                        logger.info(f"Successfully processed request {obj.object_name}")
                        self.minio_client.remove_object(self.request_queue, obj.object_name)
                        self.minio_client.remove_object(self.request_queue, f"{obj.object_name}.lock")
                    else:
                        logger.warning(f"Failed to process request {obj.object_name}")
                        
                except Exception as e:
                    logger.error(f"Error processing request {obj.object_name}: {str(e)}")
                    # Release the lock to allow retry
                    try:
                        self.minio_client.remove_object(self.request_queue, f"{obj.object_name}.lock")
                    except:
                        pass
        except Exception as e:
            logger.error(f"Error checking for requests: {str(e)}")
            
def main():
    config = get_config()
    
    if not config['sink_url']:
        logger.error("No K_SINK environment variable found. This container must be run as a ContainerSource.")
        sys.exit(1)
    
    minio_manager = MinioClientManager(config)
    minio_client = minio_manager.initialize_client()
    
    ingestion_manager = STACIngestionManager(minio_client, config)
    
    ingestion_manager.ensure_buckets()
    
    stop_event = threading.Event()
    
    def signal_handler(sig, frame):
        logger.info("Shutdown signal received, stopping...")
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info(f"Starting STAC ingestion container, watching for requests in '{config['request_queue']}' bucket")
    
    last_health_check = time.time()
    
    while not stop_event.is_set():
        try:
            current_time = time.time()
            if current_time - last_health_check >= 300:  # Every 5 minutes
                logger.info("Performing health check and cleaning up stale locks")
                ingestion_manager.cleanup_stale_locks()
                last_health_check = current_time
                
            ingestion_manager.check_for_requests()
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
        
        # Shorter sleep intervals for more responsive checking
        for _ in range(min(10, config['watch_interval'])):
            if stop_event.is_set():
                break
            time.sleep(1)
    
    logger.info("STAC ingestion container shutting down")
    
if __name__ == "__main__":
    main()