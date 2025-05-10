from parliament import Context
from cloudevents.http import CloudEvent
import os
import time
import logging
import json
import uuid
import redis
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config():
    return {
        'redis_host': os.getenv('REDIS_HOST', 'redis.eo-workflow.svc.cluster.local'),
        'redis_port': int(os.getenv('REDIS_PORT', '6379')),
        'redis_key_prefix': os.getenv('REDIS_KEY_PREFIX', 'eo:tracker:'),
        'redis_key_expiry': int(os.getenv('REDIS_KEY_EXPIRY', '86400')),  # 24 hours
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/completion-tracker'),
        'cog_bucket': os.getenv('COG_BUCKET', 'cog-assets')
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

class RedisTracker:
    def __init__(self, config):
        self.host = config['redis_host']
        self.port = config['redis_port']
        self.key_prefix = config['redis_key_prefix']
        self.key_expiry = config['redis_key_expiry']
        
        logger.info(f"Connecting to Redis at {self.host}:{self.port}")
        self.redis_client = redis.Redis(
            host=self.host,
            port=self.port,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            decode_responses=True
        )
        
        self.redis_client.ping()
        logger.info("Successfully connected to Redis")
    
    def _get_scene_key(self, request_id, item_id):
        """Get Redis key for a specific scene"""
        return f"{self.key_prefix}scene:{request_id}:{item_id}"
    
    def _get_processed_key(self, request_id, item_id):
        """Get Redis key for processed assets of a scene"""
        return f"{self._get_scene_key(request_id, item_id)}:processed"
    
    def _get_expected_key(self, request_id, item_id):
        """Get Redis key for expected assets of a scene"""
        return f"{self._get_scene_key(request_id, item_id)}:expected"
        
    def _get_complete_key(self, request_id, item_id):
        """Get Redis key for completion status of a scene"""
        return f"{self._get_scene_key(request_id, item_id)}:complete"
        
    def _get_metadata_key(self, request_id, item_id):
        """Get Redis key for metadata of a scene"""
        return f"{self._get_scene_key(request_id, item_id)}:metadata"
        
    def set_expected_assets(self, request_id, item_id, assets, metadata=None):
        """Set the expected assets for a scene and reset tracking"""
        try:
            expected_key = self._get_expected_key(request_id, item_id)
            processed_key = self._get_processed_key(request_id, item_id)
            complete_key = self._get_complete_key(request_id, item_id)
            metadata_key = self._get_metadata_key(request_id, item_id)
            
            with self.redis_client.pipeline() as pipe:
                pipe.delete(expected_key, processed_key, complete_key)
                
                if assets:
                    pipe.sadd(expected_key, *assets)
                
                pipe.expire(expected_key, self.key_expiry)
                pipe.expire(processed_key, self.key_expiry)
                pipe.set(complete_key, "0")
                pipe.expire(complete_key, self.key_expiry)
                
                if metadata:
                    pipe.delete(metadata_key)
                    pipe.hset(metadata_key, mapping=metadata)
                    pipe.expire(metadata_key, self.key_expiry)
                
                pipe.execute()
                
            logger.info(f"Set {len(assets)} expected assets for scene {item_id} (request {request_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error setting expected assets: {str(e)}")
            return False
    
    def record_asset(self, request_id, item_id, asset_id):
        """Record a processed asset"""
        try:
            processed_key = self._get_processed_key(request_id, item_id)
            
            self.redis_client.sadd(processed_key, asset_id)
            self.redis_client.expire(processed_key, self.key_expiry)
            
            logger.info(f"Recorded asset {asset_id} for scene {item_id} (request {request_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error recording asset {asset_id}: {str(e)}")
            return False
    
    def is_complete(self, request_id, item_id):
        """Check if all expected assets have been processed"""
        try:
            complete_key = self._get_complete_key(request_id, item_id)
            
            # Check if already marked as complete
            if self.redis_client.get(complete_key) == "1":
                logger.info(f"Scene {item_id} already marked as complete")
                return False  # Already processed, no need to send another event
            
            expected_key = self._get_expected_key(request_id, item_id)
            processed_key = self._get_processed_key(request_id, item_id)
            
            # Get the set of expected and processed assets
            expected_assets = self.redis_client.smembers(expected_key)
            processed_assets = self.redis_client.smembers(processed_key)
            
            if not expected_assets:
                logger.warning(f"No expected assets found for scene {item_id} (request {request_id})")
                return False
            
            # Check if all expected assets have been processed
            missing_assets = expected_assets - processed_assets
            is_complete = len(missing_assets) == 0
            
            if is_complete:
                logger.info(f"All {len(expected_assets)} assets have been processed for scene {item_id}")
                
                # Mark as complete to avoid sending duplicate events
                self.redis_client.set(complete_key, "1")
                self.redis_client.expire(complete_key, self.key_expiry)
                
                return True
            else:
                logger.info(f"Scene {item_id} not yet complete. Missing {len(missing_assets)} assets: {missing_assets}")
                return False
                
        except Exception as e:
            logger.error(f"Error checking completion status: {str(e)}")
            return False
    
    def get_scene_metadata(self, request_id, item_id):
        """Get stored metadata for a scene"""
        try:
            metadata_key = self._get_metadata_key(request_id, item_id)
            metadata = self.redis_client.hgetall(metadata_key)
            return metadata
        except Exception as e:
            logger.error(f"Error getting scene metadata: {str(e)}")
            return {}
    
    def get_processed_assets(self, request_id, item_id):
        """Get processed assets for a scene"""
        try:
            processed_key = self._get_processed_key(request_id, item_id)
            return list(self.redis_client.smembers(processed_key))
        except Exception as e:
            logger.error(f"Error getting processed assets: {str(e)}")
            return []

def main(context: Context):
    try:
        logger.info("Completion Tracker function activated")
        config = get_config()
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        event_type = context.cloud_event["type"]
        
        redis_tracker = RedisTracker(config)
        
        if event_type == "eo.scene.assets.registered":
            request_id = event_data.get("request_id", "unknown")
            item_id = event_data.get("item_id", "unknown")
            assets = event_data.get("assets", [])
            collection = event_data.get("collection", "unknown")
            
            if not assets:
                logger.warning(f"No assets to track for scene {item_id}")
                return {"status": "no_assets", "item_id": item_id, "request_id": request_id}, 200
            
            metadata = {
                "collection": collection,
                "bbox": json.dumps(event_data.get("bbox", [])),
                "acquisition_date": event_data.get("acquisition_date", ""),
                "cloud_cover": str(event_data.get("cloud_cover", "")),
                "asset_count": str(len(assets))
            }
            
            logger.info(f"Setting {len(assets)} expected assets for item: {item_id}, request: {request_id}")
            
            redis_tracker.set_expected_assets(request_id, item_id, assets, metadata)
            
            return {
                "status": "registered",
                "request_id": request_id,
                "item_id": item_id,
                "assets_count": len(assets)
            }, 200
            
        elif event_type == "eo.asset.transformed":
            request_id = event_data.get("request_id", "unknown")
            item_id = event_data.get("item_id", "unknown")
            asset_id = event_data.get("asset_id", "unknown")
            collection = event_data.get("collection", "unknown")
            
            logger.info(f"Recording transformed asset: {asset_id} for item: {item_id}, request: {request_id}")
            
            if not redis_tracker.record_asset(request_id, item_id, asset_id):
                logger.error(f"Failed to record asset {asset_id} in Redis")
                return {"status": "recording_failed", "asset_id": asset_id, "item_id": item_id}, 200
            
            if redis_tracker.is_complete(request_id, item_id):
                logger.info(f"All assets for item {item_id} have been processed!")
                
                processed_assets = redis_tracker.get_processed_assets(request_id, item_id)
                metadata = redis_tracker.get_scene_metadata(request_id, item_id)
                
                result = {
                    "request_id": request_id,
                    "item_id": item_id,
                    "collection": collection,
                    "assets": processed_assets,
                    "cog_bucket": config['cog_bucket'],
                    "timestamp": int(time.time())
                }
                
                if "bbox" in metadata:
                    try:
                        result["bbox"] = json.loads(metadata["bbox"])
                    except Exception as e:
                        logger.warning(f"Could not parse bbox from metadata: {str(e)}")
                
                if "acquisition_date" in metadata:
                    result["acquisition_date"] = metadata["acquisition_date"]
                
                scene_ready_event = create_cloud_event(
                    "eo.scene.ready",
                    result,
                    config['event_source']
                )
                
                logger.info(f"Emitting eo.scene.ready event for item {item_id}")
                return scene_ready_event
                
            else:
                return {
                    "status": "asset_recorded", 
                    "asset_id": asset_id,
                    "item_id": item_id,
                    "request_id": request_id
                }, 200
                
        else:
            logger.warning(f"Unknown event type: {event_type}")
            return {"error": f"Unknown event type: {event_type}"}, 400
        
    except Exception as e:
        logger.exception(f"Error in completion tracker function: {str(e)}")
        
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