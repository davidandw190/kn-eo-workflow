from parliament import Context
from cloudevents.http import CloudEvent
import os
import time
import logging
import json
import uuid
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config():
    return {
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/asset-scheduler'),
        'max_parallel_assets': int(os.getenv('MAX_PARALLEL_ASSETS', '3'))
    }

def create_cloud_event(event_type, data, source):
    """Create a CloudEvent with standard attributes"""
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

def main(context: Context):
    """Schedule assets for processing from registered scenes"""
    try:
        logger.info("Asset Scheduler function activated")
        config = get_config()
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        event_type = context.cloud_event["type"]
        
        if event_type != "eo.scene.assets.registered":
            error_msg = f"Unexpected event type: {event_type}"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        request_id = event_data.get("request_id", str(uuid.uuid4()))
        item_id = event_data.get("item_id")
        collection = event_data.get("collection")
        assets = event_data.get("assets", [])
        
        if not item_id or not collection or not assets:
            error_msg = "Missing required data in event"
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
        
        item_data = {
            "id": item_id,
            "collection": collection,
            "assets": {}
        }
        
        for field in ["bbox", "acquisition_date", "cloud_cover"]:
            if field in event_data:
                item_data[field] = event_data[field]
        
        asset_events = []
        
        for i, asset_id in enumerate(assets[:config['max_parallel_assets']]):
            item_data["assets"][asset_id] = {
                "href": f"planetarycomputer:sentinel-2-l2a/{item_id}/{asset_id}"
            }
            
            asset_event_data = {
                "request_id": request_id,
                "item": item_data,
                "asset_id": asset_id,
                "timestamp": int(time.time())
            }
            
            asset_event = create_cloud_event(
                "eo.asset.process",
                asset_event_data,
                config['event_source']
            )
            
            asset_events.append(asset_event)
        
        logger.info(f"Scheduled {len(asset_events)} assets for processing from scene {item_id}")
        
        if asset_events:
            return asset_events[0]
        else:
            error_msg = "No assets scheduled for processing"
            logger.error(error_msg)
            
            error_data = {
                "request_id": request_id,
                "item_id": item_id,
                "error": error_msg,
                "error_type": "NoAssetsScheduled",
                "timestamp": int(time.time())
            }
            
            error_event = create_cloud_event(
                "eo.processing.error",
                error_data,
                config['event_source']
            )
            
            return error_event
        
    except Exception as e:
        logger.exception(f"Error in asset scheduler: {str(e)}")
        
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