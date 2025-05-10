from parliament import Context
from cloudevents.http import CloudEvent
import os
import time
import logging
import json
import uuid
import requests
from datetime import datetime, timezone
import planetary_computer
from pystac_client import Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config():
    return {
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/asset-scheduler'),
        'max_parallel_assets': int(os.getenv('MAX_PARALLEL_ASSETS', '3')),
        'catalog_url': os.getenv('STAC_CATALOG_URL', 'https://planetarycomputer.microsoft.com/api/stac/v1'),
        'collection': os.getenv('STAC_COLLECTION', 'sentinel-2-l2a')
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

def get_stac_item(item_id, collection, config):
    """Retrieve the full STAC item from the catalog"""
    try:
        client = Client.open(
            config['catalog_url'],
            modifier=planetary_computer.sign_inplace
        )
        
        # First search for the item to get its exact ID
        search = client.search(
            collections=[collection],
            ids=[item_id]
        )
        
        items = list(search.items())
        if not items:
            logger.warning(f"STAC item {item_id} not found in collection {collection}")
            return None
            
        # Convert item to dictionary with all necessary information
        item = items[0]
        item_dict = {
            "id": item.id,
            "collection": item.collection_id,
            "bbox": item.bbox,
            "datetime": item.datetime.isoformat() if item.datetime else None,
            "properties": item.properties,
            "assets": {}
        }
        
        # Include only the assets we need, with their proper URLs
        for asset_id, asset in item.assets.items():
            if asset_id.startswith('B') or asset_id == 'SCL':
                item_dict["assets"][asset_id] = {
                    "href": asset.href,
                    "type": asset.media_type
                }
        
        return item_dict
    
    except Exception as e:
        logger.error(f"Error retrieving STAC item {item_id}: {str(e)}")
        return None

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
        
        item_data = get_stac_item(item_id, collection, config)
        
        if not item_data:
            error_msg = f"Failed to retrieve STAC item {item_id} from collection {collection}"
            logger.error(error_msg)
            
            error_data = {
                "request_id": request_id,
                "item_id": item_id,
                "error": error_msg,
                "error_type": "StacItemNotFound",
                "timestamp": int(time.time())
            }
            
            error_event = create_cloud_event(
                "eo.processing.error",
                error_data,
                config['event_source']
            )
            
            return error_event
            
        available_assets = [asset_id for asset_id in assets if asset_id in item_data["assets"]]
        if not available_assets:
            error_msg = f"None of the requested assets {assets} are available for item {item_id}"
            logger.error(error_msg)
            
            error_data = {
                "request_id": request_id,
                "item_id": item_id,
                "error": error_msg,
                "error_type": "NoAssetsAvailable",
                "timestamp": int(time.time())
            }
            
            error_event = create_cloud_event(
                "eo.processing.error",
                error_data,
                config['event_source']
            )
            
            return error_event
        
        assets_to_process = available_assets[:config['max_parallel_assets']]
        
        asset_events = []
        
        for asset_id in assets_to_process:
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