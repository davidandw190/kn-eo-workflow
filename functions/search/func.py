from parliament import Context
from cloudevents.http import CloudEvent
import os
import time
import logging
import json
import uuid
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
        'catalog_url': os.getenv('STAC_CATALOG_URL', 'https://planetarycomputer.microsoft.com/api/stac/v1'),
        'collection': os.getenv('STAC_COLLECTION', 'sentinel-2-l2a'),
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/search')
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

def search_scenes(config, bbox, time_range, cloud_cover, max_items):
    logger.info(f"Searching for {config['collection']} scenes in bbox: {bbox}, timerange: {time_range}, max_items: {max_items}")
    
    client = Client.open(
        config['catalog_url'],
        modifier=planetary_computer.sign_inplace
    )
    
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

def main(context: Context):
    try:
        logger.info("Search function activated")
        config = get_config()
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        request_id = event_data.get('request_id', str(uuid.uuid4()))
        bbox = event_data.get('bbox')
        time_range = event_data.get('time_range')
        cloud_cover = event_data.get('cloud_cover', 20)
        max_items = event_data.get('max_items', 1)
        
        logger.info(f"Processing search request {request_id}")
        
        if not bbox or not time_range:
            error_msg = "Missing required parameters: bbox, time_range"
            logger.error(error_msg)
            
            error_data = {
                "request_id": request_id,
                "error": error_msg,
                "error_type": "InvalidParameters",
                "timestamp": int(time.time())
            }
            
            error_event = create_cloud_event(
                "eo.processing.error",
                error_data,
                config['event_source']
            )
            
            return error_event
            
        # search for scenes
        try:
            items = search_scenes(config, bbox, time_range, cloud_cover, max_items)
            scenes_found = len(items)
            
            # response events for each scene found
            scene_events = []
            
            for item in items:
                scene_id = item.id
                collection_id = item.collection_id
                acquisition_date = item.datetime.strftime('%Y-%m-%d') if item.datetime else None
                cloud_cover_pct = item.properties.get('eo:cloud_cover', 'N/A')
                
                # find available band assets
                band_assets = [f"B{i:02d}" for i in range(1, 13)] + ["B8A", "SCL"]
                available_assets = [band for band in band_assets if band in item.assets]
                
                if not available_assets:
                    logger.warning(f"No relevant assets found for scene {scene_id}")
                    continue
                
                scene_data = {
                    "request_id": request_id,
                    "item_id": scene_id,
                    "collection": collection_id,
                    "assets": available_assets,
                    "bbox": bbox,
                    "acquisition_date": acquisition_date,
                    "cloud_cover": cloud_cover_pct
                }
                
                scene_event = create_cloud_event(
                    "eo.scene.discovered",
                    scene_data,
                    config['event_source']
                )
                
                scene_events.append(scene_event)
                logger.info(f"Created scene discovered event for {scene_id}")
            
            completion_data = {
                "request_id": request_id,
                "scenes_found": scenes_found,
                "scenes_processed": len(scene_events),
                "success": len(scene_events) > 0,
                "timestamp": int(time.time())
            }
            
            completion_event = create_cloud_event(
                "eo.search.completed",
                completion_data,
                config['event_source']
            )
            
            if scene_events:
                # we return the scene discovered event first
                # and DO NOT return the completion event until all scenes are processed
                first_scene = scene_events[0]
                logger.info(f"Returning scene discovered event to broker for item {first_scene.data['item_id']}")
                return first_scene
            else:
                logger.info(f"No scenes found, returning completion event")
                return completion_event
                
        except Exception as e:
            logger.exception(f"Error in search processing: {str(e)}")
            
            error_data = {
                "request_id": request_id,
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
            
    except Exception as e:
        logger.exception(f"Unhandled error in search function: {str(e)}")
        
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