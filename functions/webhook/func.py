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

def create_cloud_event(event_type, data, source):
    """Create a CloudEvent with the given parameters"""
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

def main(context: Context):
    try:
        logger.info("EO Webhook activated")
        
        if not hasattr(context, 'request'):
            logger.error("No request in context")
            return {"error": "Invalid request"}, 400
        
        try:
            body = context.request.json if hasattr(context.request, 'json') else {}
        except Exception as e:
            logger.error(f"Invalid JSON: {str(e)}")
            return {"error": f"Invalid JSON"}, 400
        
        bbox = body.get('bbox')
        time_range = body.get('time_range')
        
        if not bbox or not time_range:
            logger.error("Missing required parameters")
            return {"error": "Missing required parameters: bbox, time_range"}, 400
            
        if not isinstance(bbox, list) or len(bbox) != 4:
            logger.error("Invalid bbox format")
            return {"error": "bbox must be a list of 4 numbers [west, south, east, north]"}, 400
          
        cloud_cover = body.get('cloud_cover', 20)
        max_items = body.get('max_items', 1)
        
        request_id = str(uuid.uuid4())
        
        request_data = {
            "bbox": bbox,
            "time_range": time_range,
            "cloud_cover": cloud_cover,
            "max_items": max_items,
            "request_id": request_id,
            "timestamp": int(time.time())
        }
        
        create_cloud_event(
            event_type="eo.search.requested",
            data=request_data,
            source="eo-workflow/webhook"
        )
        
        logger.info(f"Emitting search request event with ID: {request_id}")
        
        return {
            "status": "success",
            "message": "Search request submitted",
            "request_id": request_id
        }, 202
        
    except Exception as e:
        logger.exception(f"Error in webhook handler: {str(e)}")
        return {"error": "Server error processing request"}, 500