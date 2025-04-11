from parliament import Context
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured
import os
import time
import logging
import json
import uuid
from datetime import datetime, timezone
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config():
    return {
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/webhook'),
        'sink_url': os.getenv('K_SINK')
    }

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
        
        config = get_config()
        
        if not hasattr(context, 'request'):
            return {"error": "No request in context"}, 400
        
        try:
            body = context.request.json if hasattr(context.request, 'json') else {}
        except Exception as e:
            return {"error": f"Invalid JSON: {str(e)}"}, 400
        
        bbox = body.get('bbox')
        time_range = body.get('time_range')
        
        if not bbox or not time_range:
            return {"error": "Missing required parameters: bbox, time_range"}, 400
            
        if not isinstance(bbox, list) or len(bbox) != 4:
            return {"error": "bbox must be a list of 4 numbers [west, south, east, north]"}, 400
            
        cloud_cover = body.get('cloud_cover', 20)
        max_items = body.get('max_items', 3)
        
        request_id = str(uuid.uuid4())
        
        request_data = {
            "bbox": bbox,
            "time_range": time_range,
            "cloud_cover": cloud_cover,
            "max_items": max_items,
            "request_id": request_id,
            "timestamp": int(time.time())
        }
        
        # Create a CloudEvent
        event = create_cloud_event(
            event_type="eo.search.requested",
            data=request_data,
            source=config['event_source']
        )
        
        # CHANGE: Direct delivery to broker if K_SINK is available
        sink_url = config.get('sink_url')
        if sink_url:
            try:
                # Convert CloudEvent to HTTP request
                headers, body = to_structured(event)
                
                # Send the event directly to the broker
                response = requests.post(
                    sink_url,
                    headers=headers,
                    data=body,
                    timeout=10
                )
                
                if 200 <= response.status_code < 300:
                    logger.info(f"Successfully sent event directly to broker: {event['type']} for request {request_id}")
                    # Return success to the client
                    return {
                        "status": "success",
                        "message": "Search request submitted successfully",
                        "request_id": request_id
                    }, 202
                else:
                    logger.error(f"Failed to send event to broker, status: {response.status_code}")
                    # Continue to return the event as a fallback
            except Exception as e:
                logger.error(f"Error sending event directly to broker: {str(e)}")
                # Continue to return the event as a fallback
        
        # Return the CloudEvent (standard Knative behavior)
        logger.info(f"Emitting eo.search.requested event with request_id: {request_id}")
        return event
        
    except Exception as e:
        logger.exception(f"Error in webhook handler: {str(e)}")
        return {"error": str(e)}, 500