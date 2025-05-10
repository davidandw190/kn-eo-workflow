from parliament import Context
import os
import time
import logging
import uuid
from cloudevents.http import CloudEvent, to_structured
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_cloud_event(event_type, data, source):
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
        
        search_data = {
            "bbox": bbox,
            "time_range": time_range,
            "cloud_cover": cloud_cover,
            "max_items": max_items,
            "request_id": request_id,
            "timestamp": int(time.time())
        }
        
        search_event = create_cloud_event(
            "eo.search.requested",
            search_data,
            "eo-workflow/webhook"
        )
        
        logger.info(f"Created search request event with ID: {request_id}")
        sink_url = os.getenv("K_SINK", "http://broker-ingress.knative-eventing.svc.cluster.local/eo-workflow/eo-event-broker")
        headers, body = to_structured(search_event)
        
        import requests
        response = requests.post(sink_url, headers=headers, data=body)
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Successfully sent event to broker, status: {response.status_code}")
        else:
            logger.warning(f"Failed to send event to broker, status: {response.status_code}")
        
        return {"status": "success", "request_id": request_id}, 202
        
    except Exception as e:
        logger.exception(f"Error in webhook handler: {str(e)}")
        return {"error": "Server error processing request"}, 500