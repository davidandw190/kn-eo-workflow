# functions/debug-events/func.py
from parliament import Context
import os
import time
import logging
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main(context: Context):
    logger.info("Debug-Events function activated")
    
    # Log the raw event data
    if hasattr(context, 'cloud_event'):
        # Extract key attributes directly rather than using .items()
        event_type = context.cloud_event["type"]
        event_source = context.cloud_event["source"]
        event_id = context.cloud_event["id"]
        
        logger.info(f"============================================")
        logger.info(f"Received CloudEvent at {datetime.now().isoformat()}:")
        logger.info(f"  Type: {event_type}")
        logger.info(f"  Source: {event_source}")
        logger.info(f"  ID: {event_id}")
        
        # List common CloudEvent attributes we might want to log
        for attr in ["time", "specversion", "subject", "datacontenttype"]:
            if attr in context.cloud_event:
                logger.info(f"  {attr}: {context.cloud_event[attr]}")
        
        # Log the data payload
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            try:
                event_data = json.loads(event_data)
            except:
                pass
        
        # Format for readability
        data_str = json.dumps(event_data, indent=2)
        logger.info(f"  Data: {data_str}")
        logger.info(f"============================================")
        
        # Return success
        return {
            "status": "event_received",
            "event_type": event_type,
            "event_id": event_id,
            "timestamp": int(time.time())
        }, 200
    else:
        logger.warning("No CloudEvent in context")
        # Log all request data to help debug
        if hasattr(context, 'request'):
            logger.info(f"Request method: {context.request.method}")
            logger.info(f"Request headers: {dict(context.request.headers)}")
            try:
                body = context.request.data.decode('utf-8')
                logger.info(f"Request body: {body}")
            except:
                logger.info("Could not decode request body")
        
        return {"error": "No CloudEvent in context"}, 400