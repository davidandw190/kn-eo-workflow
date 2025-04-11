# Create debug-events/func.py
from parliament import Context
import os
import time
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main(context: Context):
    logger.info("Debug-Events function activated")
    
    # Log the raw event data
    if hasattr(context, 'cloud_event'):
        event_type = context.cloud_event["type"]
        event_source = context.cloud_event["source"]
        event_id = context.cloud_event["id"]
        
        logger.info(f"Received CloudEvent:")
        logger.info(f"  Type: {event_type}")
        logger.info(f"  Source: {event_source}")
        logger.info(f"  ID: {event_id}")
        
        # Log the data payload
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            try:
                event_data = json.loads(event_data)
            except:
                pass
        
        logger.info(f"  Data: {json.dumps(event_data, indent=2)}")
        
        # Return success
        return {
            "status": "event_received",
            "event_type": event_type,
            "event_id": event_id
        }, 200
    else:
        logger.warning("No CloudEvent in context")
        return {"error": "No CloudEvent in context"}, 400