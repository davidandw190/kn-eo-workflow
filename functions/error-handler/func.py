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
    try:
        logger.info("Error Handler function activated")
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            try:
                event_data = json.loads(event_data)
            except:
                pass
        
        event_type = context.cloud_event["type"]
        event_source = context.cloud_event["source"]
        event_id = context.cloud_event["id"]
        
        logger.info(f"Processing error event:")
        logger.info(f"  Type: {event_type}")
        logger.info(f"  Source: {event_source}")
        logger.info(f"  ID: {event_id}")
        
        if isinstance(event_data, dict):
            error_type = event_data.get("error_type", "Unknown")
            error_msg = event_data.get("error", "No error details")
            request_id = event_data.get("request_id", "unknown")
            
            logger.error(f"Error in request {request_id}: [{error_type}] {error_msg}")

            if "item_id" in event_data:
                logger.error(f"Related to item: {event_data['item_id']}")
            if "asset_id" in event_data:
                logger.error(f"Related to asset: {event_data['asset_id']}")
            if "bucket" in event_data:
                logger.error(f"Related to bucket: {event_data['bucket']}")
                
        else:
            logger.error(f"Error event data: {event_data}")
        
        return {
            "status": "error_processed",
            "timestamp": int(time.time())
        }, 200
        
    except Exception as e:
        logger.exception(f"Error in error handler: {str(e)}")
        return {"error": str(e)}, 500