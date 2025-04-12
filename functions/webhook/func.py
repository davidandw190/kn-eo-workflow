from parliament import Context
import os
import time
import logging
import json
import uuid
import requests
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        
        ingestion_url = "http://ingestion-service.eo-workflow.svc.cluster.local"
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Sending request to ingestion service (attempt {attempt+1}/{max_retries})")
                response = requests.post(
                    ingestion_url, 
                    json=request_data, 
                    headers={"Content-Type": "application/json"},
                    timeout=10
                )
                
                if response.status_code >= 200 and response.status_code < 300:
                    logger.info(f"Successfully sent request. Status: {response.status_code}")
                    return {"status": "success", "message": "Search request submitted", "request_id": request_id}, 202
                else:
                    logger.warning(f"Request failed with status: {response.status_code}, Response: {response.text}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        return {"error": f"Failed to submit request: HTTP {response.status_code}"}, 500
            except Exception as e:
                logger.warning(f"Request attempt {attempt+1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    return {"error": f"Failed to submit request after {max_retries} attempts: {str(e)}"}, 500
    
    except Exception as e:
        logger.exception(f"Error in webhook handler: {str(e)}")
        return {"error": "Server error processing request"}, 500