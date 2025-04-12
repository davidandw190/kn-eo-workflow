from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
import os
import time
import logging
import json
import uuid
import io
import tempfile
from datetime import datetime, timezone
import rasterio
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config():
    """Get configuration from environment variables"""
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-assets'),
        'cog_bucket': os.getenv('COG_BUCKET', 'cog-assets'),
        'fmask_raw_bucket': os.getenv('FMASK_RAW_BUCKET', 'fmask-raw'),
        'fmask_cog_bucket': os.getenv('FMASK_COG_BUCKET', 'fmask-cog'),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5')),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/cog-transformer'),
    }

def initialize_minio_client(config):
    """Initialize MinIO client with retries"""
    max_retries = config['max_retries']
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Initializing MinIO client with endpoint: {config['minio_endpoint']}")
            
            client = Minio(
                config['minio_endpoint'],
                access_key=config['minio_access_key'],
                secret_key=config['minio_secret_key'],
                secure=config['secure_connection']
            )
            
            # Test connection
            client.list_buckets()
            logger.info("Successfully connected to MinIO")
            return client
            
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to initialize MinIO client after {max_retries} attempts: {str(e)}")
                raise
            logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
            time.sleep(config['connection_timeout'])

def create_cloud_event(event_type, data, source):
    """Create a CloudEvent with the specified attributes"""
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

def ensure_bucket(minio_client, bucket, config):
    """Ensure the specified bucket exists"""
    max_retries = config['max_retries']
    
    for attempt in range(max_retries):
        try:
            if not minio_client.bucket_exists(bucket):
                logger.info(f"Creating bucket: {bucket}")
                minio_client.make_bucket(bucket)
                logger.info(f"Successfully created bucket: {bucket}")
            
            logger.info(f"Bucket {bucket} exists and is ready")
            return True
            
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to ensure bucket '{bucket}' exists after {max_retries} attempts: {str(e)}")
                raise
            logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
            time.sleep(config['connection_timeout'])

def download_asset(minio_client, bucket, object_name, config):
    """Download an asset from MinIO with retries"""
    max_retries = config['max_retries']
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading asset from bucket: {bucket}, object: {object_name}")
            
            response = minio_client.get_object(bucket, object_name)
            data = response.read()
            
            try:
                metadata = minio_client.stat_object(bucket, object_name).metadata
            except:
                metadata = {}
                
            response.close()
            response.release_conn()
            
            logger.info(f"Successfully downloaded asset: {object_name}")
            return data, metadata
            
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to download asset after {max_retries} attempts: {str(e)}")
                raise
            logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}")
            time.sleep(config['connection_timeout'])


def convert_to_cog(input_data, profile="deflate"):
    """Convert raster data to Cloud Optimized GeoTIFF format with better error handling"""
    logger.info("Converting data to Cloud Optimized GeoTIFF format")
    
    with tempfile.NamedTemporaryFile(suffix='.tif') as tmp_src:
        tmp_src.write(input_data)
        tmp_src.flush()
        
        # Check if the input file is valid
        try:
            with rasterio.open(tmp_src.name) as src:
                # Get basic info to verify the file is readable
                width = src.width
                height = src.height
                dtype = src.dtypes[0]
                logger.info(f"Input raster dimensions: {width}x{height}, type: {dtype}")
                
                # For higher bit-depth data, adjust compression
                if dtype in ['uint16', 'int16', 'float32']:
                    output_profile = cog_profiles.get("deflate")
                    output_profile["predictor"] = 2
                else:
                    output_profile = cog_profiles.get(profile)
                
                output_profile["blocksize"] = 512
        except Exception as e:
            logger.error(f"Invalid input raster file: {str(e)}")
            raise RuntimeError(f"Invalid input raster: {str(e)}")
        
        with tempfile.NamedTemporaryFile(suffix='.tif') as tmp_dst:
            # Convert to COG
            try:
                cog_translate(
                    tmp_src.name,
                    tmp_dst.name,
                    output_profile,
                    quiet=True
                )
                
                logger.info("COG conversion successful")
                
                with open(tmp_dst.name, 'rb') as f:
                    cog_data = f.read()
                
                return cog_data
                
            except Exception as e:
                logger.error(f"Error in COG conversion: {str(e)}")
                raise RuntimeError(f"COG conversion failed: {str(e)}")
            
def store_cog(minio_client, bucket, object_name, data, metadata, config):
    """Store the COG in MinIO with retries"""
    max_retries = config['max_retries']
    
    for attempt in range(max_retries):
        try:
            minio_client.put_object(
                bucket,
                object_name,
                io.BytesIO(data),
                length=len(data),
                content_type="image/tiff",
                metadata=metadata
            )
            
            logger.info(f"Successfully stored COG in bucket {bucket}, object: {object_name}")
            return True
            
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to store COG after {max_retries} attempts: {str(e)}")
                raise
            logger.warning(f"Storage attempt {attempt + 1} failed: {str(e)}")
            time.sleep(config['connection_timeout'])

def main(context: Context):
    """Main function handler for processing COG transformations"""
    try:
        logger.info("COG Transformer function activated")
        
        # Get configuration
        config = get_config()
        
        # Check if we have a cloud event
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        # Extract event data
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        event_type = context.cloud_event["type"]
        
        # Determine source and target buckets based on event type
        is_fmask = event_type == "eo.fmask.completed"
        
        if is_fmask:
            source_bucket = config['fmask_raw_bucket']
            target_bucket = config['fmask_cog_bucket']
            response_event_type = "eo.fmask.transformed"
        else:
            source_bucket = config['raw_bucket']
            target_bucket = config['cog_bucket']
            response_event_type = "eo.asset.transformed"
        
        # Extract required fields from event
        bucket = event_data.get("bucket", source_bucket)
        object_name = event_data.get("object_name")
        item_id = event_data.get("item_id")
        asset_id = event_data.get("asset_id")
        request_id = event_data.get("request_id", str(uuid.uuid4()))
        collection = event_data.get("collection", "unknown")
        
        # Validate required fields
        if not object_name:
            error_msg = "Missing required parameter: object_name"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        if not item_id:
            error_msg = "Missing required parameter: item_id"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        logger.info(f"Processing asset: {object_name} from bucket: {bucket}")
        
        # Initialize MinIO client
        minio_client = initialize_minio_client(config)
        
        # Ensure target bucket exists
        ensure_bucket(minio_client, target_bucket, config)
        
        # Download the asset
        asset_data, metadata = download_asset(minio_client, bucket, object_name, config)
        
        # Convert to COG
        cog_data = convert_to_cog(asset_data)
        
        # Store the COG
        store_cog(minio_client, target_bucket, object_name, cog_data, metadata, config)
        
        # If asset_id is not provided, try to extract from object_name
        if not asset_id:
            parts = object_name.split("/")
            if len(parts) >= 3:
                asset_id = parts[-1]
            else:
                asset_id = "unknown"
        
        # Prepare result data
        result = {
            "source_bucket": bucket,
            "source_object": object_name,
            "cog_bucket": target_bucket,
            "cog_object": object_name,
            "size": len(cog_data),
            "item_id": item_id,
            "asset_id": asset_id,
            "collection": collection,
            "request_id": request_id,
            "is_fmask": is_fmask
        }
        
        # Create response event
        response_event = create_cloud_event(
            response_event_type,
            result,
            config['event_source']
        )
        
        logger.info(f"Successfully transformed asset to COG: {object_name}")
        return response_event
        
    except Exception as e:
        logger.exception(f"Error in COG transformer function: {str(e)}")
        
        # Create an error event
        error_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "bucket": event_data.get("bucket") if "event_data" in locals() else None,
            "object_name": event_data.get("object_name") if "event_data" in locals() else None,
            "request_id": event_data.get("request_id") if "event_data" in locals() else str(uuid.uuid4())
        }
        
        error_event = create_cloud_event(
            "eo.processing.error",
            error_data,
            config['event_source']
        )
        
        return error_event