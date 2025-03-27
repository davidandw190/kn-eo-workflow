import os
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting debug container")
    logger.info("Environment variables:")
    for key, value in os.environ.items():
        logger.info(f"  {key}={value}")
    
    # Keep the container running
    while True:
        logger.info("Debug container still running...")
        time.sleep(60)

if __name__ == "__main__":
    main()
