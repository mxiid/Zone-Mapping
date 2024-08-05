import logging
import os 
from pipeline.data_ingestion import DataIngestionPipeline
from pipeline.regex_processing import RegexProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

STAGE_NAME = "DATA INGESTION"

if __name__ == "__main__":
    try:
        logger.info(f">>> STAGE {STAGE_NAME} STARTED <<<")
        obj = DataIngestionPipeline()
        obj.main()
        logger.info(f">>> STAGE {STAGE_NAME} COMPLETED <<<")

    except Exception as e:
        logger.error(f">>> STAGE {STAGE_NAME} FAILED <<<")
        logger.exception(e)
        raise e

STAGE_NAME = "REGEX PROCESSING"

if __name__ == "__main__":
    try:
        logger.info(f">>> STAGE {STAGE_NAME} STARTED <<<")
        obj = RegexProcessor()
        obj.main()
        logger.info(f">>> STAGE {STAGE_NAME} COMPLETED <<<")

    except Exception as e:
        logger.error(f">>> STAGE {STAGE_NAME} FAILED <<<")
        logger.exception(e)
        raise e