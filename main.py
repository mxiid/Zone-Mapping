import logging
import os 
from pipeline.data_ingestion import DataIngestionPipeline
from pipeline.regex_processing import RegexProcessingPipeline
from pipeline.api_processing import APIGeocodingPipeline
from pipeline.warehouse_mapping import WarehouseMappingPipeline
from pipeline.data_write import DataWritingPipeline

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
        obj = RegexProcessingPipeline()
        obj.main()
        logger.info(f">>> STAGE {STAGE_NAME} COMPLETED <<<")

    except Exception as e:
        logger.error(f">>> STAGE {STAGE_NAME} FAILED <<<")
        logger.exception(e)
        raise e

STAGE_NAME = "API PROCESSING"

if __name__ == "__main__":
    try:
        logger.info(f">>> STAGE {STAGE_NAME} STARTED <<<")
        obj = APIGeocodingPipeline()
        obj.main()
        logger.info(f">>> STAGE {STAGE_NAME} COMPLETED <<<")

    except Exception as e:
        logger.error(f">>> STAGE {STAGE_NAME} FAILED <<<")
        logger.exception(e)
        raise e

STAGE_NAME = "WAREHOUSE MAPPING"

if __name__ == "__main__":
    try:
        logger.info(f">>> STAGE {STAGE_NAME} STARTED <<<")
        obj = WarehouseMappingPipeline()
        obj.main()
        logger.info(f">>> STAGE {STAGE_NAME} COMPLETED <<<")

    except Exception as e:
        logger.error(f">>> STAGE {STAGE_NAME} FAILED <<<")
        logger.exception(e)
        raise e

STAGE_NAME = "DATA WRITING"

if __name__ == "__main__":
    try:
        logger.info(f">>> STAGE {STAGE_NAME} STARTED <<<")
        obj = DataWritingPipeline()
        obj.main()
        logger.info(f">>> STAGE {STAGE_NAME} COMPLETED <<<")

    except Exception as e:
        logger.error(f">>> STAGE {STAGE_NAME} FAILED <<<")
        logger.exception(e)
        raise e
