import configparser
import logging
from logging.config import fileConfig
from utils import WindTurbineDataProcessor


def main():
    # Logging setup
    fileConfig('logging_config.ini')
    logger = logging.getLogger(__name__)
    logger.info("Application started")

    # Config params setup
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Extract configuration values
    data_paths = [config['DATA_INGESTION_PATHS'][key] for key in config['DATA_INGESTION_PATHS']]

    # Retrieve DB configuration
    db_config = {
        'jdbc_url': config['DB_CONFIG']['jdbc_url'],
        'table_name': config['DB_CONFIG']['table_name'],
        'properties': {
            'user': config['DB_CONFIG']['user'],
            'password': config['DB_CONFIG']['password'],
            'driver': config['DB_CONFIG']['driver']
        }
    }

    processor = WindTurbineDataProcessor(data_paths=data_paths, db_config=db_config)

    processor.preprocess_data()

    processor.compute_statistics()
    processor.identify_and_impute_outliers()

    processor.calculate_aggregates()

    processor.write_to_database()


if __name__ == "__main__":
    main()
