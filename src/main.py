import logging
from dotenv import load_dotenv
from data_extraction import extract_data
from data_transformation import transform_data
from data_loading import load_data, incremental_load
import reconciliation  # Ensure reconciliation.py is in the same directory

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_process.log"), logging.StreamHandler()],
)


def main():
    """
    Main function to orchestrate the ETL process: Extract, Transform, Load, and Reconcile.
    """
    try:
        # Step 1: Extract data from source
        logging.info("Starting data extraction...")
        raw_dataframes = (
            extract_data()
        )  # Assuming it returns a dictionary of raw DataFrames
        logging.info("Data extraction completed.")

        # Define the mapping for nested fields (adjust based on your dataset)
        nested_fields_mapping = {
            "order_items": "items",  # Example mapping, modify as needed
            "order_details": "details",  # Example mapping
            # Add more mappings as required
        }

        # Step 2: Transform data (cleaning, restructuring)
        logging.info("Starting data transformation...")
        processed_dataframes = transform_data(raw_dataframes, nested_fields_mapping)
        logging.info("Data transformation completed.")

        # Step 3: Load data into the database
        logging.info("Starting data loading process...")
        # Choose whether to perform a full load or incremental load
        incremental_load()  # Or use load_data(processed_dataframes) if performing a full load
        logging.info("Data loading completed.")

        # Step 4: Reconcile data
        logging.info("Starting reconciliation process...")
        reconciliation.main()
        logging.info("Reconciliation process completed.")

    except Exception as e:
        logging.error(f"An error occurred during the ETL process: {e}")
    finally:
        logging.info("ETL process completed.")


if __name__ == "__main__":
    main()
