import os
import logging
import pandas as pd
from pymongo import MongoClient
import mysql.connector
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("reconciliation.log"), logging.StreamHandler()],
)


# Function to fetch data from MongoDB
def fetch_data_from_mongo(collection_name, mongo_db):
    """
    Fetch all data from a MongoDB collection.
    """
    try:
        collection = mongo_db[collection_name]
        mongo_data = pd.DataFrame(list(collection.find()))
        logging.info(
            f"Fetched {len(mongo_data)} records from MongoDB collection '{collection_name}'."
        )
        return mongo_data
    except Exception as e:
        logging.error(
            f"Error fetching data from MongoDB collection '{collection_name}': {e}"
        )
        return pd.DataFrame()


# Function to fetch data from MySQL
def fetch_data_from_sql(connection, table_name):
    """
    Fetch all data from a MySQL table.
    """
    try:
        if table_name == "books":
            # Fetch distinct book IDs for the exploded books table
            query = "SELECT DISTINCT book_id FROM books"
        else:
            query = f"SELECT * FROM {table_name}"

        sql_data = pd.read_sql(query, connection)
        logging.info(f"Fetched {len(sql_data)} records from SQL table '{table_name}'.")
        return sql_data
    except Exception as e:
        logging.error(f"Error fetching data from SQL table '{table_name}': {e}")
        return pd.DataFrame()


# Function to compare row counts
def compare_row_counts(mongo_count, sql_count, collection_name):
    """
    Compare row counts between MongoDB and SQL.
    """
    if mongo_count != sql_count:
        logging.warning(
            f"Row count mismatch for '{collection_name}': MongoDB={mongo_count}, SQL={sql_count}"
        )
        return False
    else:
        logging.info(
            f"Row count matches for '{collection_name}': {mongo_count} records."
        )
        return True


def main():
    """
    Main function to perform data reconciliation between MongoDB and SQL.
    """
    # Load environment variables
    load_dotenv()

    # MongoDB connection setup
    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB = os.getenv("MONGO_DB")
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    logging.info("Connected to MongoDB.")

    # MySQL connection setup
    try:
        mysql_conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB"),
            port=os.getenv("MYSQL_PORT"),
        )
        logging.info("Connected to MySQL.")
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to MySQL: {err}")
        return

    # Define the tables/collections for reconciliation
    mongo_collections = [
        "books",
        "authors",
        "ratings",
        "tags",
        "to_read",
        "readers",
    ]  # MongoDB collections
    sql_tables = [
        "books",
        "authors",
        "ratings",
        "tags",
        "to_read",
        "readers",
    ]  # SQL tables 

    # Iterate through each collection/table for reconciliation
    for collection_name in mongo_collections:
        logging.info(f"Reconciling data for '{collection_name}'.")

        # Fetch data from MongoDB
        mongo_df = fetch_data_from_mongo(collection_name, mongo_db)
        if mongo_df.empty:
            logging.warning(
                f"No data found in MongoDB for collection '{collection_name}'. Skipping reconciliation."
            )
            continue

        # Fetch corresponding data from SQL
        sql_df = fetch_data_from_sql(mysql_conn, collection_name)
        if sql_df.empty:
            logging.warning(
                f"No data found in SQL for table '{collection_name}'. Skipping reconciliation."
            )
            continue

        # Compare row counts
        mongo_count = len(mongo_df)
        sql_count = len(sql_df)
        compare_row_counts(mongo_count, sql_count, collection_name)

    # Close database connections
    mongo_client.close()
    mysql_conn.close()
    logging.info("Reconciliation process completed.")


if __name__ == "__main__":
    main()
