from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()


def read_data_from_mongo(db, collection_name, batch_size=1000):
    """
    Reads data from a specified MongoDB collection and returns it as a pandas DataFrame.
    """
    try:
        collection = db[collection_name]
        cursor = collection.find({})
        data_batch = []
        all_data = []

        for document in cursor:
            data_batch.append(document)
            if len(data_batch) >= batch_size:
                print(
                    f"Processing batch of {batch_size} documents from '{collection_name}'"
                )
                all_data.extend(data_batch)
                data_batch = []

        if data_batch:
            print(f"Processing final batch from '{collection_name}'")
            all_data.extend(data_batch)

        df = pd.DataFrame(all_data)
        if "_id" in df.columns:
            df.drop("_id", axis=1, inplace=True)

        return df

    except Exception as e:
        print(f"Error reading data from MongoDB collection '{collection_name}': {e}")
        return pd.DataFrame()


def extract_data(batch_size=1000):
    """
    Main function to extract data from MongoDB.
    """
    try:
        mongo_uri = os.getenv("MONGO_URI")
        mongo_db_name = os.getenv("MONGO_DB")

        # Connect to MongoDB using URI and database name from .env file
        mongo_client = MongoClient(mongo_uri)
        mongo_db = mongo_client[mongo_db_name]

        collections = mongo_db.list_collection_names()
        raw_dataframes = {}

        for collection in collections:
            print(f"Reading data from collection: {collection}")
            data_df = read_data_from_mongo(mongo_db, collection, batch_size=batch_size)
            raw_dataframes[collection] = data_df

        return raw_dataframes

    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return {}


if __name__ == "__main__":
    extract_data()
