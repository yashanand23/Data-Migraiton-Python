from sqlalchemy import create_engine
import mysql.connector
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()


def write_dataframe_to_sql(df, table_name, engine, if_exists="replace"):
    """
    Write a pandas DataFrame to a SQL table.

    Args:
        df (pd.DataFrame): The DataFrame to write to SQL.
        table_name (str): The name of the SQL table.
        engine (SQLAlchemy Engine): The SQLAlchemy engine connected to the database.
        if_exists (str): The behavior when the table exists. Options are 'replace', 'append', or 'fail'.
    """
    try:
        df.to_sql(table_name, con=engine, index=False, if_exists=if_exists)
        print(f"Data written to table '{table_name}' successfully.")
    except Exception as e:
        print(f"Error occurred while writing to table '{table_name}': {e}")


def connect_mysql():
    """
    Connects to the MySQL database with provided credentials.

    Returns:
        connection (mysql.connector.connection): MySQL database connection if successful; None otherwise.
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB"),
            port=os.getenv("MYSQL_PORT"),
        )
        print("Successfully connected to MySQL.")
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


def connect_mongo():
    """
    Connects to the MongoDB database.

    Returns:
        collection (pymongo.collection.Collection): Collection of books if successful; None otherwise.
    """
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB")]
        collection = db["books"]
        print("Successfully connected to MongoDB.")
        return collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None


def get_last_loaded_timestamp_from_books(connection):
    """
    Retrieves the most recent last_modified_date from the books table in MySQL.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT MAX(last_modified_date) FROM books")
    result = cursor.fetchone()
    return result[0] if result[0] else datetime(2023, 1, 1)


def fetch_incremental_load(collection, last_loaded_timestamp):
    """
    Retrieves new or updated records from MongoDB based on the last loaded timestamp.
    """
    print("Fetching incremental data from MongoDB...")

    if isinstance(last_loaded_timestamp, str):
        last_loaded_timestamp = datetime.fromisoformat(last_loaded_timestamp)

    mongo_pipeline = {"last_modified_date": {"$gt": last_loaded_timestamp}}
    incremental_data = list(collection.find(mongo_pipeline))

    if incremental_data:
        print(f"Found {len(incremental_data)} new or updated records.")
    else:
        print("No new or updated records found.")

    return incremental_data


def process_and_load_to_mysql(incremental_data, connection):
    """
    Inserts or updates records in MySQL based on MongoDB incremental data.
    """
    cursor = connection.cursor()

    for data in incremental_data:
        book_id = data.get("book_id")
        title = data.get("title")
        authors = data.get("authors", [])
        publication_year = data.get("publication_year")
        isbn = data.get("isbn")
        average_rating = data.get("average_rating")
        tags = data.get("tags", [])
        image_url = data.get("image_url")
        modified_date = data.get("modified_date")
        last_modified_date = data.get("last_modified_date")

        authors = ", ".join(map(str, authors)) if isinstance(authors, list) else authors
        tags = ", ".join(map(str, tags)) if isinstance(tags, list) else tags

        print(f"Processing Book ID: {book_id}")
        print(f"Data: {data}")

        cursor.execute("SELECT COUNT(*) FROM books WHERE book_id = %s", (book_id,))
        record_exists = cursor.fetchone()[0] > 0

        try:
            if record_exists:
                update_query = """
                    UPDATE books
                    SET 
                        title = %s,
                        authors = %s,
                        publication_year = %s,
                        isbn = %s,
                        average_rating = %s,
                        tags = %s,
                        image_url = %s,
                        modified_date = %s,
                        last_modified_date = %s
                    WHERE book_id = %s
                """
                cursor.execute(
                    update_query,
                    (
                        title,
                        authors,
                        publication_year,
                        isbn,
                        average_rating,
                        tags,
                        image_url,
                        modified_date,
                        last_modified_date,
                        book_id,
                    ),
                )
                print(f"Updated existing book ID: {book_id}")
            else:
                insert_query = """
                    INSERT INTO books (book_id, title, authors, publication_year, isbn,
                                     average_rating, tags, image_url, modified_date,
                                     last_modified_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(
                    insert_query,
                    (
                        book_id,
                        title,
                        authors,
                        publication_year,
                        isbn,
                        average_rating,
                        tags,
                        image_url,
                        modified_date,
                        last_modified_date,
                    ),
                )
                print(f"Inserted new book ID: {book_id}")

            connection.commit()
        except mysql.connector.Error as e:
            print(f"Error occurred while processing book ID {book_id}: {e}")


def load_data(processed_dataframes):
    """
    Main function to load the processed data into MySQL.
    """
    print("Starting data loading process...")

    # Create SQLAlchemy engine
    engine = create_engine(
        f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
    )

    print("Created MySQL connection engine")

    # Load processed dataframes
    for collection_name, df in processed_dataframes.items():
        print(f"Loading data for collection: {collection_name}")
        write_dataframe_to_sql(df, collection_name, engine)


def incremental_load():
    """
    Main function to execute the incremental data migration.
    """
    print("Connecting to MongoDB and MySQL...")

    collection = connect_mongo()
    connection = connect_mysql()

    if connection is not None and collection is not None:
        last_loaded_timestamp = get_last_loaded_timestamp_from_books(connection)
        print(f"Last loaded timestamp from MySQL: {last_loaded_timestamp}")

        incremental_data = fetch_incremental_load(collection, last_loaded_timestamp)

        if incremental_data:
            process_and_load_to_mysql(incremental_data, connection)
        else:
            print("No new data to process.")

        connection.close()
    else:
        print("Failed to connect to MongoDB or MySQL.")


if __name__ == "__main__":
    incremental_load()
