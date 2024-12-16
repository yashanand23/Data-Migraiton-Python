import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()


def flatten_collection(doc, nested_fields):
    """
    Flatten a document with nested fields where values can have multiple entries.
    Each entry in the nested fields will create separate rows.

    Args:
        doc (dict): The document to flatten.
        nested_fields (list): List of nested fields to flatten.

    Returns:
        list: A list of flattened documents.
    """
    flattened_docs = [{}]

    try:
        # Loop through each field in the document
        for key, value in doc.items():
            if key in nested_fields and isinstance(value, list):
                expanded_docs = []
                for nested_value in value:
                    for base_doc in flattened_docs:
                        new_doc = base_doc.copy()
                        new_doc[key] = (
                            nested_value["name"]
                            if isinstance(nested_value, dict) and "name" in nested_value
                            else nested_value
                        )
                        expanded_docs.append(new_doc)
                flattened_docs = expanded_docs
            else:
                for base_doc in flattened_docs:
                    base_doc[key] = value
    except Exception as e:
        print(f"Error flattening document for key '{key}': {e}")

    return flattened_docs


def clean_dataframe(df):
    """
    Clean a pandas DataFrame by handling missing values, duplicates,
    and ensuring consistent data types.

    Args:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: A cleaned DataFrame with consistent data types and no duplicates.
    """
    if df.empty:
        print("Warning: The DataFrame is empty. Returning without processing.")
        return df

    try:
        # Drop duplicates and ensure consistent data types
        df = df.drop_duplicates()
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].astype(str).fillna("unknown").str.strip().str.lower()
            elif pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].fillna(pd.Timestamp("1970-01-01"))
            else:
                df[col] = df[col].astype(str).fillna("unknown").str.strip().str.lower()

        # Standardize column names
        df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()

    except Exception as e:
        print(f"Error cleaning DataFrame: {e}")

    return df


def transform_data(raw_dataframes, nested_fields_mapping):
    """
    Main function to transform the raw data.

    Args:
        raw_dataframes (dict): Dictionary of raw dataframes.
        nested_fields_mapping (dict): Dictionary where keys are collection names and values are lists of nested fields to flatten.

    Returns:
        dict: Processed and cleaned dataframes.
    """
    processed_dataframes = {}

    for collection_name, df in raw_dataframes.items():
        if df is None or df.empty:
            print(
                f"Warning: The DataFrame for collection '{collection_name}' is None or empty. Skipping transformation."
            )
            processed_dataframes[collection_name] = (
                pd.DataFrame()
            )  # Return an empty DataFrame
            continue  # Skip this collection and move to the next one

        print(f"Transforming collection: {collection_name}")
        flattened_docs = []

        if collection_name in nested_fields_mapping:
            nested_fields = nested_fields_mapping[collection_name]
            for doc in df.to_dict("records"):
                flattened_docs.extend(flatten_collection(doc, nested_fields))
            processed_dataframes[collection_name] = clean_dataframe(
                pd.DataFrame(flattened_docs)
            )
        else:
            # No nested fields, clean dataframe as is
            processed_dataframes[collection_name] = clean_dataframe(df)

        print(f"Transformation complete for collection: {collection_name}")

    return processed_dataframes


if __name__ == "__main__":
    # Sample data for testing (example only)
    # Load raw_dataframes from actual MongoDB extraction in your main code

    raw_dataframes = {
        "example_collection": pd.DataFrame(
            [
                {
                    "title": "Example A",
                    "authors": [{"name": "Author 1"}, {"name": "Author 2"}],
                    "tags": [{"name": "Tag1"}, {"name": "Tag2"}],
                    "average_rating": 4.5,
                },
                {
                    "title": "Example B",
                    "authors": [{"name": "Author 3"}],
                    "tags": [{"name": "Tag3"}],
                    "average_rating": 3.8,
                },
            ]
        ),
    }

    # Define nested fields for each collection
    nested_fields_mapping = {"example_collection": ["authors", "tags"]}

    # Run transformation
    processed_dataframes = transform_data(raw_dataframes, nested_fields_mapping)

    # Print the processed dataframes for inspection
    for collection, df in processed_dataframes.items():
        print(f"Processed Data for '{collection}' collection:")
        print(df)
        print("\n" + "=" * 40 + "\n")
