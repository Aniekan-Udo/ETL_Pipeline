from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
# import pymongo # No longer directly needed if using MongoHook's client
from pymongo import MongoClient
from airflow.providers.mongo.hooks.mongo import MongoHook # Import MongoHook
from bson import ObjectId
import json
import os
import logging

logger = logging.getLogger("airflow.task")

# Custom JSON Encoder (Still needed for writing to file in extract for interim storage)
class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime and ObjectId objects for serialization."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat(timespec='microseconds')
        if isinstance(obj, ObjectId):
            return str(obj) # Convert ObjectId to string
        return json.JSONEncoder.default(self, obj)

# --- Updated mongo_connect function using MongoHook ---
def get_mongo_client(conn_id):
    """
    Retrieves a MongoClient instance using Airflow's MongoHook.
    """
    hook = MongoHook(mongo_conn_id=conn_id)
    # The get_conn method on MongoHook returns a pymongo.MongoClient instance
    return hook.get_conn()

@dag(
    dag_id="mongo_etl_dag",
    schedule="@daily",
    start_date=(datetime.today() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0),
    catchup=False,
    tags=["etl", "mongo"]
)
def mongo_etl_dag():
    # Define your MongoDB connection ID here
    MONGO_CONN_ID = "mongo_rewards_ap_v3"

    @task()
    def extract():
        """
        Extracts data from MongoDB and writes it to a JSON file.
        Returns the path to the JSON file via XCom.
        """
        logger.info("Starting MongoDB extraction (source)..")
        client = None
        dag_run_id = "{{ run_id }}"
        temp_file_path = f"/tmp/mongo_data_{dag_run_id}.json"

        try:
            # Use the new get_mongo_client function with the connection ID
            client = get_mongo_client(MONGO_CONN_ID)
            db = client['rewards-ap-v3']
            collection = db["BottleDetail"]
            raw_data = list(collection.find({}))
            logger.info(f"Successfully extracted {len(raw_data)} documents from MongoDB (source).")

            if raw_data:
                logger.info(f"Sample of extracted data (first document): {raw_data[0]}")
            else:
                logger.info("No documents found in MongoDB collection.")

            with open(temp_file_path, 'w') as f:
                json.dump(raw_data, f, cls=DateTimeEncoder, indent=2)
            logger.info(f"Data written to temporary file: {temp_file_path}")

            return temp_file_path

        except Exception as e:
            logger.error(f"Error during extract task: {e}")
            raise
        finally:
            if client:
                client.close()

    @task()
    def transform(file_path):
        """
        Reads data from the specified JSON file, transforms it,
        and returns it as a list of dictionaries with standard Python datetimes.
        """
        logger.info(f"Transform task received file path: {file_path}")

        if not os.path.exists(file_path):
            logger.warning(f"File not found at {file_path}. No data to transform.")
            return []

        data = []
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            logger.info(f"Successfully read {len(data)} documents from {file_path}.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {file_path}: {e}")
            raise
        finally:
            try:
                os.remove(file_path)
                logger.info(f"Removed temporary file: {file_path}")
            except OSError as e:
                logger.warning(f"Error removing file {file_path}: {e}")

        if not data:
            logger.warning("Transform task received empty data after loading from file. Skipping DataFrame creation.")
            return []

        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            logger.error(f"Data loaded from file is not a list of dictionaries. Type: {type(data)}")
            raise TypeError("Expected data to be a list of dictionaries for DataFrame creation.")

        try:
            df = pd.DataFrame(data)
            logger.info(f"DataFrame created successfully. Shape: {df.shape}")
            logger.info(f"DataFrame dtypes (before datetime conversion): \n{df.dtypes}")

            date_cols = ["dateAdded", "createdAt", "updatedAt"]
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    logger.info(f"Converted column '{col}' to Pandas Timestamp.")
            
            logger.info(f"DataFrame dtypes (after initial datetime conversion): \n{df.dtypes}")

            for col in date_cols:
                if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)
                    logger.info(f"Converted column '{col}' to Python datetime objects for MongoDB load.")

            if "_id" in df.columns:
                df = df.drop(columns=["_id"])
                logger.info("Dropped '_id' column to allow MongoDB to generate new _id.")
            
            if not df.empty:
                logger.info(f"Sample of transformed DataFrame (first 5 rows): \n{df.head().to_string()}")

            return df.to_dict(orient="records")
        except Exception as e:
            logger.error(f"Error during transform task: {e}")
            raise

    @task()
    def load(data):
        """
        Loads the transformed data into a target MongoDB collection.
        Expects a list of dictionaries with standard Python datetimes.
        """
        logger.info(f"Load task received {len(data)} records for loading into MongoDB.")

        if not data:
            logger.info("No data to load into MongoDB. Skipping insert operations.")
            return

        client = None
        try:
            # Use the new get_mongo_client function with the connection ID
            client = get_mongo_client(MONGO_CONN_ID)
            db = client['rewards-ap-v3']
            target_collection = db["transformed_bottle_details"]
            logger.info(f"Attempting to insert {len(data)} documents into '{target_collection.name}' collection.")

            result = target_collection.insert_many(data)
            logger.info(f"Successfully loaded {len(result.inserted_ids)} records into MongoDB.")

        except Exception as e:
            logger.error(f"Error during load task: {e}")
            raise
        finally:
            if client:
                client.close()

    
    file_path = extract()
    transformed_data = transform(file_path)
    load(transformed_data)

mongo_etl = mongo_etl_dag()