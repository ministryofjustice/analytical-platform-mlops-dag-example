import logging
import os
from datetime import datetime
from functools import wraps

import boto3
import pandas as pd
import requests

s3_client = boto3.client("s3")

SOURCE_BUCKET = os.environ.get("S3_SOURCE_BUCKET")
SOURCE_KEY = os.environ.get("S3_SOURCE_KEY")
DESTINATION_BUCKET = os.environ.get("S3_DESTINATION_BUCKET")
DESTINATION_KEY = os.environ.get("S3_DESTINATION_KEY")
LLM_GATEWAY_URL = os.environ.get("SECRET_LLM_GATEWAY_URL")


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def call_llm_gateway(text):
    try:
        response = requests.post(LLM_GATEWAY_URL, json={"text": text}, timeout=10)
        response.raise_for_status()
        result = response.json()
        return result.get("transformed_text", text)
    except Exception as e:
        logging.error("LLM Gateway call failed: %s", e)
        return text


def log_timing(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        logging.info("Starting %s with args=%s, kwargs=%s", func_name, args, kwargs)
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        logging.info(
            "Completed %s in %s seconds",
            func_name,
            (end_time - start_time).total_seconds(),
        )
        return result

    return wrapper


@log_timing
def read_csv_from_s3():
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
    df = pd.read_csv(obj["Body"])
    return df


@log_timing
def transform_data(df):
    # Use a call to the gateway to examine each line in the input-data.csv file and correct any spelling mistakes.
    for index, row in df.iterrows():
        text = row["text"]
    # Call LLM Gateway API to transform text
    transformed_text = call_llm_gateway(text)
    df.at[index, "text"] = transformed_text
    logging.debug("Row %s transformed at %s", index, datetime.now().isoformat())
    return df


@log_timing
def write_csv_to_s3(df):
    s3 = boto3.client("s3")
    csv_buffer = df.to_csv(index=False)
    s3.put_object(Bucket=DESTINATION_BUCKET, Key=DESTINATION_KEY, Body=csv_buffer)
