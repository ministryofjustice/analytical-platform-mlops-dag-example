import logging
import os
import sys
from datetime import datetime
from functools import wraps

import boto3
import pandas as pd
import requests
import openai

s3_client = boto3.client("s3")

SOURCE_BUCKET = os.environ.get("S3_SOURCE_BUCKET")
SOURCE_KEY = os.environ.get("S3_SOURCE_KEY")
DESTINATION_BUCKET = os.environ.get("S3_DESTINATION_BUCKET")
DESTINATION_KEY = os.environ.get("S3_DESTINATION_KEY")
LLM_GATEWAY_URL = os.environ.get("SECRET_LLM_URL")
LLM_GATEWAY_API_KEY = os.environ.get("SECRET_LLM_KEY")


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout
)

def log_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Running {func.__name__}")
        return func(*args, **kwargs)
    return wrapper


def _normalized_llm_base_url():
    if not LLM_GATEWAY_URL:
        raise ValueError("SECRET_LLM_URL environment variable not set")
    url = LLM_GATEWAY_URL.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        url = "https://" + url.lstrip("/")
    return url.rstrip("/")


def call_llm_gateway(text: str) -> str:
    try:
        base_url = _normalized_llm_base_url()
        client = openai.OpenAI(
            api_key=LLM_GATEWAY_API_KEY,
            base_url=base_url,
        )
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": text}],
            timeout=30,
        )
        return response.choices[0].message.content
    except Exception as e:
        logging.error("LLM Gateway call failed for text '%s': %s", text, e)
        return text


@log_function
def read_csv_from_s3():
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
    df = pd.read_csv(obj["Body"])
    print(f"DEBUG - inside read_csv_from_s3: {df.head()}")
    return df


@log_function
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    if "text" not in df.columns:
        raise KeyError("Input DataFrame missing required 'text' column")

    for index, row in df.iterrows():
        original = row["text"]
        transformed = call_llm_gateway(original)
        if transformed == original:
            logging.warning("Row %s unchanged (LLM error or no modification)", index)
        df.at[index, "text"] = transformed
        logging.debug("Row %s transformed at %s", index, datetime.now().isoformat())

    return df


@log_function
def write_csv_to_s3(df):
    s3 = boto3.client("s3")
    csv_buffer = df.to_csv(index=False)
    s3.put_object(Bucket=DESTINATION_BUCKET, Key=DESTINATION_KEY, Body=csv_buffer)


if __name__ == "__main__":
    df = read_csv_from_s3()
    df = transform_data(df)
    write_csv_to_s3(df)
    print(f"Completed at {datetime.now().isoformat()}")
