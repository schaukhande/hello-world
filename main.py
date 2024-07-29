"""Web scrapper"""
import io
# import logging
import base64
# import datetime
# from io import StringIO
import json
import traceback
from time import perf_counter
import pandas as pd
import functions_framework
from google.cloud import storage
from google.cloud import bigquery

import scrape_config
from scrape_helper import ScrapeHelper

scrape_helper_obj = ScrapeHelper()


@functions_framework.cloud_event
def scrape_handler(cloud_event):
    try: 

        """Wrapper function to scrape data from given file"""
        print("PubSub request message details are:",
            base64.b64decode(cloud_event.data["message"]["data"]))
        message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode()
        msg_json = json.loads(message_data)
        loc_name = msg_json["name"].split("/")

        # TODO: Dummy data to be replaced with pubsub message contents
        week_num = loc_name[1]
        # week_num = str(today.year) + "_" + str(today.isocalendar()[1])
        sitemap_name = loc_name[2].replace(".csv","")

        # Check for valid retailer name
        retailer = scrape_helper_obj.get_retailer(sitemap_name)

        # Extract category of retailer
        category_all_data = False
        if "cat01" in sitemap_name:
            category = "all"
            category_all_data = True
        else:
            category = sitemap_name.split("-")[1].capitalize()

        # Google cloud storage i/o operations
        # Create a GCS client
        storage_client = storage.Client()

        # Get the bucket and blob objects
        bucket = storage_client.get_bucket(scrape_config.GCS_ARTIFACTS_BUCKET_NAME)
        blobs = bucket.list_blobs(
            prefix=f'{scrape_config.SCRAPPER_PROCESSED_DATA_BLOB_NAME}/', delimiter="/")
        blob_response = blobs._get_next_page_response()
        week_dir = f'{scrape_config.SCRAPPER_PROCESSED_DATA_BLOB_NAME}/{week_num}/'

        # Create folder inside bucket for current week if does not exists
        if blob_response.get("prefixes") and week_dir not in blob_response["prefixes"]:
            blob = bucket.blob(
                f'{scrape_config.SCRAPPER_PROCESSED_DATA_BLOB_NAME}/{week_num}/')
            blob.upload_from_string('')

        # Check if raw data for corresponding week is already processed
        existing_files_iterator = bucket.list_blobs(
            prefix=f'{scrape_config.SCRAPPER_PROCESSED_DATA_BLOB_NAME}/{week_num}/')
        for file_obj in existing_files_iterator:
            if sitemap_name in file_obj.name:
                print(f"Job {sitemap_name} is already processed")
                return None

        # Load raw data from gcs bucket for processing
        blob = bucket.blob(
            f'{scrape_config.SCRAPPER_RAW_DATA_BLOB_NAME}/{week_num}/{sitemap_name}.csv')
        data = blob.download_as_bytes()

        if  scrape_config.INCLUDE_BRANDS:
            # Load brands master list from big query
            brands_df = scrape_helper_obj.get_default_product_brands(retailer, category)

        # Extract brand info of corresponding retailer and category
        # brand_names_default = scrape_helper_obj.get_retailer_specific_brands(
        #     brands_df, retailer, category)

        #TODO dummy data for brands
        brand_names_default = []

        # Read and process cvs loaded from gcs bucket
        out_data = []
        for chunk in  pd.read_csv(io.BytesIO(data), chunksize=100):
            scrape_helper_obj.process_csv_parallel(
                chunk,
                retailer,
                category,
                out_data,
                brand_names_default,
                week_num
            )

        scrape_helper_obj.generate_output_file(
            out_data, f"scrape_result_{sitemap_name}", retailer,category, bucket, category_all_data, week_num)

        print("Data extraction and scrapping completed")

    except Exception as ex:
        print("Error occured while processing the request:",ex)
        print(traceback.format_exc())
        return ("Error occured while processing the request:",ex)
    return "Execution finished!"
