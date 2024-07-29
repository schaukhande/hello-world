"""Utils to support scrape wrapper"""


import datetime
import json
import io
import os
import time
import re
import copy
import logging
from io import StringIO
from datetime import timezone
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing.pool import Pool
import scrape_config
import scrape_subroutines
from custom_exceptions import UnknownRetailer

from google.cloud import bigquery


class ScrapeHelper:
    """Class with utils to support scrape wrapper"""

    scrapper_subroutines = scrape_subroutines.Subroutines()

    def add_data_to_database(self, retailer, category, week_num, processed_df, bq_weekly_scrapes_table_id):
        """Function to upload scrape results to bigquery database"""
        try:
            client = bigquery.Client()
            row_list = []
            data_exists_flag = False
            data_exist_query = "SELECT CASE WHEN EXISTS (SELECT 1 FROM TABLE_ID_HERE WHERE retailer='RETAILER_NAME_HERE' and category='CATEGORY_HERE' and scrape_name='SCRAPE_NAME_HERE') THEN true ELSE false END"
            data_exist_query = data_exist_query.replace("RETAILER_NAME_HERE", retailer).replace(
                "CATEGORY_HERE", category).replace("SCRAPE_NAME_HERE", week_num.replace("_", "-")).replace(
                "TABLE_ID_HERE", bq_weekly_scrapes_table_id)
            results = client.query(data_exist_query)
            results_row = next(results.result())
            data_exists_flag = results_row.values()[0]
            if data_exists_flag:
                print(
                    f"Cannot add data in data in database..Data exists for retailer {retailer} with category {category} for week {week_num}")
                return
            for index, rows in processed_df.iterrows():
                time_scraped = rows.time_scraped
                if time_scraped:
                    if "-" not in time_scraped:
                        time_scraped = datetime.datetime.strptime(
                            time_scraped, "%m/%d/%y %H:%M")
                        time_scraped = time_scraped.strftime(
                            "%Y-%m-%d %H:%M:%S")
                else:
                    time_scraped = None
                sponsored_in_top_10_count = rows.sponsored_in_top_10_count
                sponsored_in_top_20_count = rows.sponsored_in_top_20_count
                organic_product_count = rows.organic_product_count
                sr_no = index
                if sponsored_in_top_10_count == '':
                    sponsored_in_top_10_count = None
                if sponsored_in_top_20_count == '':
                    sponsored_in_top_20_count = None
                if organic_product_count == '':
                    organic_product_count = None
                time_processed = rows.time_processed
                my_list = (
                    sr_no,
                    rows.retailer,
                    rows.category,
                    rows.search_key,
                    rows.scraped_url,
                    rows.sponsored_product_count,
                    rows.sponsored_product_positions,
                    sponsored_in_top_10_count,
                    sponsored_in_top_20_count,
                    rows.sponsored_product_titles,
                    organic_product_count,
                    rows.organic_product_titles,
                    time_scraped,
                    time_processed,
                    rows.ad_unit_type,
                    rows.ad_unit_location,
                    rows.scrape_page_id,
                    rows.scrape_job_id,
                    rows.page_type,
                    rows.scrape_name,
                    rows.problem_detected,
                    rows.problem_description
                )
                # append the list to the final list
                row_list.append(my_list)
            # Get Biq Query Set up
            table = client.get_table(bq_weekly_scrapes_table_id)
            errors = []
            if row_list == []:
                print("Warning: No data to upload to bigquery")
                return None
            # Make an API request.
            errors = client.insert_rows(table, row_list)
            print("database errors:",errors)
            if errors == []:
                return None
        except Exception as err:
            print("Error while adding data to bigquery database",err)
            return f'Exception occured: {err}'

        return f"Execution finished!..errors {errors}"

    def generate_output_file(self, out_data, filename_prefix, retailer, category, bucket, category_all_data, week_num):
        """Function to generate scrape results excel"""
        try:
            processed_df = pd.DataFrame(
                out_data, columns=[
                    "retailer",
                    "category",
                    "search_key",
                    "scraped_url",
                    "sponsored_product_count",
                    "sponsored_product_positions",
                    "sponsored_in_top_10_count",
                    "sponsored_in_top_20_count",
                    "sponsored_product_titles",
                    "organic_product_count",
                    "organic_product_titles",
                    "time_scraped",
                    "time_processed",
                    "ad_unit_type",
                    "ad_unit_location",
                    "scrape_page_id",
                    "scrape_job_id",
                    "page_type",
                    "scrape_name",
                    "problem_detected",
                    "problem_description"

                ]
            )

            processed_df["time_processed"] = processed_df["time_processed"].astype(
                str)
            processed_df.reset_index(inplace=True, drop=True)
            processed_df.index = processed_df.index+1
            processed_df.index.name = "sr_no"
            # Upload processed data to gcs bucket
            # week_num_dir = f'{scrape_config.SCRAPPER_PROCESSED_DATA_BLOB_NAME}/{week_num}/'
            # time_created = str(int(datetime.datetime.utcnow().timestamp()))
            bucket.blob(
                f'{scrape_config.SCRAPPER_PROCESSED_DATA_BLOB_NAME}/{week_num}/{filename_prefix}.csv'
            ).upload_from_string(
                processed_df.to_csv(), 'text/csv'
            )

            # Upload scraping results to bigquery table
            if category_all_data:
                bq_weekly_scrapes_table_id = scrape_config.BQ_CATEGORY_WEEKLY_SCRAPES_TABLE_ID
            else:
                bq_weekly_scrapes_table_id = scrape_config.BQ_STAGING_WEEKLY_SCRAPES_TABLE_ID
            self.add_data_to_database(
                retailer, category, week_num, processed_df, bq_weekly_scrapes_table_id)
        except Exception as ex:
            print(
                "Error while generating scrape report",ex)

    def get_new_product_brands(self, soup, retailer):
        all_brands = set()
        RESULTS_ALL_BRAND_LIST_QUERY = scrape_config.RESULTS_ALL_BRAND_LIST_QUERY.get(
            retailer)
        try:
            if RESULTS_ALL_BRAND_LIST_QUERY is not None:
                brand_data = soup.select(RESULTS_ALL_BRAND_LIST_QUERY)
                brand_list = []
                brand_text = ""
                if retailer.lower() == "homedepot":
                    for dim in brand_data:
                        brands_flag = dim.select("h2[data-group='Brand']")
                        if brands_flag:
                            brand_data = ['']
                            brand_data[0] = dim
                            break
                if brand_data and brand_data[0] is not None:
                    if retailer.lower() == "staples":
                        try:
                            data = brand_data[0].text
                            brand_json = json.loads(data)
                            brand_list = brand_json["props"]["initialStateOrStore"]["searchState"]["productTileData"]
                        except Exception as err:
                            logging.warning(
                                "Excluding Staples page due to error %s", err)
                            brand_list = []
                    elif retailer.lower() == "walmart":
                        data = brand_data[0].text
                        brand_json = json.loads(data)
                        section_list = brand_json["props"]["pageProps"]["initialData"][
                            "moduleDataByZone"]["topZone3"]["configs"]["topNavFacets"]
                        for item in section_list:
                            if item["title"] == "Brand":
                                brand_list = item["values"]
                                break
                    elif retailer.lower() == "bestbuy":
                        for entry in brand_data:
                            if entry.get("id") and "shop-product-list-" in entry["id"]:
                                data = entry.text
                                brand_json = json.loads(data)
                                section_list = brand_json["search"]["facetList"]
                                for item in section_list:
                                    if item["displayName"] == "Brand":
                                        brand_list = item["values"]
                                        break
                                break
                    else:
                        brand_list = brand_data[0].select(
                            scrape_config.RESULTS_BRAND_QUERY.get(retailer))
                    if brand_list:
                        for item in brand_list:
                            if retailer.lower() == "staples":
                                brand_text = "^" + \
                                    item["brandName"].strip().replace(
                                        "^", "") + "^"
                            elif retailer.lower() == "walmart":
                                brand_text = "^" + \
                                    item["title"].strip().replace(
                                        "^", "") + "^"
                            elif retailer.lower() == "bestbuy":
                                brand_text = "^" + \
                                    item["displayName"].strip().replace(
                                        "^", "") + "^"
                            elif retailer.lower() == "lowes":
                                brand_text = "^" + \
                                    item["data-brand"].strip().replace("^",
                                                                       "") + "^"
                            else:
                                brand_text = "^" + item.text.strip().replace("^", "") + "^"
                            if brand_text != "^Brands^":
                                all_brands.add(brand_text)
        except Exception as ex:
            logging.warning(
                "Excluding page due to error %s for retailer %s", ex, retailer)

        return list(all_brands)

    def extract_product_brand(self, brand_names, product_text):
        """Identify brand of specific product"""
        try:
            brand_found = False
            product_text = " " + product_text.strip().replace(u'\xa0', ' ')

            if brand_names != []:
                for brand in brand_names:
                    brand = " " + brand.replace("^", "").strip() + " "
                    if product_text.upper().startswith(brand.upper()):
                        product_text = brand + " :" + product_text
                        brand_found = True
                        break
                if not brand_found:
                    for brand in brand_names:
                        brand = " " + brand.replace("^", "").strip() + " "
                        if brand.upper() in product_text.upper():
                            product_text = brand + " :" + product_text
                            brand_found = True
                            break
                if not brand_found:
                    product_text = product_text.strip()
        except Exception as ex:
            logging.warning(
                "Error while getting brand name for product %s : %s", product_text, ex)
        return product_text, brand_found

    def get_default_product_brands(self,retailer,category):
        try:
            # brands_csv = scrape_config.BRANDS_MASTER_LIST_CSV_PATH
            # brands_df = pd.read_csv(brands_csv)
            client = bigquery.Client()
            data_query = "SELECT * FROM TABLE_ID_HERE WHERE retailer='RETAILER_NAME_HERE' and category='CATEGORY_HERE'"
            data_query = data_query.replace(
                "RETAILER_NAME_HERE", retailer).replace(
                "CATEGORY_HERE", category).replace(
                "TABLE_ID_HERE", scrape_config.BQ_BRANDS_MASTER_LIST_TABLE_ID)
            results = client.query(data_query)
            results_row = next(results.result())
            print("results_row dir:",dir(results_row))
            print("results_row: ",results_row)
            print("results_row get: ",results_row.get("brands"))
            print("results_row[0] get: ",results_row[0].get("brands"))

            print("results_row.values()",results_row.values())
            #data_exists_flag = results_row.values()[0]
            return brands_df
        except Exception as ex:
            logging.warning(
                "Error while loading  default brands : %s", ex)
            return None
        

    def get_retailer_specific_brands(self, brands_df, retailer, category):
        try:
            brand_names_default = []
            if brands_df is not None:
                dd = brands_df[(brands_df["retailer"] == retailer) & (
                    brands_df["category"] == category)]["brands"]
                brand_names_default = dd.values[0].strip("[]^").split("^,^")
        except Exception as ex:
            logging.warning(
                "Error while getting brand names for retailer csv %s with category %s : %s", retailer, category, ex)
            return None
        return brand_names_default

    def get_other_sponsored(self, soup_obj, retailer, sponsored_query, brand_names_default):
        """Extract sponsored product detailes present at top/bottom of the page"""
        try:
            other_sponsored_products = []
            sponsored_products_data = ""
            product_count = 0
            if sponsored_query:
                results_top_sponsored = soup_obj.select(
                    sponsored_query)
                if retailer.lower() == "cvs":
                    products_str = results_top_sponsored[0]["shelf-type"]
                    products_json = json.loads(products_str)
                    results_top_sponsored = products_json["criteoCarouselProducts"]["placements"][
                        0]["viewSearchResult_API_desktop-ingrid"][0]["products"]
                else:
                    results_top_sponsored = []
                for item in results_top_sponsored:
                    if retailer.lower() == "cvs":
                        product_text = item["ProductName"]
                    else:
                        product_text = item.text
                    # product_text = item.text
                    product_text = " ".join(product_text.split())
                    product_text = product_text.replace("^", "")

                    # product_text, brand_found = self.extract_product_brand(
                    #     brand_names_default, product_text)
                    # if not brand_found:
                    #     brand_names = self.get_new_product_brands(
                    #         soup_obj, retailer)
                    #     product_text, brand_found = self.extract_product_brand(
                    #         brand_names, product_text)
                    product_text = "^" + product_text + "^"

                    other_sponsored_products.append(product_text)
                    product_count += 1

            sponsored_products_data = ",".join(other_sponsored_products)
            sponsored_products_data = "[" + sponsored_products_data + "]"
            return sponsored_products_data, product_count
        except Exception as ex:
            print(
                f"Error while extracting other sponsored product for reatiler {retailer}: {ex}")
            return sponsored_products_data,product_count

    def generate_scrapes(self, retailer, soup, brand_names_default):
        """Extract product details from given html code"""
        page_text_sponsored = []
        page_text_organic = []
        organic_data_positions = []
        sponsored_data_positions = []
        sponsored_top_ten_count = 0
        sponsored_top_twenty_count = 0
        position = 0
        if retailer.lower() == "cvs":
            results_all = soup.find_all(
                "div", attrs={"class": "r-1pi2tsx", "id": re.compile("product-tile-*")})
        else:
            results_all = soup.select(
                scrape_config.RESULTS_ALL_QUERY.get(retailer))
        # print("len res all", len(results_all))
        if retailer.lower() == "lowes":
            sponsored_results = soup.select(
                scrape_config.ONLY_SPONSORED_RESULTS_QUERY.get(retailer))
        for result in results_all:
            position += 1

            if retailer.lower() == "amazon":
                extracted_position_tag = result.select("span.a-declarative")
                if extracted_position_tag:
                    extracted_position = extracted_position_tag[0].get(
                        "data-csa-c-posx")
                    if extracted_position and extracted_position.isdigit():
                        position = int(extracted_position)
                    elif not extracted_position:
                        position -= 1
                        continue
            if retailer.lower() == "lowes":
                result = [result, sponsored_results]
            if hasattr(self.scrapper_subroutines, f"{retailer.lower()}_subroutine") and callable(func := getattr(self.scrapper_subroutines, f"{retailer.lower()}_subroutine")):
                product_info = func(result)
            if not product_info:
                position -= 1
                continue
            product_info["position"] = position
            product_text = product_info["product_text"].replace("^", "")
            # product_text, brand_found = self.extract_product_brand(
            #     brand_names_default, product_text)

            # if not brand_found:
            #     brand_names = self.get_new_product_brands(soup, retailer)
            #     product_text, brand_found = self.extract_product_brand(
            #         brand_names, product_text)

            product_text = "^" + product_text + "^"

            product_info["product_text"] = product_text

            (
                sponsored_top_ten_flag,
                sponsored_top_twenty_flag
            ) = self.get_products_positions(
                product_info,
                page_text_organic,
                page_text_sponsored,
                organic_data_positions,
                sponsored_data_positions,
            )
            sponsored_top_ten_count += sponsored_top_ten_flag
            sponsored_top_twenty_count += sponsored_top_twenty_flag
        sponsored_products_data = ",".join(page_text_sponsored)
        sponsored_products_data = "[" + sponsored_products_data + "]"
        organic_products_data = ",".join(page_text_organic)
        organic_products_data = "[" + organic_products_data + "]"
        total_num_of_products = len(
            organic_data_positions) + len(sponsored_data_positions)
        if total_num_of_products < 20:
            sponsored_top_twenty_count = ""

        return (
            len(page_text_sponsored),
            str(sponsored_data_positions),
            sponsored_top_ten_count,
            sponsored_top_twenty_count,
            sponsored_products_data,
            len(page_text_organic),
            organic_products_data
        )

    def get_products_positions(
            self,
            product_data,
            page_text_organic,
            page_text_sponsored,
            organic_data_positions,
            sponsored_data_positions,
    ):
        """Get position/rank of specific product on current page"""
        sponsored_top_ten_flag = 0
        sponsored_top_twenty_flag = 0

        product_text = product_data["product_text"]
        position = product_data["position"]

        if product_data["sponsored_flag"]:
            page_text_sponsored.append(product_text)
            sponsored_data_positions.append(position)
            if position <= 10:
                sponsored_top_ten_flag += 1
                sponsored_top_twenty_flag += 1
            elif position <= 20:
                sponsored_top_twenty_flag += 1
        else:
            organic_data_positions.append(position)
            page_text_organic.append(product_text)

        return sponsored_top_ten_flag, sponsored_top_twenty_flag

    def get_retailer(self, filepath):
        """ Check if retailer is in allowed retailers list """

        for retailer in scrape_config.RETAILERS_LIST:
            if retailer.lower() in filepath:
                retailer_name = retailer
                break
        else:
            raise UnknownRetailer(
                f"Retailer not found for file {filepath}")

        return retailer_name

    def process_html(self, retailer_name, search_category, raw_df, brand_names_default,week_num, ind):
        """Process html to extract scrape data"""

        raw_extracts = []
        search_url = raw_df['web-scraper-start-url'][ind]
        job_id = None
        if "scraping-job-id" in raw_df.columns:
            job_id = raw_df['scraping-job-id'][ind]
        if isinstance(search_url, float):
            return
        html_content = raw_df['html'][ind]
        if isinstance(html_content, float):
            html_content = ""

        # Parsing html content
        soup = BeautifulSoup(html_content, 'html.parser')
        (
            no_of_sponsored,
            positions_of_sponsored,
            no_of_top_ten_sponsored,
            no_of_top_twenty_sponsored,
            sponsored_products,
            no_of_organic,
            organic_products,
        ) = self.generate_scrapes(retailer_name, soup, brand_names_default)

        # Extract sponsored products present at top of curent page
        top_sponsored_query = scrape_config.RESULTS_TOP_SPONSORED_QUERY.get(
            retailer_name)
        sponsored_products_at_top, sponsored_products_at_top_count = self.get_other_sponsored(
            soup, retailer_name,
            top_sponsored_query,
            brand_names_default
        )
        bottom_sponsored_query = scrape_config.RESULTS_BOTTOM_SPONSORED_QUERY.get(
            retailer_name)
        sponsored_products_at_bottom, sponsored_products_at_bottom_count = self.get_other_sponsored(
            soup, retailer_name,
            bottom_sponsored_query,
            brand_names_default
        )

        video_sponsored_query = scrape_config.RESULTS_VIDEO_SPONSORED_QUERY.get(
            retailer_name)
        sponsored_products_videos, sponsored_products_videos_count = self.get_other_sponsored(
            soup, retailer_name,
            video_sponsored_query,
            brand_names_default
        )

        if retailer_name.lower() == "staples":
            search_key = raw_df['web-scraper-start-url'][ind].strip().split("/")[-2]
        elif retailer_name.lower() in ["macys", "homedepot"]:
            search_key = raw_df['web-scraper-start-url'][ind].strip().split("/")[-1]
        else:
            search_key = raw_df['web-scraper-start-url'][ind].strip().split("=")[-1]

        time_scrapped = ""
        # print(raw_df.columns)
        if 'time-scraped' in raw_df.columns:
            time_scrapped = raw_df['time-scraped'][ind]
        today = datetime.datetime.utcnow()
        # week_num = str(today.year) + "-" + str(today.isocalendar()[1])
        week_num = week_num.replace("_","-")
        # week_num = "2024-9"
        page_type = "search"
        if search_category == "all":
            page_type = "category"
            search_key = '/'.join(raw_df['web-scraper-start-url']
                                  [ind].strip().split("/")[3:])

        scrape_info = [
            retailer_name,
            search_category,
            search_key,
            raw_df['web-scraper-start-url'][ind],
            no_of_sponsored,
            positions_of_sponsored,
            no_of_top_ten_sponsored,
            no_of_top_twenty_sponsored,
            sponsored_products,
            no_of_organic,
            organic_products,
            time_scrapped,
            datetime.datetime.now(tz=timezone.utc),
            "result_grid",
            "center",
            raw_df['web-scraper-order'][ind],
            job_id,
            page_type,
            week_num,
            "",
            ""
        ]
        raw_extracts.append(scrape_info)

        # sponsored products at top
        if sponsored_products_at_top_count:
            top_sponsored_scrapes = copy.deepcopy(scrape_info)
            top_sponsored_scrapes[4] = sponsored_products_at_top_count
            top_sponsored_scrapes[5] = str(list(
                range(1, sponsored_products_at_top_count+1)))
            top_sponsored_scrapes[6] = ""
            top_sponsored_scrapes[7] = ""
            top_sponsored_scrapes[8] = sponsored_products_at_top
            top_sponsored_scrapes[9] = ""
            top_sponsored_scrapes[10] = ""
            top_sponsored_scrapes[13] = "sponsored_brands"
            top_sponsored_scrapes[14] = "top"
            if retailer_name == "BestBuy":
                top_sponsored_scrapes[13] = "sponsored_carousel"
            raw_extracts.append(top_sponsored_scrapes)

        # sponsored products at bottom
        if sponsored_products_at_bottom_count > 0:
            bottom_sponsored_scrapes = copy.deepcopy(scrape_info)
            bottom_sponsored_scrapes[4] = sponsored_products_at_bottom_count
            bottom_sponsored_scrapes[5] = str(list(
                range(1, sponsored_products_at_bottom_count+1)))
            bottom_sponsored_scrapes[6] = ""
            bottom_sponsored_scrapes[7] = ""
            bottom_sponsored_scrapes[8] = sponsored_products_at_bottom
            bottom_sponsored_scrapes[9] = ""
            bottom_sponsored_scrapes[10] = ""
            bottom_sponsored_scrapes[13] = "sponsored_carousel"
            bottom_sponsored_scrapes[14] = "bottom"
            raw_extracts.append(bottom_sponsored_scrapes)

        # video sponsored products
        if sponsored_products_videos_count > 0:
            video_sponsored_scrapes = copy.deepcopy(scrape_info)
            video_sponsored_scrapes[4] = sponsored_products_videos_count
            video_sponsored_scrapes[5] = str(list(
                range(1, sponsored_products_videos_count+1)))
            video_sponsored_scrapes[6] = ""
            video_sponsored_scrapes[7] = ""
            video_sponsored_scrapes[8] = sponsored_products_videos
            video_sponsored_scrapes[9] = ""
            video_sponsored_scrapes[10] = ""
            video_sponsored_scrapes[13] = "sponsored_video"
            video_sponsored_scrapes[14] = "center"
            raw_extracts.append(video_sponsored_scrapes)

        return raw_extracts

    def process_csv_parallel(self, raw_df, retailer_name, search_category, out_data, brand_names_default, week_num):
        """ Process raw data from csv of retailer"""
        pool_args = [(retailer_name, search_category, raw_df, brand_names_default, week_num, i)
                     for i in raw_df.index]
        with Pool() as pool:
            for result in pool.starmap(self.process_html, pool_args):
                if result is not None:
                    out_data.extend(result)
