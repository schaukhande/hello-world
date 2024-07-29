""" Configuartions of web scrapper"""

RETAILERS_LIST = [
    "Amazon",
    "Albertsons",
    "BestBuy",
    "HomeDepot",
    "Kroger",
    "Lowes",
    "Macys",
    "Staples",
    "Target",
    "Walmart",
    "Cvs",
    "OfficeDepot"
]
SCRAPPING_JOBS_URL = 'https://api.webscraper.io/api/v1/scraping-jobs?api_token=<API_TOKEN>&page=<PAGE>'
SCRAPPING_JOB_CSV_URL = "https://api.webscraper.io/api/v1/scraping-job/<JOB_ID>/csv?api_token=<API_TOKEN>"
LOGGING_FILE_NAME = "scrapper_debug.log"
# RESULTS_ALL_QUERY = {
#     "Amazon": "div.s-result-item > div.sg-col-inner > div.s-widget-container",
#     "Albertsons": "product-item-v2",
#     "BestBuy": "li.sku-item,li.embedded-sponsored-listing",
#     "HomeDepot": "div.browse-search__pod:not(:-soup-contains('Get It Fast'))",
#     "Kroger": "div.ProductCard",
#     "Lowes": "div.DescriptionHolderStyle-sc-4v6c0e-23",
#     "Macys": "li.productThumbnailItem",
#     "Staples": "div.SearchResults-UX2__searchGridColumn",
#     "Target": "div.styles__StyledCardWrapper-sc-z8946b-0",
#     "Walmart": "div.pb1-xl a.absolute",
# }
RESULTS_ALL_QUERY = {
    "Amazon": "div.s-result-item > div.sg-col-inner > div.s-widget-container",
    "Albertsons": "div.product-card-container",
    "BestBuy": "li.sku-item,li.embedded-sponsored-listing",
    "HomeDepot": "div.browse-search__pod:not(:-soup-contains('Get It Fast'))",
    "Kroger": "div.ProductCard",
    "Lowes": "div.DescriptionHolderStyle-sc-4v6c0e-23",
    "Macys": "li.productThumbnailItem",
    "Staples": "div.SearchResults-UX2__searchGridColumn",
    "Target": "div[data-test='product-details']",
    "Walmart": "div.ph0-xl a.absolute",
    "OfficeDepot": "div.od-search-browse-products-vertical-grid-product",
}
ONLY_SPONSORED_RESULTS_QUERY = {
    "Lowes": "div.lormn-badge",
}
RESULTS_TOP_SPONSORED_QUERY = {
    "Walmart": ".absolute a.absolute",
    "BestBuy": "div.product-flexbox",
    "Staples": "div.standard-tile__top_section_modifier"
}

RESULTS_BOTTOM_SPONSORED_QUERY = {
    "Walmart": ".carousel-6-l .sans-serif.h-100 a",
    "HomeDepot": "div.sponsored-product-pod",
    "Macys": "#sponsoredCarouselSSR .carousel li",
    "Cvs": "div.css-1dbjc4n > cvs-shop-product-shelf-container"
}
RESULTS_VIDEO_SPONSORED_QUERY = {
    "Walmart": ".fr a"
}


INCLUDE_BRANDS = False

RESULTS_ALL_BRAND_LIST_QUERY = {
    "Amazon": "div#brandsRefinements",
    "Albertsons": "div#brand",
    "HomeDepot": "div.dimension",
    "Lowes": "section.pl[data-selector='splp-prd-lst-pl']",
    "Macys": "div.product-grid-wider-exp",
    "Staples": "script#__NEXT_DATA__",
    "Target": "section.styles__StyledRowWrapper-sc-z8946b-1",
    "Walmart": "script#__NEXT_DATA__",
    "BestBuy": "script",
    "OfficeDepot": "div.od-refinements-facet:contains('Brand')"
}
RESULTS_BRAND_QUERY = {
    "Amazon": "span.a-size-base",
    "Albertsons": "span.facet__label__text",
    "HomeDepot": ".dimension__group h3",
    "Lowes": "div.img_top div.js-save-to-list",
    "Macys": "div.productBrand",
    "Target": "div.ProductCardBrandAndRibbonMessage__BrandAndRibbonWrapper-sc-2i9l6s-0 a",
    "OfficeDepot": "span.od-checkbox-text"
}
WEBSCRAPPER_IO_REQUEST_TIMEOUT = 600
GCS_ARTIFACTS_BUCKET_NAME = "us.artifacts.crealytics-rm-market-data-stg.appspot.com"
SCRAPPER_RAW_DATA_BLOB_NAME = "web_scrapper_io_artifacts"
SCRAPPER_PROCESSED_DATA_BLOB_NAME = "rm-market-data-scrape-artifacts"
# commenting for testing
BQ_STAGING_WEEKLY_SCRAPES_TABLE_ID = "crealytics-rm-market-data-stg.scrape_data_analysis.staging_weekly_data"
# BQ_STAGING_WEEKLY_SCRAPES_TABLE_ID = "crealytics-rm-market-data-stg.scrape_data_analysis.test_staging_weekly_data"
# BQ_CATEGORY_WEEKLY_SCRAPES_TABLE_ID = "crealytics-rm-market-data-stg.scrape_data_analysis.test_staging_weekly_data"
# commenting for testing
BQ_CATEGORY_WEEKLY_SCRAPES_TABLE_ID = "crealytics-rm-market-data-stg.scrape_data_analysis.category_weekly_scrape_data"

BQ_BRANDS_MASTER_LIST_TABLE_ID = "crealytics-rm-market-data-stg.scrape_data_analysis.brands_master_list"