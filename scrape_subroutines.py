"""Module containing subroutine for each retailer"""

import re


class Subroutines:
    """Contains subroutine for each retailer"""

    def amazon_subroutine(self, result):
        """Subroutine function to scrape data for Amazon retailer"""
        product_info = {}

        if 'AdHolder' in result.parent.parent.get('class', ""):
            data1 = result.select(
                'a.a-text-normal > span.a-text-normal')
            product_text = ""
            for k in data1:
                product_text = product_text + "," + k.text
            product_info["product_text"] = product_text.strip(",")
            product_info["sponsored_flag"] = True
        else:
            data1 = result.select(
                'h2', class_='a-size-base-plus a-color-base a-text-normal')
            product_text = ""
            for k in data1:
                product_text = product_text + "," + k.text
            product_info["product_text"] = product_text.strip(",")
            product_info["sponsored_flag"] = False

        return product_info

    def albertsons_subroutine(self, result):
        """Subroutine function to scrape data for Albertsons retailer"""
        product_info = {}
        product_info["product_text"] = result.find(
            "a", class_="product-title__name").text
        product_info["sponsored_flag"] = result.find(
            "div", class_="product-title__right-text")

        return product_info

    def bestbuy_subroutine(self, result):
        """Subroutine function to scrape data for Target retailer"""
        product_info = {}
        product_text = result.select("div.sku-title,h4.sku-title")
        if product_text:
            product_info["product_text"] = product_text[0].find("a").text
        else:
            return None
        product_info["sponsored_flag"] = result.find(
            "div", attrs={"class": "sku-item"})

        return product_info

    def cvs_subroutine(self, result):
        product_info = {}
        product_tag = result.find(
            "section")
        if not product_tag:
            product_info["product_text"] = ""
        else:
            product_info["product_text"] = product_tag.text
            
        sponsored_flag = None
        sponsored_query = result.find(
            "div", attrs={"class": "r-d9jaho"})
        if sponsored_query is not None:
            sponsored_query_text = sponsored_query.text
            if sponsored_query_text.strip().lower() == "sponsored":
                sponsored_flag = True
        product_info["sponsored_flag"] = sponsored_flag

        return product_info

    def homedepot_subroutine(self, result):
        """Subroutine function to scrape data for HomeDepot retailer"""
        product_info = {}
        product_tag = result.find(
            "div", attrs={"data-testid": "product-header"})
        if not product_tag:
            product_info["product_text"] = ""
        else:
            product_info["product_text"] = product_tag.text
        product_info["sponsored_flag"] = result.select(
            ".sui-inline-block span.sui-text-xs")


        return product_info

    def kroger_subroutine(self, result):
        """Subroutine function to scrape data for Target retailer"""
        product_info = {}
        product_info["product_text"] = result.find("span",
            attrs={"data-testid": "cart-page-item-description"}).text
        product_info["sponsored_flag"] = result.find(
            "span", attrs={"data-testid": "featured-product-tag"})

        return product_info

    def lowes_subroutine(self, results):
        """Subroutine function to scrape data for Target retailer"""
        product_info = {}
        result = results[0]
        sponsored_results = results[1]
        product_info["product_text"] = result.find(
            "article", attrs={"class": "brnd-desc"}).text
        product_info["sponsored_flag"] = False
        for item in sponsored_results:
            if item["data-tile"] == result["data-tile"]:
                product_info["sponsored_flag"] = True
        # if result.previous_sibling:
        #     product_info["sponsored_flag"] = 'lormn-badge' in result.previous_sibling.attrs['class']

        return product_info

    def macys_subroutine(self, result):
        """Subroutine function to scrape data for Macys retailer"""
        product_info = {}
        product_text = result.find(
            "div", attrs={"class": "productDescription"}
        ).find("a", attrs={"class": "productDescLink"}).text
        product_info["product_text"] = product_text.replace(
            "\n", "").replace("\t", "").strip()
        product_info["sponsored_flag"] = result.find(
            "div", attrs={"class": "sponsored-items-label"})

        return product_info

    def officedepot_subroutine(self, result):
        """Subroutine function to scrape data for OfficeDepot retailer"""
        product_info = {}
        product_text = result.find(
            "a", attrs={"class": "od-product-card-description"}
        )["title"]
        product_info["product_text"] = product_text.replace(
            "\n", "").replace("\t", "").strip()
        product_info["sponsored_flag"] = result.find(
            "div", attrs={"class": "sponsored-lable"})

        return product_info

    def staples_subroutine(self, result):
        """Subroutine function to scrape data for staples retailer"""
        product_info = {}
        product_text_tag = result.find(
            "div", attrs={"class": "standard-tile__title"})
        if product_text_tag:
            product_info["product_text"] = product_text_tag.find("a").text
        else:
            product_info["product_text"] = ""
        product_info["sponsored_flag"] = result.find(
            "div", attrs={"id": "sponsored-sku"})

        return product_info

    def target_subroutine(self, result):
        """Subroutine function to scrape data for Target retailer"""
        product_info = {}
        product_text_tag = result.find(
            "a", attrs={"data-test": "product-title"})
        if product_text_tag:
            product_info["product_text"] = product_text_tag.text
        else:
            product_text_tag = result.find(
                "h3", class_="h-sr-only")
            if product_text_tag:
                product_info["product_text"] = product_text_tag.text
            else:
                product_info["product_text"] = ""
        product_info["sponsored_flag"] = result.find(
            "p", class_="h-text-sm")

        return product_info

    def walmart_subroutine(self, result):
        """Subroutine function to scrape data for Walmart retailer"""
        product_info = {}
        product_info["product_text"] = result.find(
            "span", class_="w_iUH7").text
        product_info["sponsored_flag"] = 'track' in result["href"]

        return product_info
