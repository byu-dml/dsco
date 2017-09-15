from __future__ import print_function
from semantics3 import Products, Offers, Categories, Semantics3Request

class Semantics3Client():

    def __init__(self):
        # bobby@byu.edu keys
        self.api_key = "SEM33840F42A4C497699D4605B0E1E462EC2"
        self.api_secret = "ZTQ3ZGQxYTk5MWY2NTE1YjUyYjUxNDEyNzZlZWQ5ZmQ"

    def query_categories(self, **kwargs):
        fields = ["cat_id", "crumb", "name", "parent_cat_id", "parent_name"]
        sem3 = Categories(self.api_key, self.api_secret)
        for field in fields:
            if field in kwargs:
                sem3.categories_field(field, kwargs[field])
                break # only one can be used
        return sem3.get_categories(), sem3.data_query

    def query_products(self, **kwargs):
        fields = ["search", "upc", "gtin", "ean", "sem3_id", "url", "site", "name", "cat_id", "variation_id"]
        sem3 = Products(self.api_key, self.api_secret)
        for field in fields:
            if field in kwargs:
                sem3.products_field(field, kwargs[field])
        return sem3.get_products(), sem3.data_query

    def query_offers(self, sem3_id, **kwargs):
        fields = ["firstrecorded_at", "lastrecorded_at", "sitedetails_name", "offset", "limit", "isotime"]
        sem3 = Offers(self.api_key, self.api_secret)
        sem3.offers_field("sem3_id", sem3_id)
        for field in fields:
            if field in kwargs:
                sem3.offers_field(field, kwargs[field])
        return sem3.get_offers(), sem3.data_query

    def query_skus(self, **kwargs):
        # from their website: "The SKUs resource only returns URLs that are active; if a URL goes dead, or the SKU goes out of stock, the corresponding SKU entry is no longer returned via the SKUs resource."
        fields = ["url", "site", "page", "sku", "fields", "isotime"]
        endpoint = "skus"
        sem3 = Semantics3Request(self.api_key, self.api_secret, endpoint, "https://api.semantics3.com/test/v1/")
        for field in fields:
            if field in kwargs:
                sem3.field(field, kwargs[field])
        return sem3.get(endpoint), sem3.data_query


if __name__ == "__main__":
    import json
    sem3_client = Semantics3Client()

    #response = sem3_client.query_categories(name="footwear")

    #response = sem3_client.query_products(search="Cole Haan", site="nordstrom.com", cat_id=8551) # footwear cat_id

    #response = sem3_client.query_offers(sem3_id="6TIbv7NZHEeak8iYQGMM8m", sitedetails_name="nordstrom.com") # loafers sem3_id

    #response = sem3_client.query_offers(sem3_id="6TIbv7NZHEeak8iYQGMM8m", sitedetails_name="nordstrom.com", lastrecorded_at={"$gte": 1497225600}) # loafers sem3_id, unix time UTC June 12, 2017 12:00:00 am

    #response = sem3_client.query_skus(site="nordstrom.com")

    response = sem3_client.query_products(site="nordstrom.com", search="2791386_35420823") # sku for MAC lipstick sold at Nordstrom

    print(json.dumps(response, sort_keys=True, indent=2, separators=(",", ": ")))
