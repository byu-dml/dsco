from __future__ import print_function
from urllib import urlencode, urlopen
from urllib2 import URLError, HTTPError
import json

class IndixClient():

    def __init__(self):
        self.app_key =

    def _request(self, url, params=None):
        if params is None:
            params = {}
        params["app_key"] = self.app_key
        url += "?"+urlencode(params)

        try:
            response = urlopen(url)
        except HTTPError as e:
            response = e
        except URLError as e:
            print(e)
            print(url)

        response_data = response.read()
        query_result = json.loads(response_data)
        return query_result, url

    def query_stores(self, q):
        # 0 credits
        url = "https://api.indix.com/v2/stores"
        params = {"q": q}
        return self._request(url, params)

    def query_brands(self, q):
        # 0 credits
        url = "https://api.indix.com/v2/brands"
        params = {"q": q}
        return self._request(url, params)

    def get_categories(self):
        # 0 credits
        url = "https://api.indix.com/v2/categories"
        return self._request(url)

    def _query_products(self, url, **kwargs):
        param_keys = ['countryCode', 'q', 'storeId', 'alsoSoldAt', 'brandId', 'categoryId', 'url', 'upc', 'mpn', 'sku', 'startPrice', 'endPrice', 'availability', 'priceHistoryAvailable', 'priceChange', 'onPromotion', 'lastRecordedIn', 'storesCount', 'applyFiltersTo', 'selectOffersFrom', 'sortBy', 'facetBy', 'pageNumber', 'pageSize']

        if not kwargs:
            raise ValueError("query_products_summary requires at least one keyword argument")

        params = {}
        if "countryCode" not in kwargs:
            params["countryCode"] = "US"

        for key in param_keys:
            if key in kwargs:
                params[key] = kwargs[key]
        return self._request(url, params)

    def query_products_summary(self, **kwargs):
        # 2 credits per product returned
        url = 'https://api.indix.com/v2/summary/products'
        return self._query_products(url, **kwargs)

    def query_products_offers_standard(self, **kwargs):
        # 4 credits per product returned
        url = "https://api.indix.com/v2/offersStandard/products"
        return self._query_products(url, **kwargs)

    def query_products_offers_premium(self, **kwargs):
        # 4 credits per product returned
        url = "https://api.indix.com/v2/offersPremium/products"
        return self._query_products(url, **kwargs)

    def query_products_catalog_standard(self, **kwargs):
        # 18 credits per product returned
        url = "https://api.indix.com/v2/catalogStandard/products"
        return self._query_products(url, **kwargs)

    def query_products_catalog_premium(self, **kwargs):
        # 18 credits per product returned
        url = "https://api.indix.com/v2/catalogPremium/products"
        return self._query_products(url, **kwargs)

    def query_products_universal(self, **kwargs):
        # 22 credits per product returned
        url = "https://api.indix.com/v2/universal/products"
        return self._query_products(url, **kwargs)


if __name__ == '__main__':
    indix = IndixClient()
    #response = indix.query_stores("Nordstrom")
    #response = indix.query_brands("ECCO")
    #response = indix.query_brands(1)
    #response = indix.get_categories()

    #response = indix.query_products_summary(q="Cole Haan", storeId=41, pageSize=10) # Nordstrom storeId
    response = indix.query_products_summary(storeId=41, q="shoes") # Nordstrom storeId

    #response = indix.query_products_offers_standard(q="Cole Haan", storeId=41, pageSize=10)
    #response = indix.query_products_offers_standard(storeId=41, sku=190595172531)

    #response = indix.query_products_offers_premium(q="Cole Haan", storeId=41, pageSize=10)
    #response = indix.query_products_offers_premium(storeId=41, sku=190595172531)

    print(json.dumps(response, sort_keys=True, indent=2, separators=(",", ": ")))
