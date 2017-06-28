from dscodbclient import execute_db_transaction
from indixclient import IndixClient
from semantics3client import Semantics3Client
from bson.json_util import dumps
from datetime import datetime
from time import sleep
from lib.core import *
from traceback import print_exc

def get_items(mongo_client):
    item_cursor = mongo_client.Puma.Item.find()
    return list(item_cursor)

def save_indix_result(item, response, query):

    doc = {
        "api": "indix",
        "api_response": response,
        "api_query": query,
        "item": item,
    }

    def f(mongo_client):
        writeResult = mongo_client.Puma.Indix.insert(doc)
        return writeResult

    return execute_db_transaction(f)

def run_indix_api():
    indix = IndixClient()
    items = execute_db_transaction(get_items)
    for item in items:
        start_time = datetime.now()
        upc = item["upc"]
        print "upc:", upc,
        response, url = indix.query_products_universal(upc=upc)
        # print url
        # print dumps(response, sort_keys=True, indent=4, separators=(",", ": "))
        save_indix_result(item, response, url)
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        sleep_time = max(0, 1 - elapsed_time)
        print "| sleep_time:", sleep_time
        sleep(sleep_time)

def save_sem3_result(item, response, query):
    doc = {
        "api": "semantics3",
        "api_response": response,
        "api_query": query,
        "item": item,
    }

    def f(mongo_client):
        writeResult = mongo_client.Puma.Semantics3.insert(doc)
        return writeResult

    return execute_db_transaction(f)

def run_semantics3_api():
    sem3 = Semantics3Client()
    items = execute_db_transaction(get_items)
    for item in items[53:]:
        start_time = datetime.now()
        upc = item["upc"]
        if str(upc)[0] in ["2", "4"]: # semantics3.error.Semantics3Error: We do not track UPCs that start with "2" or "4"
            continue
        print "upc:", upc,
        try:
            response, query = sem3.query_products(upc=upc)
        except:
            print_exc
            continue

        save_sem3_result(item, response, query)
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        sleep_time = max(0, .5 - elapsed_time)
        print "| sleep_time:", sleep_time
        sleep(sleep_time)


def update(mongo_client):
    result = mongo_client.Puma.Indix.update_many({}, {
        "$rename": {
            "api_url": "api_query"
        }
    })
    return result

def results_analysis():
    '''
    for each item:
        was there a result?
        does the result (one of the results) match the item?
        who is the supplier?

    in aggregate:
        how many items had a result?
        how many items had a matching result?
        schema
    '''
    indix_results = {
        "items_queried": 200,
        "successful_queries": 179,          # failed because of period in field key
        "queries_with_results": 30,         # {"api_response.result.count": {"$gt": 0}}
        "queries_with_multiple_results": 7,  # {"api_response.result.count": {"$gt": 1}
        "supplier_ids": [1000006330, 1000006331, 1000006332, 1000006339, 1000006340, 1000006350, 1000006366, 1000006385, 1000006415, 1000006421, 1000006423, 1000006430, 1000006437, 1000006440, 1000006441, 1000006450, 1000006477, 1000006504, 1000006505, 1000006515, 1000006519, 1000006525, 1000006613],
    }
    sem3_results = {
        "items_queried": 200,
        "successful_queries": 190,          # failed because of period in field key
        "queries_with_results": 40,         # {"api_response.total_results_count": {"$gt": 0}}
        "queries_with_multiple_results": 0,  # {"api_response.total_results_count": {"$gt": 1}}
        "supplier_ids": [1000006339, 1000006345, 1000006350, 1000006362, 1000006366, 1000006385, 1000006415, 1000006420, 1000006421, 1000006423, 1000006430, 1000006440, 1000006441, 1000006450, 1000006454, 1000006457, 1000006477, 1000006515, 1000006523, 1000006525, 1000006551, 1000006562, 1000006571, 1000006584, 1000006592, 1000006613],
    }




if __name__ == "__main__":
    pass
