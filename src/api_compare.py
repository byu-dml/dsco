from __future__ import print_function
from dscodbclient import execute_db_transaction
# from indixclient import IndixClient
# from semantics3client import Semantics3Client
import bson
from bson.json_util import dumps, loads
from datetime import datetime
from time import sleep
from lib.utils import *
from traceback import print_exc
from schemaanalyzer import SchemaAnalyzer, analyze_schema
from sklearn import cluster, metrics
import numpy as np
from tqdm import tqdm
import re

def get_items(mongo_client):
    item_cursor = mongo_client.Puma.Item2.find()
    return list(item_cursor)

def save_indix_result(item, response, query):

    doc = {
        "api": "indix",
        "api_response": response,
        "api_query": query,
        "item": item,
    }

    with open("Indix_2.jsonl", "a") as f:
        try:
            json_doc = dumps(doc, separators=(",", ":"), sort_keys=True)
            print(json_doc, file=f)
        except:
            print(doc)

    def f(mongo_client):
        writeResult = mongo_client.Puma.Indix_2.insert(doc)
        return writeResult

    return execute_db_transaction(f)

def run_indix_api():
    indix = IndixClient()
    items = execute_db_transaction(get_items)
    for item in items:
        start_time = datetime.now()
        upc = item["upc"]
        # print("upc:", upc,)
        response, url = indix.query_products_universal(upc=upc)
        save_indix_result(item, response, url)
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        sleep_time = max(0, 1 - elapsed_time)
        # print("| sleep_time:", sleep_time)
        sleep(sleep_time)

def save_sem3_result(item, response, query):
    doc = {
        "api": "semantics3",
        "api_response": response,
        "api_query": query,
        "item": item,
    }

    with open("Semantics3_2.jsonl", "a") as f:
        try:
            json_doc = dumps(doc, separators=(",", ":"), sort_keys=True)
            print(json_doc, file=f)
        except:
            print(doc)

    def f(mongo_client):
        writeResult = mongo_client.Puma.Semantics3_2.insert(doc)
        return writeResult

    return execute_db_transaction(f)

def run_semantics3_api():
    sem3 = Semantics3Client()
    items = execute_db_transaction(get_items)
    for item in items:
        start_time = datetime.now()
        upc = item["upc"]
        if str(upc)[0] in ["2", "4"]: # semantics3.error.Semantics3Error: We do not track UPCs that start with "2" or "4"
            continue
        # print("upc:", upc,)
        try:
            response, query = sem3.query_products(upc=upc)
        except:
            # print_exc
            continue

        save_sem3_result(item, response, query)
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        sleep_time = max(0, .5 - elapsed_time)
        # print("| sleep_time:", sleep_time)
        sleep(sleep_time)

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
    "there is no significant difference between the schema of the items which had good results and those which bad results for either API"
    

def compare_schema():
    with open("/home/user/dml/dsco/docs/database/schema/semantics3_bad_items.json", "r") as f:
        bad_schema_str = f.read()
    bad_schema = loads(bad_schema_str)["schema"]

    with open("/home/user/dml/dsco/docs/database/schema/semantics3_good_items.json", "r") as f:
        good_schema_str = f.read()
    good_schema = loads(good_schema_str)["schema"]

    for key, value in list(bad_schema.iteritems()):
        if key in good_schema:
            good_schema.pop(key)
            bad_schema.pop(key)

    print(bson_dump_pretty(bad_schema))
    print(bson_dump_pretty(good_schema))


def compare_field_types():

    def value_to_type(value):
        if value is None:
            return "null"
        if is_bool(value):
            return "bool"
        if is_numeric(value) or type(value) == bson.objectid.ObjectId:
            return "number"
        if is_str(value):
            return "string"
        if is_list(value):
            return "array"
        if is_dict(value):
            return "object"
        err("type not defined for value: {}".format(value))

    with open("../docs/database/schema/Puma/Item.json", "r") as f:
        schema = loads(f.read())["schema"]
    
    def item_dist(item_a, item_b):
        special_fields = ["__quantity_changes", "__cost_changes", "__last_actor", "__pricing_tiers"]
        dist = 0
        n = 0
        for key in schema:
            is_subfield_of_special_field = False
            for f in special_fields:
                if key != f and f in key:
                    is_subfield_of_special_field = True
                    break
            if is_subfield_of_special_field:
                continue

            if key in item_a and key in item_b:
                type_a = value_to_type(item_a[key])
                type_b = value_to_type(item_b[key])
                if type_a != type_b:
                    dist += 1
            elif key in item_a or key in item_b:
                dist += 1
            n += 1
        return dist #/ float(n)

    def get_items(mongo_client):
        item_cursor = mongo_client.Puma.Item.find()
        return list(item_cursor)
    
    items = execute_db_transaction(get_items)

    def metric(x, y):
        i, j = int(x[0]), int(y[0])
        dist = item_dist(items[i], items[j])
        return dist

    X = np.arange(len(items)).reshape(-1, 1)

    results = []
    for e in tqdm([1, 2, 4, 6, 8, 10, 15, 20]):
        dbscan_core_samples, dbscan_labels = cluster.dbscan(X, eps=e, metric=metric)
        results.append({
            "clust_method": "dbscan",
            "epsilon": e,
            "labels": dbscan_labels,
        })

    docs = []
    for i, item in enumerate(items):
        cluster_results = []
        for r in results:
            cluster_results.append({
                "epsilon": r["epsilon"],
                "label": r["labels"][i]
            })
        docs.append({
            "item": item,
            "dbscan_labels": cluster_results
        })

    def save_cluster_results(mongo_client):
        for doc in docs:
            indix_cursor = mongo_client.Puma.Indix.find({
                "item._id": doc["item"]["_id"]
            })
            indix_result_count = -1
            if indix_cursor.count() == 1:
                indix_doc = indix_cursor.next()
                if indix_doc["api_response"]["message"] == "ok":
                    indix_result_count = indix_doc["api_response"]["result"]["count"]
            elif indix_cursor.count() > 1:
                raise Exception("duplicate items queried")

            sem3_cursor = mongo_client.Puma.Semantics3.find({
                "item._id": doc["item"]["_id"]
            })
            sem3_result_count = -1
            if sem3_cursor.count() == 1:
                sem3_result_count = sem3_cursor.next()["api_response"]["results_count"]
            elif sem3_cursor.count() > 1:
                raise Exception("duplicate items queried")

            doc["indix_result_count"] = indix_result_count
            doc["semantics3_result_count"] = sem3_result_count
            mongo_client.Puma.Item_cluster.insert(doc)

    execute_db_transaction(save_cluster_results)

def rough_set_intersection(set_a, set_b):
        result = set()
        for a in set_a:
            for b in set_b:
                if a in b:
                    result.add(a)
                elif b in a:
                    result.add(b)
        return result

def json_object_to_word_bag(json_obj, delimiter=r"[^\w\d]+"):
    bag = set()
    callback = lambda key, value: bag.update(re.split(delimiter, str(value).lower()))
    traverse_json_object(json_obj, callback)
    return bag

def clean_word_bag(bag, remove_words=set(["","0","1","false","true"]), min_word_length=3, strip_chars="0"):
    bag -= remove_words
    bag = set(filter(lambda x: len(x) >= min_word_length, bag))
    for c in strip_chars:
        bag = set(map(lambda x: x.strip(c), bag))
    return bag

def bag_of_words():

    def items_and_api_results(mongo_client):
        api_cursor = mongo_client.Puma.API.find({
            "$or": [
                {"api_response.result.count": {"$gt": 0}},
                {"api_response.results_count": {"$gt": 0}},
            ]
        })
        items = []
        api_results = []
        for doc in api_cursor:
            items.append(doc["item"])
            api_results.append(doc["api_response"])
        return items, api_results

    items, api_results = execute_db_transaction(items_and_api_results)

    for item, api_result in zip(items, api_results):
        item_bag = clean_word_bag(json_object_to_word_bag(item))
        api_result_bag = clean_word_bag(json_object_to_word_bag(api_result))
        words_in_common = item_bag.intersection(api_result_bag)
        print(item_bag)
        print(api_result_bag)
        print(words_in_common)
        input()

def get_supplier_item_frequency():

    def f(mongo_client):
        group = {
            "$group": {
                "_id": {
                    "supplier_name": "$item.__supplier_name",
                    "supplier_id": "$item.supplier_id",
                },
                "indix_result_count": {
                    "$sum": {
                        "$cond": {
                            "if": {
                                "$and": [
                                    {
                                        "$eq": ["$api", "indix"]
                                    },
                                    {
                                        "$gt": ["$api_response.result.count", 0]
                                    }
                                ]
                            },
                            "then": 1,
                            "else": 0,
                        }
                    }
                },
                "semantics3_result_count": {
                    "$sum": {
                        "$cond": {
                            "if": {
                                "$and": [
                                    {
                                        "$eq": ["$api", "semantics3"]
                                    },
                                    {
                                        "$gt": ["$api_response.results_count", 0]
                                    }
                                ]
                            },
                            "then": 1,
                            "else": 0,
                        }
                    }
                },
            }
        }
        lookup = {
            "$lookup": {
                "from": "Item",
                "localField": "_id.supplier_id",
                "foreignField": "supplier_id",
                "as": "items"
            }
        }
        sort = {
            "$sort": {
                "_id.supplier_name": 1
            }
        }
        project = {
            "$project": {
                "_id": 0,
                "supplier_name": "$_id.supplier_name",
                "supplier_id": "$_id.supplier_id",
                "item_count": {
                    "$size": "$items"
                },
                "indix_result_count": "$indix_result_count",
                "semantics3_result_count": "$semantics3_result_count",
            }
        }
        group2 = {
            "$group": {
                "_id": None,
                "item_count": {
                    "$sum": "$item_count"
                }
            }
        }
        pipeline = [
            group,
            lookup,
            sort,
            project,
            # group2,
        ]
        result = mongo_client.Puma.API.aggregate(pipeline)
        return list(result)

    result = execute_db_transaction(f)
    print(dumps(result, indent=4, separators=(",", ": ")))

if __name__ == "__main__":
    # run_indix_api()
    # run_semantics3_api()
    # compare_schema()
    # compare_field_types()
    bag_of_words()
    # def f(mongo_client):
        
    #     with open("temp.jsonl", "r") as f:
    #         for line in f:
    #             line = line.strip()
    #             if line != "":
    #                 doc = loads(line)
    #                 result = mongo_client.Puma.Indix_2.insert(doc)
    #                 print(result)

    # execute_db_transaction(f)
    # get_supplier_item_frequency()
