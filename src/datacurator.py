from dscodbclient import execute_db_transaction
from bson.json_util import dumps

retailer_ids = [
    1000003564,
    1000004878,
    1000000454,
]

def get_retailer_accounts(mongo_client, retailer_ids):
    retailer_account_cursor = mongo_client.DropshipCommon.Account.find({
        "account_id": {
            "$in": retailer_ids,
        }
    })
    return list(retailer_account_cursor)

def get_supplier_ids(mongo_client, retailer_ids):
    match_retailer_id = {
        "$match": {
            "retailer_id": {
                "$in": retailer_ids
            }
        }
    }
    project_supplier_id = {
        "$project": {
            "supplier_id": "$suppliers",
            "_id": 0,
        },
    }
    unwind_suppliers = {
        "$unwind": "$supplier_id",
    }
    group_unique_suppliers = {
        "$group": {
            "_id": None,
            "supplier_ids": {
                "$addToSet": "$supplier_id"
            }
        }
    }
    supplier_ids = mongo_client.DropshipCommon.Retailer.aggregate([
        match_retailer_id,
        project_supplier_id,
        unwind_suppliers,
        group_unique_suppliers,
    ]).next()["supplier_ids"]

    return supplier_ids

def get_active_supplier_accounts(mongo_client, supplier_ids):
    supplier_account_cursor = mongo_client.DropshipCommon.Account.find({
        "account_id": {
            "$in": supplier_ids,
        },
        "status": "active",
        "condition": {
            "$in": [
                "active",
                "onboarding",
            ],
        },
    })
    return list(supplier_account_cursor)

def build_accounts(mongo_client):
    retailer_accounts = get_retailer_accounts(mongo_client, retailer_ids)
    supplier_ids = get_supplier_ids(mongo_client, retailer_ids)
    supplier_accounts = get_active_supplier_accounts(mongo_client, supplier_ids)
    accounts = retailer_accounts + supplier_accounts
    insertManyResult = mongo_client.Puma.Account.insert_many(accounts)
    result = {
        "acknowledged": insertManyResult.acknowledged,
        "inserted_ids": insertManyResult.inserted_ids,
    }
    return result

def build_retailers(mongo_client):
    retailer_cursor = mongo_client.DropshipCommon.Retailer.find({
        "retailer_id": {
            "$in": retailer_ids
        }
    })
    retailers = list(retailer_cursor) # seems to be needed so that the same mongo_client is not doing two different things simultaneously
    insertManyResult = mongo_client.Puma.Retailer.insert_many(retailers)
    result = {
        "acknowledged": insertManyResult.acknowledged,
        "inserted_ids": insertManyResult.inserted_ids,
    }
    return result

def build_suppliers(mongo_client):
    match_suppliers = {
        "$match": {
            "account_type": "supplier"
        }
    }
    group_supplier_ids = {
        "$group": {
            "_id": None,
            "supplier_ids": {
                "$addToSet": "$account_id"
            }
        }
    }
    supplier_ids = mongo_client.Puma.Account.aggregate([
        match_suppliers,
        group_supplier_ids,
    ]).next()["supplier_ids"]
    supplier_ids = [int(sid) for sid in supplier_ids]

    supplier_cursor = mongo_client.DropshipCommon.Supplier.find({
        "supplier_id": {
            "$in": supplier_ids
        }
    })
    suppliers = list(supplier_cursor)

    insertManyResult = mongo_client.Puma.Supplier.insert_many(suppliers)
    result = {
        "acknowledged": insertManyResult.acknowledged,
        "inserted_ids": insertManyResult.inserted_ids,
    }
    return result

def exists_and_not_empty(field):
    return {
        "$and": [
            {
                field: {
                    "$exists": True
                }
            },
            {
                field: {
                    "$nin": [
                        None,
                        "",
                    ]
                }
            },
        ]
    }

def build_items(mongo_client):

    def get_items_by_supplier(supplier_id):
        match = {
            "$match": {
                "supplier_id": supplier_id,
                "status": "in-stock",
                "$and": exists_and_not_empty("upc")["$and"],
            }
        }
        sample = {
            "$sample": {
                "size": 2
            }
        }
        item_cursor = mongo_client.Catalog.Item.aggregate([
            match,
            sample,
        ])
        items = list(item_cursor)
        return items

    item_count = 0

    match_suppliers = {
        "$match": {
            "account_type": "supplier"
        }
    }
    group_supplier_ids = {
        "$group": {
            "_id": None,
            "supplier_ids": {
                "$addToSet": "$account_id"
            }
        }
    }
    sample = {
        "$sample": {
            "size": 200
        }
    }
    supplier_ids = mongo_client.Puma.Account.aggregate([
        match_suppliers,
        group_supplier_ids,
        sample,
    ]).next()["supplier_ids"]

    items = []
    for supplier_id in supplier_ids:
        supplier_id = int(supplier_id)
        items += get_items_by_supplier(supplier_id)
        if len(items) >= 200:
            break
    
    insertManyResult = mongo_client.Puma.Item.insert_many(items)
    result = {
        "acknowledged": insertManyResult.acknowledged,
        "inserted_ids": insertManyResult.inserted_ids,
    }
    return result

f = build_items
result = execute_db_transaction(f)
print dumps(result, sort_keys=True, indent=4, separators=(",", ": "))
