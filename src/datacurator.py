from dscodbclient import execute_db_transaction
import json

def good_retailers(mongo_client):
    result = mongo_client.DropshipCommon.Retailer.find(
        {
            "retailer_id": {
                "$in": [
                    1000003564,
                    1000004878,
                    1000000454,
                ]
            }
        },
        {
            "account_status": 1,
            "name": 1,
            "retailer_id": 1,
            "status": 1,
            "suppliers": 1,
            "website": 1,
            "_id": 0,
        }
    )
    return list(result)

def good_suppliers(mongo_client):
    match_retailer_id = {
        "$match": {
            "retailer_id": {
                "$in": [
                    1000003564,
                    1000004878,
                    1000000454,
                ],
            },
        },
    }
    project_retailers = {
        "$project": {
            "account_status": 1,
            "name": 1,
            "retailer_id": 1,
            "status": 1,
            "supplier_id": "$suppliers",
            "website": 1,
            "_id": 0,
        },
    }
    unwind_suppliers = {
        "$unwind": "$supplier_id",
    }
    group_supplier_ids = {
        "$group": {
            "_id": None,
            "supplier_ids": {
                "$addToSet": "$supplier_id",
            },
        },
    }
    lookup_suppliers = {
        "$lookup": {
            "from": "Supplier",
            "localField": "supplier_id",
            "foreignField": "supplier_id",
            "as": "supplier",
        },
    }
    project_first_lookup = {
        "$project": {
            "supplier": {
                "$arrayElemAt": ["$supplier", 0],
            },
        },
    }
    project_suppliers = {
        "$project": {
            "supplier_id": "$supplier.supplier_id",
            "name": "$supplier.name",
            "account_status": "$supplier.account_status",
            "website": "$supplier.website",
        },
    }
    match_active_suppliers = {
        "$match": {
            "account_status": "active",
        },
    }
    count_suppliers = {
        "$count": "supplier_count"
    }
    result = mongo_client.DropshipCommon.Retailer.aggregate(
        [
            match_retailer_id,
            project_retailers,
            unwind_suppliers,
            #group_supplier_ids,
            lookup_suppliers,
            project_first_lookup,
            project_suppliers,
            match_active_suppliers,
            #count_suppliers,
        ]
    )
    return list(result)

result = execute_db_transaction(good_suppliers)
print json.dumps(result, sort_keys=True, indent=4, separators=(",", ": "))
