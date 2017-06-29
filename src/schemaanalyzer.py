import bson
import datetime
from random import randint
from tqdm import tqdm
from pymongo.cursor import Cursor
from lib.utils import bson_dump_pretty, dir_ensure_exists, verify

class SchemaAnalyzer(object):

    def __init__(self):
        self.field_type_map = {
            unicode: "String",
            list: "Array",
            int: "Number",
            dict: "Object",
            bson.int64.Int64: "Number",
            bson.objectid.ObjectId: "ObjectId",
            datetime.datetime: "DateTime",
            bool: "Boolean",
            type(None): None,
            float: "Number",
        }

    def __before_analyze(self, collection_name):
        self.collection_name = collection_name
        self.n_documents_analyzed = 0
        self.schema = {}

    def __validate_analyze_params(self, cursor, sample_size, chunk_size, collection_name):
        if not isinstance(cursor, Cursor):
            raise ValueError("Expected cursor to be an instance of pymongo.cursor.Cursor, but is of type {}".format(type(cursor)))

        collection_size = cursor.count()
        if sample_size is None:
            sample_size = 100
        if collection_size <= sample_size:
            sample_size = collection_size
            chunk_size = 1
        elif chunk_size is None or collection_size < sample_size * chunk_size:
            chunk_size = collection_size // sample_size + 1     # the remainder is handled elsewhere

        if collection_name is None:
            collection_name = cursor.collection.name

        return cursor, sample_size, chunk_size, collection_name, collection_size    # collection_size returned to optimize runtime

    def analyze(self, cursor, sample_size=None, chunk_size=None, collection_name=None):
        cursor, sample_size, chunk_size, collection_name, collection_size = self.__validate_analyze_params(cursor, sample_size, chunk_size, collection_name)
        self.__before_analyze(collection_name)
        print "collection name: {}, collection size: {}, sample size: {}, chunk size: {}".format(self.collection_name, collection_size, sample_size, chunk_size)

        min_doc_index = 0
        for _ in tqdm(xrange(sample_size)):
            if self.n_documents_analyzed == collection_size % sample_size:
                chunk_size = max(1, chunk_size - 1)
            max_doc_index = min_doc_index + chunk_size - 1
            doc_index = randint(min_doc_index, max_doc_index)
            doc = cursor[doc_index]
            self.__recursive_analyze(doc)
            self.n_documents_analyzed += 1
            min_doc_index += chunk_size

        return self.__get_schema()

    def __recursive_analyze(self, doc, key_path_prefix=""):
        if type(doc) is not dict:
            return

        for key, value in doc.iteritems():
            if key_path_prefix == "":
                key_path = key
            else:
                key_path = key_path_prefix+"."+key
            self.__add_field_to_schema(key_path, type(value))
            if type(value) == type([]):
                for sub_doc in value:
                    self.__recursive_analyze(sub_doc, key_path)
            else:
                self.__recursive_analyze(value, key_path)

    def __add_field_to_schema(self, field_name, field_type):
        field_name = str(field_name)    # remove unicode prefix
        if field_type in self.field_type_map:
            field_type = self.field_type_map[field_type]
        else:
            raise Exception("field type '{}' not in field type map".format(field_type))

        if field_name not in self.schema:
            self.schema[field_name] = [{"type": field_type, "count": 1}]
        else:
            found_field_type = False
            for item in self.schema[field_name]:
                if item["type"] == field_type:
                    item["count"] += 1
                    found_field_type = True
                    break
            if not found_field_type:
                self.schema[field_name].append({"type": field_type, "count": 1})

    def __repr__(self):
        return bson_dump_pretty(self.__get_schema())

    def __get_schema(self):
        return {
            "collection_name": self.collection_name,
            "n_documents_analyzed": self.n_documents_analyzed,
            "schema": self.schema,
        }

    def save(self, directory=None):
        filename = self.collection_name+".json"
        if directory is None:
            path = filename
        else:
            dir_ensure_exists(directory, True)
            path = directory+filename
        with open(path, "w") as f:
            f.write(self.__repr__())

def analyze_schema(cursor, sample_size=None, chunk_size=None, save_dir=None, collection_name=None):
    sa = SchemaAnalyzer()
    schema = sa.analyze(cursor, sample_size, chunk_size, collection_name)
    if save_dir is not None:
        sa.save(save_dir)
    return schema

def get_cla_args(db_name=None, collection_name=None, sample_size=None, chunk_size=None):
    # python schemaanalyzer.py DropshipCommon                       # 2 all collections, default sample_size, default chunk_size
    # python schemaanalyzer.py DropshipCommon 100                   # 3 all collections, default chunk_size
    # python schemaanalyzer.py DropshipCommon Account               # 3 default sample_size, default chunk_size
    # python schemaanalyzer.py DropshipCommon 100       100         # 4 all collections
    # python schemaanalyzer.py DropshipCommon Account   100         # 4 default chunk_size
    # python schemaanalyzer.py DropshipCommon Account   100     100 # 5 database, collection, sample_size, chunk_size

    if len(sys.argv) < 2:
        raise ValueError("too few cla arguments")
    elif len(sys.argv) > 5:
        raise ValueError("too many cla arguments")
    
    db_name = sys.argv[1]

    if len(sys.argv) == 3:
        if is_numeric_int(sys.argv[2]):
            sample_size = int(sys.argv[2])
        else:
            collection_name = sys.argv[2]

    elif len(sys.argv) == 4:
        if is_numeric_int(sys.argv[2]):
            sample_size = int(sys.argv[2])
            chunk_size = int(sys.argv[3])
        else:
            collection_name = sys.argv[2]
            sample_size = int(sys.argv[3])

    elif len(sys.argv) == 5:
        collection_name = sys.argv[2]
        sample_size = int(sys.argv[3])
        chunk_size = int(sys.argv[4])

    return db_name, collection_name, sample_size, chunk_size

def main():
    db_name, collection_name, sample_size, chunk_size = get_cla_args()
    save_dir = "/home/user/dml/dsco/docs/database/schema/"+db_name+"/"

    def analyze_collection(mongo_client):
        verify(db_name in mongo_client.database_names(), "'{}' is not a valid database".format(db_name))
        verify(collection_name in mongo_client[db_name].collection_names(), "'{}' is not a valid collection".format(collection_name))
        cursor = mongo_client[db_name][collection_name].find()        
        analyze_schema(cursor, sample_size, chunk_size, save_dir)

    def analyze_database(mongo_client):
        verify(db_name in mongo_client.database_names(), "'{}' is not a valid database".format(db_name))
        for collection_name in mongo_client[db_name].collection_names():
            cursor = mongo_client[db_name][collection_name].find()
            analyze_schema(cursor, sample_size, chunk_size, save_dir)

    if collection_name is None:
        execute_db_transaction(analyze_database)
    else:
        execute_db_transaction(analyze_collection)

if __name__ == "__main__":
    import sys
    from dscodbclient import execute_db_transaction
    from lib.core import is_numeric_int
    # main()
    def analyze_items_with_semantics3_results(mongo_client):
        temp_collection_name = "temp"
        match = {
            "$match": {
                "api_response.results_count": {
                    "$gt": 0
                }
            }
        }
        replaceRoot = {
            "$replaceRoot": {
                "newRoot": "$item"
            }
        }
        out = {
            "$out": temp_collection_name
        }
        pipeline = [
            match,
            replaceRoot,
            out
        ]
        mongo_client.Puma.Semantics3.aggregate(pipeline)
        cursor = mongo_client["Puma"][temp_collection_name].find()
        analyze_schema(cursor, 200, 1, "/home/user/dml/dsco/docs/database/schema/", "semantics3_good_items")
        mongo_client.Puma[temp_collection_name].drop()

    execute_db_transaction(analyze_items_with_semantics3_results)
