from __future__ import print_function
import bson
import datetime
from random import randint
from tqdm import tqdm
from pymongo.cursor import Cursor
from lib.utils import bson_dump_pretty, dir_ensure_exists, verify

class SchemaAnalyzer(object):

    def __init__(self, value_to_type=None):
        if value_to_type is None:
            value_to_type = lambda value: {unicode: "String", list: "Array", int: "Number", dict: "Object", bson.int64.Int64: "Number", bson.objectid.ObjectId: "ObjectId", datetime.datetime: "DateTime", bool: "Boolean", type(None): None, float: "Number"}[type(value)]
        self.value_to_type = value_to_type

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
        print("collection name: {}, collection size: {}, sample size: {}, chunk size: {}".format(self.collection_name, collection_size, sample_size, chunk_size))

        min_doc_index = 0
        for _ in tqdm(range(sample_size)):
            if self.n_documents_analyzed == collection_size % sample_size:
                chunk_size = max(1, chunk_size - 1)
            max_doc_index = min_doc_index + chunk_size - 1
            doc_index = randint(min_doc_index, max_doc_index)
            doc = cursor[doc_index]
            self.__recursive_analyze(doc)
            self.n_documents_analyzed += 1
            min_doc_index += chunk_size

        return self.__get_schema()

    def __recursive_analyze(self, doc, field_path_prefix=""):
        if type(doc) is not dict:
            return

        for field, value in doc.iteritems():
            if field_path_prefix == "":
                field_path = field
            else:
                field_path = field_path_prefix+"."+field
            self.__add_field_to_schema(field_path, value)
            if type(value) == type([]):
                for sub_doc in value:
                    self.__recursive_analyze(sub_doc, field_path)
            else:
                self.__recursive_analyze(value, field_path)

    def __add_field_to_schema(self, field, value):
        field = str(field)    # remove unicode prefix
        try:
            value_type = self.value_to_type(value)
        except:
            raise Exception("value '{}' not defined in value_to_type function".format(value))

        if field not in self.schema:
            self.schema[field] = [{"type": value_type, "count": 1}]
        else:
            found_value_type = False
            for item in self.schema[field]:
                if item["type"] == value_type:
                    item["count"] += 1
                    found_value_type = True
                    break
            if not found_value_type:
                self.schema[field].append({"type": value_type, "count": 1})

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
    main()

    # def analyze_items(mongo_client):
    #     item_cursor = mongo_client.Puma.Item.find({"item_id": {"$in": [1026141608,1026141593,1026189333,1026098526,1026098528,1026089182,1026161739,1026048221,1026024329,1026024326,1026024327,1026121385,1026121631,1026007604,1026023889,1026126316,1026021957,1025986565,1025986494,1026091766,1025975056,1026082750,1026082752,1026023828,1025903283,1025903285,1026126412,1025979509,1025957290,1025957289,1025985631,1025985632,1026156095,1026154591,1025966205,1025966193,1026024893,1025910393,1025910359,1026066057,1026065958,1025871113,1025871036,1025475883,1026162928,1026162939,1026158054,1026158102,1025432960,1025431406,1025431419,1026066306,1025964975,1025996613,1025996506,1026141163,1026141041,1026154072,1026157002,1025902920,1025902937,1025306349,1025306341,1026026610,1025869038,1025869082,1025413556]}})
    #     analyze_schema(item_cursor, 200, 1, "./", "good_items")
    # execute_db_transaction(analyze_items)
