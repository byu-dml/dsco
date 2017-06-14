import bson
import json
import datetime
from random import randint
from tqdm import tqdm
import os

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
            type(None): "null",
            float: "Number",
        }

    def __before_analyze(self, collection):
        self.collection_name = collection.name
        self.schema = {}
        self.n_documents_analyzed = 0

    def analyze(self, collection, sample_size=100, chunk_size=None):
        self.__before_analyze(collection)

        collection_size = collection.count()
        if chunk_size == None or collection_size < sample_size * chunk_size:
            chunk_size = collection_size // sample_size + 1
        if collection_size <= sample_size:
            sample_size = collection_size
            chunk_size = 1
        print "collection size: {}, sample size: {}, chunk size: {}".format(collection_size, sample_size, chunk_size)
        cursor = collection.find({})
        for _ in tqdm(xrange(sample_size)):
            if self.n_documents_analyzed == collection_size % sample_size:
                chunk_size = max(1, chunk_size - 1)
            min_doc_index = self.n_documents_analyzed * chunk_size
            max_doc_index = (self.n_documents_analyzed + 1) * chunk_size - 1
            doc_index = randint(min_doc_index, max_doc_index)
            doc = cursor[doc_index]
            self.__recursive_analyze(doc)
            self.n_documents_analyzed += 1
            

    def __recursive_analyze(self, doc, prefix=""):
        if type(doc) is not dict:
            return

        for key, value in doc.iteritems():
            if key == None:
                print value
            if prefix == "":
                key_path = key
            else:
                key_path = prefix+"."+key
            self.__insert_field(key_path, type(value))
            if type(value) == type([]):
                for sub_doc in value:
                    self.__recursive_analyze(sub_doc, key_path)
            else:
                self.__recursive_analyze(value, key_path)

    def __insert_field(self, field_name, field_type):
        field_name = str(field_name)    # remove unicode prefix
        if field_type in self.field_type_map:
            field_type = self.field_type_map[field_type]
        else:
            raise Exception("field type '{}' not in field type map".format(field_type))

        if field_name not in self.schema:
            self.schema[field_name] = [{"type": field_type, "count": 1}]
        else:
            for item in self.schema[field_name]:
                if item["type"] == field_type:
                    item["count"] += 1
                    return
            self.schema[field_name].append({"type": field_type, "count": 1})

    def __repr__(self):
        return json.dumps(
            {
                "collection_name": self.collection_name,
                "schema": self.schema,
                "n_documents_analyzed": self.n_documents_analyzed
            },
            sort_keys=True, indent=4, separators=(",", ": ")
        )

    def save(self, directory=None):
        filename = "{}.json".format(self.collection_name)
        if directory is None:
            path = filename
        else:
            path = directory+filename
        with open(path, "w") as f:
            f.write(self.__repr__())

def get_cla_args(db=None, collection=None, sample_size=100, chunk_size=100):
    if len(sys.argv) < 2:
        raise Exception("too few cla arguments")
    elif len(sys.argv) > 5:
        raise Exception("too many cla arguments")
    
    db = sys.argv[1]
    if len(sys.argv) >= 3:
        collection = sys.argv[2]
    if len(sys.argv) >= 4:
        sample_size = int(sys.argv[3])
    if len(sys.argv) == 5:
        chunk_size = int(sys.argv[4])
    return db, collection, sample_size, chunk_size

def validate_cla_args(mongo_client, db, collection, sample_size, chunk_size):
    if not db in mongo_client.database_names():
        raise Exception("database '{}' does not exist".format(db))
    if collection != None and not collection in mongo_client[db].collection_names():
        raise Exception("database '{}' does not have a collection named '{}'".format(db, collection))

def run_schema_analyzer(mongo_client, db, collection, sample_size=None, chunk_size=None):
    print "analyzing {}.{}".format(db, collection)
    sa = SchemaAnalyzer()
    if sample_size == None:
        sa.analyze(mongo_client[db][collection])
    elif chunk_size == None:
        sa.analyze(mongo_client[db][collection], sample_size)
    else:
        sa.analyze(mongo_client[db][collection], sample_size, chunk_size)
    sa.save("../docs/database/{}/".format(db))

def main():
    dsco_client = DscoDBClient()
    try:
        dsco_client.start()
        mongo_client = dsco_client.getMongoDBClient()
        db, collection, sample_size, chunk_size = get_cla_args()
        validate_cla_args(mongo_client, db, collection, sample_size, chunk_size)

        if collection == None:
            for collection in mongo_client[db].collection_names():
                run_schema_analyzer(mongo_client, db, collection, sample_size, chunk_size)
        else:
            run_schema_analyzer(mongo_client, db, collection, sample_size, chunk_size)

    except Exception, e:
        print_exc()
    except KeyboardInterrupt:
        pass
    dsco_client.stop()

if __name__ == "__main__":
    import sys
    from traceback import print_exc
    from dscodbclient import DscoDBClient

    main()
