from __future__ import print_function
from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient
from traceback import print_exc

class DscoDBClient():

    # def __init__(self, ssh_pkey='/home/user/.ssh/id_rsa'):
    #     # manual ssh key authentication
    #     from getpass import getpass   # move to top of page
    #     MONGO_HOST =
    #     MONGO_USER =
    #     self.server = SSHTunnelForwarder(
    #         MONGO_HOST,
    #         ssh_username=MONGO_USER,
    #         remote_bind_address=('127.0.0.1', 27017),
    #         ssh_pkey=pkey,
    #         ssh_private_key_password=getpass('ssh private key password: ')
    #     )

    def __init__(self):
        # automatic ssh key authentication
        MONGO_HOST =
        MONGO_USER =
        self.remote_bind_ip = "127.0.0.1"
        self.server = SSHTunnelForwarder(
            MONGO_HOST,
            ssh_username=MONGO_USER,
            remote_bind_address=(self.remote_bind_ip, 27017)
        )

    def start(self):
        # connect to server and create mongodb client
        if not self.server.is_active:
            self.server.start()
            self.client = MongoClient(self.remote_bind_ip, self.server.local_bind_port)
        return self

    def stop(self):
        # disconnect from server
        if self.server.is_active:
            self.server.stop()
            self.client = None
        return self

    def getMongoDBClient(self):
        return self.client


    def __del__(self):
        if self.server.is_active == True:
            self.stop()

def execute_db_transaction(f):
    result = None
    dsco_client = DscoDBClient()
    try:
        dsco_client.start()
        mongo_client = dsco_client.getMongoDBClient()
        result = f(mongo_client)
    except Exception as e:
        print_exc()
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    dsco_client.stop()
    return result

if __name__ == "__main__":
    import json
    # def get_database_names(mongo_client):
    #     return mongo_client.database_names()
    
    # print(execute_db_transaction(get_database_names))

    def f(mongo_client):
        return mongo_client.DropshipCommon.Supplier.find({}, {"name": 1})

    result = execute_db_transaction(f)
    for item in result:
        print(json.dumps(item, sort_keys=True, indent=2, separators=(",", ": ")))
        break
