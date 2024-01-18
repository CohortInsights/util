from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

import traceback
from util.log import LogEvent


def connect(uri : str, user : str, password : str, ping_test = True) -> MongoClient:
    """
    Connect to uri using specified credentials and return MongoClient connection

    Args:
        uri (str) : Template uri containing <user> and <password>
        user (str) : Name for <user>
        password (str): Credential read from configuration
        ping_test (bool) : Whether to perform ping test on returned connection

    Returns:
        (MongoClient): Client connection object
    """
    uri = uri.replace("<user>", user)
    info = {'uri': uri }
    uri = uri.replace("<password>", password)
    # Create a new client and connect to the server
    event = LogEvent("mongo_connect")
    client = MongoClient(host=uri)
    if ping_test:
        try:
            client.admin.command('ping')
            info['status'] = 'success'
        except Exception as e:
            traceback.print_exception(e)
            info['status'] = 'failed'
        event.properties(info)
    if client:
        def close_and_log():
            _close, _event, _info = client._info    # Get states from client
            _close()    # Invoke native close method
            _info['status'] = 'closed'
            _event.properties(_info)
        # Save local state in client
        client._info = (client.close, event, info)
        client.close = close_and_log    # Override native close method with close_and_log
    return client


def replace_values(expression : dict, map : dict, key_list : list[str] = None) -> dict:
    """
    Replace all keys in the input expression with their corresponding map values
    Args:
        expression: Input dictionary
        map: Map of keys and values
        key_list: Optional selected list of keys from the map

    Returns:
        Expression with all @key tokens replaced with their corresponding values from the map
    """
    if key_list is None:
        key_list = map.keys()

    for k in key_list:
        value = map.get(k)
        if value:
            expression[k] = value
    return expression


def get_parameter(operation : dict, name : str, meta_data : dict = None, default_value = None, error_check = True):
    value = operation.get(name)
    if not value and meta_data:
        value = meta_data.get(value)
    if not value and default_value:
        value = default_value
    if not value and error_check:
        message = str.join("No ",name, " present in ",operation," or meta data")
        raise Exception(message)
    return value


class MongoDB:
    """
    Wrapper around a Mongo Database that uses meta data to provide store and query operations
    """
    def __init__(self, client : MongoClient, meta_data : dict):
        """
        Constructor

        Args:
            database : Instance of a Mongo database
            meta_data: Meta data used to form queries, filters, and configured operations
        """
        self.client = client
        self.meta_data = meta_data

    def get_store_callback(self, name : str):
        """
        Gets a callback function that stores an item in the database

        Args:
            name: Name of store operation derived from meta data

        Returns:
            Function that stores an item in the underlying DB
        """
        operations = self.meta_data.get('operations')
        store_oper = operations.get(name)
        if store_oper is None:
            return None

        database_name = get_parameter(store_oper, 'database', meta_data=self.meta_data)
        collection_name = get_parameter(store_oper, 'collection', meta_data=self.meta_data)
        collection : Collection = self.client.get_database(database_name).get_collection(collection_name)
        if collection is None:
            return None

        key_list = get_parameter(store_oper, 'key_list')
        filter = get_parameter(store_oper, 'filter')
        upsert = get_parameter(store_oper, 'upsert', default_value=True)

        def store_value(item : dict):
            if filter:
                _filter = replace_values(filter, item, key_list)
                return collection.replace_one(filter=_filter, replacement=item, upsert=upsert)
            else:
                return collection.insert_one(item)

        return store_value



