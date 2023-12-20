from pymongo.mongo_client import MongoClient

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