from pymongo.mongo_client import MongoClient

import os
import yaml
import traceback
from util.log import LogEvent

yaml_default_location = "<user_home>/.ssh"


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


def get_connection_info(connection_name: str, yaml_file='mongo.yaml', yaml_location=None) -> dict:
    """
    Get mongo client connection configuration for the named connection

    Args:
        connection_name (str) : Name of the connection within the yaml config
        yaml_file (str) : Name of the yaml file of all available mongo configurations
        yaml_location (str) : Path of yaml configuration file (overrides default if specified)

    Returns:
        (dict) : Named configuration read from yaml resource

    """
    if connection_name is None:
        raise IOError("Connection name must be specified")

    if yaml_location is None:
        # Assign yaml_location from default location
        yaml_location = yaml_default_location
        # Get user home directory
        user_home = os.path.expanduser('~')
        # Replace token within default location
        yaml_location = yaml_location.replace("<user_home>", user_home)

    if not os.path.exists(yaml_location):
        raise IOError("Not able to access directory at '" + yaml_location + "'")

    yaml_file = os.path.join(yaml_location, yaml_file)
    if not os.path.exists(yaml_file):
        raise IOError("Not able to access yaml configuration file at '" + yaml_file + "'")

    # Now ready to open stream to yaml_file...
    with open(yaml_file,"r") as stream:
        yaml_config : dict = yaml.safe_load(stream)    # Read and parse yaml
        if yaml_config is None:
            raise IOError("Unable to read yaml configuration at '" + yaml_file + "'")
        # Get named configuration
        selected_config : dict = yaml_config.get(connection_name)
        if selected_config is None:
            raise Exception("No key exists in yaml configuration for '" + connection_name + "'")
        return selected_config