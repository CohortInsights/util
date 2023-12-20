import os
import yaml


def get_config(yaml_file : str = 'config.yaml') -> dict:
    """
    Get a configuration parsed from yaml_file located in a yaml_root directory

    Args:
        yaml_file (str) : Name of the yaml file

    Returns:
        (dict) : All configurations read from yaml resource

    """
    if not yaml_file:
        raise IOError("No yaml file specified!")

    yaml_root = "/.ssh"
    if not os.path.exists(yaml_root):
        user_dir = os.path.expanduser('~')
        yaml_root = os.path.join(user_dir,'.ssh')

    if not os.path.exists(yaml_root):
            raise ("No root path found for yaml configuration at '" + yaml_root + "'")

    yaml_file = os.path.join(yaml_root, yaml_file)
    if not os.path.exists(yaml_file):
        raise IOError("No such resource exists at '" + yaml_file + "'")

    # Now ready to open stream to yaml_file...
    with open(yaml_file,"r") as stream:
        yaml_config : dict = yaml.safe_load(stream)    # Read and parse yaml
        if yaml_config is None:
            raise IOError("Unable to read yaml configuration at '" + yaml_file + "'")
        return yaml_config
