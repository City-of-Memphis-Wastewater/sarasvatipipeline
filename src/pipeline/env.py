#env.__main__.py
 
import yaml
from src.pipeline.projectmanager import ProjectManager 

class SecretsYaml:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def load_config(secrets_file_path): 
        with open(secrets_file_path, 'r') as f:
            return yaml.safe_load(f)
        
    def print_config(self):
        # Print the values
        for section, values in self.config.items():
            print(f"[{section}]")
            for key, val in values.items():
                print(f"{key} = {val}")


def find_urls(config, url_set=None):
    '''determine all values with the key "url" in a config file.'''
    if url_set is None:
        url_set = set()

    if isinstance(config, dict):
        for key, value in config.items():
            if key == "url":
                url_set.add(value)
            else:
                find_urls(value, url_set)
    elif isinstance(config, list):
        for item in config:
            find_urls(item, url_set)

    return url_set

def demo_secrets():
    """
    The defaut SecretsYaml.load_config() call 
    should load fromthe default-project 
    as defined by the configuration file in the projects directorys,
    caed defaut_project.toml - Clayton Bennett 26 April 2025.
    However this call can also be made if another project is made the active project.
    """
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    config = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    secrets = SecretsYaml(config)
    secrets.print_config()
    return secrets

if __name__ == "__main__":
    # call from the root directory using poetry run python -m pipeline.env
    secrets=demo_secrets()
