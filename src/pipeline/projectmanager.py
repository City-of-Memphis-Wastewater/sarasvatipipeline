import os
import toml
import logging
from pathlib import Path
import sys

'''
Goal:
Implement default-project.toml variable: use-most-recently-edited-directory 
'''

class ProjectManager:
    # It has been chosen to not make the ProjectManager a singleton if there is to be batch processing.
    
    PROJECTS_DIR_NAME = 'projects'
    QUERIES_DIR_NAME = 'queries'
    IMPORTS_DIR_NAME = 'imports'
    EXPORTS_DIR_NAME = 'exports'
    SCRIPTS_DIR_NAME = 'scripts'
    CONFIGS_DIR_NAME ='secrets'
    SECRETS_YAML_FILE_NAME ='secrets.yaml'
    SECRETS_EXAMPLE_YAML_FILE_NAME ='secrets-example.yaml'
    DEFAULT_PROJECT_TOML_FILE_NAME = 'default-project.toml'
    
    def __init__(self, project_name):
        self.project_name = project_name
        self.base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        #self.project_dir = os.path.join(self.base_dir, self.PROJECTS_DIR_NAME, project_name)
        self.project_dir = self.get_project_dir()
        self.exports_dir = self.get_exports_dir()
        self.create_exports_dir()
        self.imports_dir = self.get_imports_dir()
        self.configs_dir = self.get_configs_dir()
        self.scripts_dir = self.get_scripts_dir()
        self.aggregate_dir = self.get_aggregate_dir()

    def get_exports_dir(self):
        return os.path.join(self.project_dir, self.EXPORTS_DIR_NAME)

    def get_exports_file_path(self, filename):
        # Return the full path to the export file
        return os.path.join(self.exports_dir, filename)

    def get_aggregate_dir(self):
        # This is for five-minute aggregation data to be stored between hourly bulk passes
        # This should become defunct once the tabular trend data request is functional 
        return os.path.join(self.exports_dir, 'aggregate')
    
    def create_exports_dir(self):
        if not os.path.exists(self.exports_dir):
            os.makedirs(self.exports_dir)

    def get_imports_dir(self):
        return os.path.join(self.project_dir, self.IMPORTS_DIR_NAME)

    def get_imports_file_path(self, filename):
        # Return the full path to the export file
        return os.path.join(self.imports_dir, filename)

    def create_imports_dir(self):
        if not os.path.exists(self.imports_dir):
            os.makedirs(self.imports_dir)

    def get_configs_dir(self):
        return os.path.join(self.project_dir, self.CONFIGS_DIR_NAME)

    def get_configs_secrets_file_path(self):
        # Return the full path to the config file
        file_path = os.path.join(self.configs_dir, self.SECRETS_YAML_FILE_NAME)
        if not os.path.exists(file_path):
            logging.warning(f"Configuration file {self.SECRETS_YAML_FILE_NAME} not found in:\n{self.configs_dir}.\nHint: Copy and edit the {self.SECRETS_YAML_FILE_NAME}.")
            print("\n")
            choice = str(input(f"Auto-copy {self.SECRETS_EXAMPLE_YAML_FILE_NAME} [Y] or sys.exit() [n] ? "))
            if choice.lower().startswith("y"):
                file_path = self.get_configs_secrets_file_path_or_copy()
            else:
                sys.exit()
        return file_path
    
    def get_configs_secrets_file_path_or_copy(self):
        # Return the full path to the config file or create it from the fallback copy if it exists
        file_path = os.path.join(self.configs_dir, self.SECRETS_YAML_FILE_NAME)
        fallback_file_path = os.path.join(self.configs_dir, self.SECRETS_EXAMPLE_YAML_FILE_NAME)
        if not os.path.exists(file_path) and os.path.exists(fallback_file_path):
            import shutil
            shutil.copy(fallback_file_path, file_path)
            print(f"{self.SECRETS_YAML_FILE_NAME} not found, copied from {self.SECRETS_YAML_FILE_NAME}")
        elif not os.path.exists(file_path) and not os.path.exists(fallback_file_path):
            raise FileNotFoundError(f"Configuration file {self.SECRETS_YAML_FILE_NAME} nor {self.SECRETS_EXAMPLE_YAML_FILE_NAME} not found in directory '{self.configs_dir}'.")
        return file_path
    
    def create_configs_dir(self):
        if not os.path.exists(self.configs_dir):
            os.makedirs(self.configs_dir)

    def get_scripts_dir(self):
        return os.path.join(self.project_dir, self.SCRIPTS_DIR_NAME)

    def get_scripts_file_path(self, filename):
        # Return the full path to the config file
        return os.path.join(self.scripts_dir, filename)
    
    def create_scripts_dir(self):
        if not os.path.exists(self.scripts_dir):
            os.makedirs(self.scripts_dir)
    
    def get_queries_dir(self):
        return os.path.join(self.get_project_dir(), self.QUERIES_DIR_NAME)
    
    def get_queries_file_path(self,filename='points.csv'): # default fallback filename
        # Return the full path to the config file
        #! Migrate this function to the QueryManager class,
        #if filename is str: # have different behavior is a full path is fed vs just a file name expected in the queries directory
        return os.path.join(self.get_queries_dir(), filename)

    def get_projects_dir(self):
        return os.path.join(self.base_dir, self.PROJECTS_DIR_NAME)

    def get_project_dir(self):
        return os.path.join(self.get_projects_dir(), self.project_name)

    
    @classmethod
    def identify_default_project(cls):
        """
        Class method that reads default-project.toml to identify the default-project.
        """
        # This climbs out of /src/pipeline/ to find the root.
        # parents[0] → The directory that contains the Python file.
        # parents[1] → The parent of that directory.
        # parents[2] → The grandparent directory (which should be the root), if root_pipeline\src\pipeline\
        # This organization anticipates PyPi packaging.
        root_dir = Path(__file__).resolve().parents[2]  # This assumes that this python file's directory is two levels below the root. 
        projects_dir = root_dir / cls.PROJECTS_DIR_NAME
        print(f"projects_dir = {projects_dir}")
        default_toml_path = projects_dir / cls.DEFAULT_PROJECT_TOML_FILE_NAME

        if not os.path.exists(default_toml_path):
            raise FileNotFoundError(f"Missing {cls.DEFAULT_PROJECT_TOML_FILE_NAME} in {projects_dir}")

        with open(default_toml_path, 'r') as f:
            data = toml.load(f)
            print(data)
        try:
            return data['default-project']['project'] # This dictates the proper formatting of the TOML file.
        except KeyError as e:
            raise KeyError(f"Missing key in {cls.DEFAULT_PROJECT_TOML_FILE_NAME}: {e}")

def find_project_root():
    """Recursively search for the project's root directory."""
    path = Path(__file__).resolve()
    while path != path.root:
        if (path / "default-project.toml").exists():  # Change to a relevant marker
            return path
        path = path.parent  # Move up one level
    raise FileNotFoundError("Project root not found!")

def demo_find_project_root():
    root_dir = find_project_root()
    print(root_dir)
        
def establish_default_project():
    project_name = ProjectManager.identify_default_project()
    print(f"project_name = {project_name}")
    project_manager = ProjectManager(project_name)
    return project_manager.get_project_dir()

def demo_projectmanager():
    print(f"establish_default_project() = {establish_default_project()}")

if __name__ ==  "__main__":
    demo_projectmanager()

    