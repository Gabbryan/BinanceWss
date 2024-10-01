import os

import yaml
from dotenv import load_dotenv


class EnvController:
    """
    Controller for managing environment variables and YAML configurations.
    """

    def __init__(self, config_base_path: str = None):
        """
        Initialize the controller by specifying the environment and base config path.
        :param config_base_path: The base path where the environment configurations are stored.
        """
        # Automatically detect the project root if config_base_path is not provided
        if config_base_path is None:
            self.config_base_path = self.find_project_root()
        else:
            self.config_base_path = config_base_path

        self.env = os.getenv('ENV', 'development')
        self.yaml_config = {}
        self.load_env_files()
        self.load_yaml_files()
        self.set_google_key_path()

    def find_project_root(self):
        """
        Find the project root by looking for a known marker file (e.g., .git or config directory).
        """
        current_dir = os.getcwd()

        while True:
            if os.path.isdir(os.path.join(current_dir, 'config')):
                return os.path.join(current_dir, 'config')
            if current_dir == os.path.dirname(current_dir):  # Stop if we reach the root of the filesystem
                raise FileNotFoundError("Could not find project root with 'config' directory.")
            current_dir = os.path.dirname(current_dir)

    def load_env_files(self):
        """
        Load environment-specific and shared environment files.
        """
        # Load shared environment variables
        shared_env_path = os.path.join(self.config_base_path, "shared", ".env.shared")
        print(f"Loading shared env from: {shared_env_path}")
        if os.path.exists(shared_env_path):
            load_dotenv(shared_env_path, override=False)

        # Load environment-specific variables (e.g., development, staging, production)
        env_specific_path = os.path.join(self.config_base_path, self.env, "secrets.env")
        print(f"Loading environment-specific env from: {env_specific_path}")
        if os.path.exists(env_specific_path):
            load_dotenv(env_specific_path, override=True)

    def load_yaml_files(self):
        """
        Load YAML configuration files (app-config.yaml and db-config.yaml).
        """
        # Load shared YAML config (e.g., database config)
        shared_yaml_path = os.path.join(self.config_base_path, "shared", "db-config.yaml")
        print(f"Loading shared YAML config from: {shared_yaml_path}")
        self._load_yaml_file(shared_yaml_path)

        # Load environment-specific YAML config
        env_yaml_path = os.path.join(self.config_base_path, self.env, "app-config.yaml")
        print(f"Loading environment-specific YAML config from: {env_yaml_path}")
        self._load_yaml_file(env_yaml_path)

    def _load_yaml_file(self, file_path):
        """
        Helper function to load a YAML file and merge it into the yaml_config dictionary.
        """
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                print(f"Loading YAML file: {file_path}")
                config = yaml.safe_load(file)
                if config:
                    self.yaml_config.update(config)

    def get_env(self, key, default_value=None):
        """
        Retrieves the value of the specified environment variable.
        :param key: The environment variable key.
        :param default_value: The default value if the variable is not found.
        :return: The value of the environment variable, or default_value if not found.
        """
        return os.getenv(key, default_value)

    def get_yaml_config(self, *keys, default_value=None):
        """
        Retrieves a nested value from the YAML configuration.
        :param keys: The hierarchy of keys (e.g., 'app', 'name').
        :param default_value: The default value if the key is not found.
        :return: The value from the YAML configuration, or default_value if not found.
        """
        config = self.yaml_config
        for key in keys:
            if config is not None and key in config:
                config = config[key]
            else:
                return default_value
        return config

    def reload_env(self):
        """
        Reload the environment variables and YAML configurations from the files (useful if they change during runtime).
        """
        self.load_env_files()
        self.load_yaml_files()

    def get_google_key_path(self):
        """
        Returns the path to the Google Cloud key file and sets it as the environment variable.
        """
        google_key_path = os.path.join(self.config_base_path, "shared", "key.json")
        if os.path.exists(google_key_path):
            google_key_path = self.get_env('GOOGLE_APPLICATION_CREDENTIALS')
            return google_key_path
        else:
            raise FileNotFoundError(f"Google Cloud key file not found at: {google_key_path}")

    def set_google_key_path(self):
        """
        Returns the path to the Google Cloud key file and sets it as the environment variable.
        """
        google_key_path = os.path.join(self.config_base_path, "shared", "key.json")
        if os.path.exists(google_key_path):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
        else:
            raise FileNotFoundError(f"Google Cloud key file not found at: {google_key_path}")
