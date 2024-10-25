import os
import yaml
from dotenv import load_dotenv

class EnvController:
    """
    Controller for managing environment variables and YAML configurations.
    Implements a singleton pattern to ensure it's initialized only once.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(EnvController, cls).__new__(cls)
        return cls._instance

    def __init__(self, config_base_path: str = None):
        """
        Initialize the controller by specifying the environment and base config path.
        :param config_base_path: The base path where the environment configurations are stored.
        """
        if hasattr(self, '_initialized') and self._initialized:
            return  # Skip if already initialized

        # Lazy-load the logger to avoid circular imports

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

        self._initialized = True  # Mark the controller as initialized

    def find_project_root(self):
        current_dir = os.getcwd()
        while True:
            if os.path.isdir(os.path.join(current_dir, 'config')):
                return os.path.join(current_dir, 'config')
            if current_dir == os.path.dirname(current_dir):  # Stop if we reach the root of the filesystem
                error_msg = "Could not find project root with 'config' directory."
                raise FileNotFoundError(error_msg)
            current_dir = os.path.dirname(current_dir)

    def load_env_files(self):
        shared_env_path = os.path.join(self.config_base_path, "shared", ".env.shared")
        if os.path.exists(shared_env_path):
            load_dotenv(shared_env_path, override=False)

        env_specific_path = os.path.join(self.config_base_path, self.env, "secrets.env")
        if os.path.exists(env_specific_path):
            load_dotenv(env_specific_path, override=True)

    def load_yaml_files(self):
        shared_yaml_path = os.path.join(self.config_base_path, "shared", "db-config.yaml")
        self._load_yaml_file(shared_yaml_path)

        env_yaml_path = os.path.join(self.config_base_path, self.env, "app-config.yaml")
        self._load_yaml_file(env_yaml_path)

    def _load_yaml_file(self, file_path):
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                config = yaml.safe_load(file)
                if config:
                    self.yaml_config.update(config)

    def get_env(self, key, default_value=None):
        value = os.getenv(key, default_value)
        return value

    def get_yaml_config(self, *keys, default_value=None):
        config = self.yaml_config
        for key in keys:
            if config is not None and key in config:
                config = config[key]
            else:
                return default_value
        return config

    def reload_env(self):
        self.load_env_files()
        self.load_yaml_files()

    def get_google_key_path(self):
        google_key_path = os.path.join(self.config_base_path, "shared", "key.json")
        if os.path.exists(google_key_path):
            return google_key_path
        else:
            error_msg = f"Google Cloud key file not found at: {google_key_path}"
            raise FileNotFoundError(error_msg)

    def set_google_key_path(self):
        google_key_path = self.get_google_key_path()
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
