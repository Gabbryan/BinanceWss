import pandas as pd

from src.commons.env_manager.env_controller import EnvController  # Import EnvController


class PandasConfig:
    """
    Manages global pandas-related configurations and integrates with environment variables and YAML.
    Uses EnvController internally to handle environment and YAML configurations.
    """

    def __init__(self, config_base_path: str = None):
        """
        Initialize the PandasConfig by specifying the config base path.
        Uses EnvController to manage environment variables and YAML configs.
        :param config_base_path: Base path where the environment configurations are stored.
        """
        # Initialize EnvController as a composition inside PandasConfig
        self.env_controller = EnvController(config_base_path=config_base_path)

    @staticmethod
    def set_pandas_options(options: dict):
        """
        Sets pandas display and performance options.
        :param options: Dictionary of pandas options to set.
        """
        try:
            for option, value in options.items():
                pd.set_option(option, value)
        except KeyError as e:
            raise ValueError(f"Invalid pandas option: {str(e)}")

    @staticmethod
    def get_pandas_option(option: str):
        """
        Gets the current value of a specific pandas option.
        :param option: The pandas option to retrieve.
        :return: The value of the pandas option.
        """
        try:
            return pd.get_option(option)
        except KeyError:
            raise ValueError(f"Pandas option '{option}' not found.")

    @staticmethod
    def reset_pandas_options():
        """
        Resets all pandas options to their default values.
        """
        pd.reset_option("all")

    def apply_yaml_to_pandas(self):
        """
        Apply pandas-related settings from the loaded YAML configuration.
        Uses EnvController's `get_yaml_config` to retrieve YAML configurations.
        """
        pandas_settings = self.env_controller.get_yaml_config('pandas_options')
        if pandas_settings:
            PandasConfig.set_pandas_options(pandas_settings)

    def apply_env_to_pandas(self):
        """
        Apply pandas settings based on environment variables.
        Uses EnvController's `get_env` to retrieve environment variables.
        """
        env_settings = {
            'display.max_rows': self.env_controller.get_env('PANDAS_MAX_ROWS', 60),
            'display.max_columns': self.env_controller.get_env('PANDAS_MAX_COLUMNS', 20),
            'display.precision': self.env_controller.get_env('PANDAS_PRECISION', 2)
        }
        PandasConfig.set_pandas_options(env_settings)

    def apply_default_settings(self):
        """
        Applies default pandas settings if no other config is provided.
        """
        defaults = {
            'display.max_rows': 100,
            'display.max_columns': 20,
            'display.float_format': '{:.2f}'.format
        }
        PandasConfig.set_pandas_options(defaults)

    def get_google_key_path(self):
        """
        Returns the path to the Google Cloud key file.
        Uses EnvController's method `get_google_key_path`.
        """
        return self.env_controller.get_google_key_path()
