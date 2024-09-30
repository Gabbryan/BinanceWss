from src.libs.utils.pandas.config.config import PandasConfig

# Initialize PandasConfig
pandas_config = PandasConfig()

# Try loading environment and YAML files
pandas_config.apply_env_to_pandas()
pandas_config.apply_yaml_to_pandas()

# Check if the correct paths were loaded
print(f"Google key path: {pandas_config.get_google_key_path()}")
