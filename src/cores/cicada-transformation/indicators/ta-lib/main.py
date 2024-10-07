from src.commons.env_manager.env_controller import EnvController
from talib_processing_controller import TransformationTaLib

env_controller = EnvController()
if __name__ == "__main__":
    controllerTaLib = TransformationTaLib()
    controllerTaLib.compute_and_upload_indicators()
