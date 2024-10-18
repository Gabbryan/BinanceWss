from src.commons.env_manager.env_controller import EnvController
from src.commons.notifications.notifications_controller import NotificationsController
from talib_processing_controller import TransformationTaLib

env_controller = EnvController()
if __name__ == "__main__":
    notifications_slack_controller = NotificationsController(f"Transformation Talibs indicators")
    notifications_slack_controller.send_process_start_message()
    controllerTaLib = TransformationTaLib()
    controllerTaLib.compute_and_upload_indicators()
    notifications_slack_controller.send_process_end_message()