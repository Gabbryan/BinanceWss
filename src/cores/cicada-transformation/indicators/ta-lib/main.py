from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.commons.notifications.notifications_controller import NotificationsController
from talib_processing_controller import TransformationTaLib

env_controller = EnvController()
logging_controller = LoggingController('Transformation Talibs indicators')
if __name__ == "__main__":
    logging_controller.log_info("starting the service")
    notifications_slack_controller = NotificationsController(f"Transformation Talibs indicators")
    notifications_slack_controller.send_process_start_message()
    controllerTaLib = TransformationTaLib()
    controllerTaLib.compute_and_upload_indicators()
    notifications_slack_controller.send_process_end_message()
    logging_controller.log_info('Shutting down the service')
