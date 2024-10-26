from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.commons.notifications.notifications_controller import NotificationsController
from src.commons.logs.logging_controller import LoggingController
from talib_processing_controller import TransformationTaLib

# Initialize the logging controller
logger = LoggingController("TransformationTaLibProcess")

env_controller = EnvController()

if __name__ == "__main__":
    # Initialize Slack notifications controller
    notifications_slack_controller = NotificationsController("Transformation Talibs indicators")
    logger.log_info("Slack notifications controller initialized.", context={'mod': 'MainProcess', 'action': 'InitializeSlackController'})

    # Send start process message
    notifications_slack_controller.send_process_start_message()
    logger.log_info("Process start message sent to Slack.", context={'mod': 'MainProcess', 'action': 'SendStartMessage'})

    # Initialize and run the TA-Lib controller
    try:
        controllerTaLib = TransformationTaLib()
        logger.log_info("TransformationTaLib controller initialized.", context={'mod': 'MainProcess', 'action': 'InitializeController'})

        # Compute and upload indicators
        controllerTaLib.compute_and_upload_indicators()
        logger.log_info("Indicators computed and uploaded.", context={'mod': 'MainProcess', 'action': 'ComputeAndUpload'})

    except Exception as e:
        logger.log_error(f"Error in TransformationTaLib process: {e}", context={'mod': 'MainProcess', 'action': 'ProcessError'})
        raise

    # Send end process message
    notifications_slack_controller.send_process_end_message()
    logger.log_info("Process end message sent to Slack.", context={'mod': 'MainProcess', 'action': 'SendEndMessage'})
