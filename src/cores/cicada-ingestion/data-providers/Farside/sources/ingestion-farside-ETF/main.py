import time
from datetime import datetime, timedelta

from controller_ingestion_farside_etf import ControllerIngestionFarsideETF as CIFETF
from src.commons.env_manager.env_controller import EnvController
from src.libs.utils.sys.scheduler.scheduler_controller import SchedulerController


controller = CIFETF()
env_manager = EnvController()
if __name__ == "__main__":

    scheduler_controller = SchedulerController()
    # Schedule a daily task at midnight
    scheduler_controller.schedule_job(controller.run_daily_tasks, schedule_type="daily", time="02:00")
    # Initialize the last run time to a time in the past
    last_run_time = datetime.now() - timedelta(days=1)
    scheduler_controller.run_all_jobs()

    while True:
        now = datetime.now()

        # Run any pending jobs
        scheduler_controller.run_pending_jobs()

        # Check every hour to ensure tasks run at the correct time
        time.sleep(3600)
