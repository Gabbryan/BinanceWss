import schedule
from src.commons.logs.logging_controller import LoggingController

class SchedulerController:
    def __init__(self):
        self.logger = LoggingController("SchedulerController")
        self.logger.log_info("SchedulerController initialized.", context={'mod': 'SchedulerController', 'action': 'Init'})

    def schedule_job(self, job, schedule_type="daily", **kwargs):
        """
        Schedules a job based on the given schedule type.

        :param job: The function to be scheduled.
        :param schedule_type: The type of schedule ('daily', 'weekly', 'interval', etc.).
        :param kwargs: Additional arguments for scheduling (e.g., time for 'daily', interval for 'interval').
        """
        try:
            if schedule_type == "daily":
                time_of_day = kwargs.get("time", "00:00")
                schedule.every().day.at(time_of_day).do(job)
                self.logger.log_info(f"Scheduled daily job at {time_of_day}.", context={'mod': 'SchedulerController', 'action': 'ScheduleDailyJob'})

            elif schedule_type == "weekly":
                day_of_week = kwargs.get("day", "monday")
                time_of_day = kwargs.get("time", "00:00")
                schedule.every().week.at(time_of_day).do(job).tag(day_of_week)
                self.logger.log_info(f"Scheduled weekly job on {day_of_week} at {time_of_day}.", context={'mod': 'SchedulerController', 'action': 'ScheduleWeeklyJob'})

            elif schedule_type == "interval":
                interval = kwargs.get("interval", 1)
                unit = kwargs.get("unit", "minutes")
                if unit == "minutes":
                    schedule.every(interval).minutes.do(job)
                elif unit == "hours":
                    schedule.every(interval).hours.do(job)
                elif unit == "seconds":
                    schedule.every(interval).seconds.do(job)
                elif unit == "days":
                    schedule.every(interval).days.do(job)
                self.logger.log_info(f"Scheduled interval job every {interval} {unit}.", context={'mod': 'SchedulerController', 'action': 'ScheduleIntervalJob'})

            else:
                raise ValueError("Unsupported schedule type")

        except Exception as e:
            self.logger.log_error(f"Error scheduling job: {e}", context={'mod': 'SchedulerController', 'action': 'ScheduleJobError'})

    def run_all_jobs(self):
        """
        Executes all scheduled jobs.
        """
        try:
            self.logger.log_info("Executing all scheduled jobs.", context={'mod': 'SchedulerController', 'action': 'RunAllJobs'})
            schedule.run_all()
        except Exception as e:
            self.logger.log_error(f"Error executing all jobs: {e}", context={'mod': 'SchedulerController', 'action': 'RunAllJobsError'})

    def run_pending_jobs(self):
        """
        Runs only pending jobs.
        """
        try:
            self.logger.log_info("Executing pending scheduled jobs.", context={'mod': 'SchedulerController', 'action': 'RunPendingJobs'})
            schedule.run_pending()
        except Exception as e:
            self.logger.log_error(f"Error executing pending jobs: {e}", context={'mod': 'SchedulerController', 'action': 'RunPendingJobsError'})
