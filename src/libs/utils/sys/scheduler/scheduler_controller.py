import schedule

class SchedulerController:
    def __init__(self):
        pass

    def schedule_job(self, job, schedule_type="daily", **kwargs):
        """
        Schedules a job based on the given schedule type.
        :param job: The function to be scheduled.
        :param schedule_type: The type of schedule ('daily', 'weekly', 'interval', etc.).
        :param kwargs: Additional arguments for scheduling (e.g., time for 'daily', interval for 'interval').
        """
        if schedule_type == "daily":
            time_of_day = kwargs.get("time", "00:00")
            schedule.every().day.at(time_of_day).do(job)
        elif schedule_type == "weekly":
            day_of_week = kwargs.get("day", "monday")
            time_of_day = kwargs.get("time", "00:00")
            schedule.every().week.at(time_of_day).do(job).tag(day_of_week)
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
        else:
            raise ValueError("Unsupported schedule type")

    def run_all_jobs(self):
        """
        Executes all scheduled jobs.
        """
        schedule.run_all()

    def run_pending_jobs(self):
        """
        Runs only pending jobs.
        """
        schedule.run_pending()
