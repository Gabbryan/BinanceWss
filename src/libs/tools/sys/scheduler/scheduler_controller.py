import schedule


class SchedulerController:
    def __init__(self):
        pass

    def schedule_job(self, job, interval):
        schedule.every(interval).minutes.do(job)

    def schedule_jobs(self, jobs, interval):
        for job in jobs:
            self.schedule_job(job, interval)

    def async_schedule_jobs(self, jobs, interval):
        for job in jobs:
            schedule.every(interval).minutes.do(job)
            schedule.run_pending()
