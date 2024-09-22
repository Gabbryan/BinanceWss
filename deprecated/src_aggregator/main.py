import time

import schedule

from tasks.task_scheduler import schedule_tasks


def main():
    schedule_tasks()
    # Run scheduled tasks in an infinite loop, pausing for 1 second between each loop iteration
    while True:
        schedule.run_pending()
        time.sleep(1)


# Standard boilerplate to execute the main function if the script is run directly
if __name__ == "__main__":
    main()
