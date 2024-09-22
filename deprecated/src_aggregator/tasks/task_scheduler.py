import schedule

from cores.BinanceAPIManager import BinanceAPI
from .task_handler import run_threaded, task_wrapper
from .tasks import tasks as TASKS


def schedule_tasks():
    api = BinanceAPI("https://www.binance.com")
    # Define a list of tasks with the corresponding API methods, endpoints, data, and names
    # Schedule each task to run every minute using the scheduler library
    for method, endpoint, data, task_name in TASKS:
        schedule.every(1).minutes.do(
            run_threaded, task_wrapper, api, method, endpoint, data, task_name
        )
    schedule.run_all()
