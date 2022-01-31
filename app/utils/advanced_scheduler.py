import datetime
import functools
import traceback

import pytz
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

job_stores = {
    'default': MemoryJobStore()
}

transaction_scheduler: AsyncIOScheduler


def init_scheduler():
    global transaction_scheduler
    transaction_scheduler = AsyncIOScheduler(jobstores=job_stores, timezone=pytz.utc)
    transaction_scheduler.start()
    # Here, we resume the paused jobs in the jobstore (in case of an ungraceful program exit)
    pending_jobs = transaction_scheduler.get_jobs()
    for job in pending_jobs:
        job.reschedule(trigger='date', run_date=datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=20))


def async_repeat_deco(repeat_count, reschedule_count=0, inter_repeat_time=5, always_reschedule=False, store='default'):
    def deco_wrapper(func):
        # this notation is copying the information (name, docstring, ecc.) of the original function to the
        # function wrapper
        @functools.wraps(func)
        async def func_wrapper(trans_id=None, *args, **kwargs):  # Here, we added the default parameter 'trans_id'
            counter = 0
            while counter < repeat_count:
                try:
                    # If the 'trans_id' is valid, we forward the parameter to the wrapped function 'func'
                    if trans_id is not None:
                        return await func(trans_id, *args, **kwargs)
                    else:
                        # Otherwise, we call the wrapped function 'func' with the passed arguments (positional and
                        # keywords)
                        return await func(*args, **kwargs)
                except:
                    counter += 1
            if func_wrapper.reschedule_count > 0 and trans_id is not None:
                if not always_reschedule:
                    func_wrapper.reschedule_count -= 1
                transaction_scheduler.add_job(func_wrapper,
                                              id=str(trans_id),
                                              run_date=datetime.datetime.now(pytz.utc) + datetime.timedelta(
                                                  seconds=inter_repeat_time),
                                              args=[trans_id, *args], kwargs=kwargs, replace_existing=True,
                                              misfire_grace_time=None,
                                              jobstore=store)

            raise Exception(f'Failed execution in function: {func.__name__!r}')

        func_wrapper.reschedule_count = reschedule_count
        return func_wrapper

    return deco_wrapper


def repeat_deco(repeat_count, reschedule_count=0, inter_repeat_time=5, always_reschedule=False, store='default'):
    def deco_wrapper(func):
        # this notation is copying the information (name, docstring, ecc.) of the original function to the
        # function wrapper
        @functools.wraps(func)
        def func_wrapper(trans_id=None, *args, **kwargs):  # Here, we added the default parameter 'trans_id'
            counter = 0
            while counter < repeat_count:
                try:
                    # If the 'trans_id' is valid, we forward the parameter to the wrapped function 'func'
                    if trans_id is not None:
                        return func(trans_id, *args, **kwargs)
                    else:
                        # Otherwise, we call the wrapped function 'func' with the passed arguments (positional and
                        # keywords)
                        return func(*args, **kwargs)
                except:
                    traceback.print_exc()
                    counter += 1
            if func_wrapper.reschedule_count > 0 and trans_id is not None:
                if not always_reschedule:
                    func_wrapper.reschedule_count -= 1
                transaction_scheduler.add_job(func_wrapper,
                                              id=str(trans_id),
                                              run_date=datetime.datetime.now(pytz.utc) + datetime.timedelta(
                                                  seconds=inter_repeat_time),
                                              args=[trans_id, *args], kwargs=kwargs, replace_existing=True,
                                              misfire_grace_time=None,
                                              jobstore=store)

            raise Exception(f'Failed execution in function: {func.__name__!r}')

        func_wrapper.reschedule_count = reschedule_count
        return func_wrapper

    return deco_wrapper
