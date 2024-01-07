import logging
import functools
import datetime
import time
from collections.abc import Hashable

logger = logging.getLogger("schedule.log")


# personal exception
class ScheduleError(Exception):
    pass


class ScheduleValueError(ScheduleError):
    pass


class IntervalError(ScheduleError):
    pass


class CancelJob():
    """can be retruned from a job to unschedule itself"""


class scheduler:
    """ this class is an executor """

    def __init__(self):
        self.jobs = []

    def every(self, interval):
        job = Job(interval, scheduler=self)
        return job

    def run_pending(self):
        runable_jobs = (job for job in self.jobs if job.should_run)
        for job in sorted(runable_jobs):
            self._job_run(job)

    def _job_run(self, job):
        job.run()

    def get_jobs(self,tag=None):
        if tag is None: return self.jobs[:]
        return [job for job in self.jobs if tag in job.tags]
    
    def get_next_run(self,tag=None):
        if not self.jobs:
            return None
        job_filterd = self.get_jobs(tag)
        if not job_filterd:
            return None
        return min(job_filterd).next_run
    

    next_run = property(get_next_run)
    @property
    def idle_seconds(self):
        if not self.next_run:
            return None
        return (self.next_run - datetime.datetime.now()).total_seconds()

    def clear(self,tag=None):
        if tag is None:
            logger.debug("Deleteing all jobs")
            del self.jobs[:]
        else:
            logger.debug(f"Deleteing all tagged {tag}")
            self.jobs[:] = (job for job in self.jobs if tag not in job.tags)

    def run_all(self,delay_seconds=0):
        logger.debug(f"Runnig all {len(self.jobs)} with {delay_seconds} delay in between")
        for job in self.jobs[:]:
            self._job_run(job)
            time.sleep(delay_seconds)

    def cancel_job(self,job):
        try:
            logger.debug(f"canceling job {str(job)}")
            self.jobs.remove(job)
        except ValueError:
            logger.debug("Cancelong failed")


class Job:
    """ this class receives the jobs and determines the job type """

    def __init__(self, interval, scheduler):
        self.interval = interval  # فاصبه زمانی
        self.jub_func = None  # فانکشنی که باید در فواصل زمانی مشحص شده اجرا کنیم
        self.unit = None  # واحد زمانی - ثانیه-دقیقه-ساعت
        self.period = None  # مثال -> هر ۱۰ ثانیه
        self.next_run = None  # زمان بعدی اجرای جاب را در این متغیر ذخیزه میکنیم
        self.last_run = None  # مشخص میکند هر جاب اخرین بار کی اجرا شده
        self.cancel_after = None #زمان یا تاریخی که میخوایم جاب بعد از ان انجام نشود
        self.tags = set()
        self.scheduler = scheduler

    def __lt__(self, other):
        return self.next_run < other.next_run


    @property
    def second(self):
        if self.interval != 1:
            raise IntervalError("Use seconds insted of second")
        self.seconds

    @property
    def seconds(self):
        self.unit = "seconds"
        return self

    @property
    def minute(self):
        if self.interval != 1:
            raise IntervalError("Use minutes insted of second")
        return self.minutes

    @property
    def minutes(self):
        self.unit = "minutes"
        return self

    @property
    def hour(self):
        if self.interval != 1:
            raise IntervalError("Use hours insted of hour")
        return self.hour

    @property
    def hours(self):
        self.unit = "hours"
        return self

    @property
    def day(self):
        if self.interval != 1:
            raise IntervalError("Use days insted of day")
        return self.day

    @property
    def days(self):
        self.unit = "days"
        return self

    @property
    def week(self):
        if self.interval != 1:
            raise IntervalError("Use weeks insted of week")
        return self.week

    @property
    def weeks(self):
        self.unit = "weeks"
        return self

    def until(self,until_time):
        if isinstance(until_time,datetime.datetime):
            self.cancel_after = until_time
        elif isinstance(until_time,datetime.timedelta):
            self.cancel_after = datetime.datetime.now() + until_time
        elif isinstance(until_time,datetime.time):
            self.cancel_after = datetime.datetime.combine(datetime.datetime.now(),until_time)
        elif isinstance(until_time,str):
            cancel_after = self._decode_datetime_str(
                until_time,
                [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%Y-%m-%d",
                    "%H:%M:%S",
                    "%H:%M",
                ]
                )
            if cancel_after is None:
                raise ScheduleValueError("Invalid string format for until")
            if '-' not in until_time:
                now = datetime.datetime.now()
                cancel_after = cancel_after.replace(year=now.year,month=now.month,day=now.day)
            self.cancel_after=cancel_after
        else:
            raise TypeError("until just take string/datetime.datetime/datetime.time/timedelta parameter")

        if self.cancel_after < datetime.datetime.now():
            raise ScheduleValueError("can not schedule to run until a time in the past")
        
        return self

    def tag(self,*tags):
        if not all(isinstance(tag,Hashable) for tag in tags):
            TypeError("Tags must be hashable")
        self.tags.update(tags)
        return self

    def do(self, job_func, *wargs, **kwargs):
        self.job_func = functools.partial(job_func, *wargs, **kwargs)
        functools.update_wrapper(self.job_func, job_func)
        self._schedule_next_run()
        if self.scheduler is None:
            raise ScheduleError(
                "Unable to add jot to schedule. job is not assosiated with an scheduler")

        self.scheduler.jobs.append(self)
        return self
    
    @property
    def should_run(self):
        assert self.next_run is not None, "must run _schedule_next_run before"
        return datetime.datetime.now() >= self.next_run

    def run(self):
        if self._is_overdue(datetime.datetime.now()):
            logger.debug(f"cancelling job {self}")
            return CancelJob
        logger.debug(f"Running job {self}")
        ret = self.job_func()   
        self.last_run = datetime.datetime.now()
        self._schedule_next_run()
        if self._is_overdue(self.next_run):
            logger.debug(f"cancelling job {self}")
            return CancelJob
        return ret

    def _schedule_next_run(self):
        if self.unit not in ("seconds", "minutes", "hours", "days", "weeks"):
            raise IntervalError(
                "invalid unit, valid unit are :`seconds`-`minutes`-`hours`-`days`-`weeks`")

        interval = self.interval
        self.period = datetime.timedelta(**{self.unit: interval})
        self.next_run = datetime.datetime.now() + self.period

    def _is_overdue(self,when):
        return self.cancel_after is not None and when > self.cancel_after

    def _decode_datetime_str(self,datetime_str,formats):
        for f in formats:
            try:
                return datetime.datetime.strptime(datetime_str,f)          
            except ValueError:
                pass
        return None

default_scheduler = scheduler()

def every(interval):
    return default_scheduler.every(interval=interval)

def run_pending():
    default_scheduler.run_pending()

def get_jobs(tag=None):
    return default_scheduler.get_jobs(tag)

# job badi che taraikhi ast
def get_next_run(tag=None):
    return default_scheduler.get_next_run(tag)

# ta job badi chand seconds moonde
def idle_seconds():
    return default_scheduler.idle_seconds

def clear(tag=None):
    return default_scheduler.clear(tag)

def run_all(delay_seconds=None):
    return default_scheduler.run_all(delay_seconds)

def cancel_job(job):
    return default_scheduler.cancel_job(job)

#decorator function
def repeat(job,*args,**kwargs):
    def _schedule_decorator(decorated_function):
        job.do(decorated_function,*args,**kwargs)
        return decorated_function
    return _schedule_decorator

