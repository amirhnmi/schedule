import logging 
import functools
import datetime


logger = logging.getLogger("schedule.log")


# personal exception
class ScheduleError(Exception):
    pass


class ScheduleValueError(ScheduleError):
    pass


class IntervalError(ScheduleError):
    pass


class scheduler:
    """ this class is an executor """

    def __init__(self):
        self.jobs = []

    def every(self,interval):
        job = Job(interval,scheduler=self)
        return job

    def run_pending(self):
        runable_jobs = (job for job in self.jobs if should_run)
        for job in sorted(runable_jobs):
            self._run_job(job)


    def _job_run(self,job):
        pass

        

class Job:
    """ this class receives the jobs and determines the job type """

    def __init__(self,interval,scheduler):
        self.interval = interval    #فاصبه زمانی     
        self.jub_func = None        #فانکشنی که باید در فواصل زمانی مشحص شده اجرا کنیم
        self.unit = None            #واحد زمانی - ثانیه-دقیقه-ساعت 
        self.period = None          #مثال -> هر ۱۰ ثانیه
        self.next_run = None        #زمان بعدی اجرای جاب را در این متغیر ذخیزه میکنیم
        self.scheduler = scheduler


    def __lt__(self,other):
        return self.next_run < other.next_run

    @property  #vaghti yek method ra property mikonid digar niaz be () baraye seda zadan function nadarid. darvaghe function hich parameter nadard.
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
    

    def do(self,job_func,*wargs,**kwargs):
        self.job_func = functools.partial(job_func,*wargs,**kwargs)
        functools.update_wrapper(self.job_func,job_func)
        self._schedule_next_run()
        if self.scheduler is None:
            raise ScheduleError("Unable to add jot to schedule. job is not assosiated with an scheduler")
        
        self.scheduler.jobs.append(self)
        return self
    

    @property
    def should_run(self):
        assert self.next_run is not None, " must run _schedule_next_run before"
        return datetime.datetime.now() >= self.next_run

    def _schedule_next_run(self):
        if self.unit not in ("seconds","minutes","hours","days","weeks"):
            raise IntervalError("invalid unit, valid unit are :`seconds`-`minutes`-`hours`-`days`-`weeks`")

        interval = self.interval
        self.period = datetime.timedelta(**{self.unit:interval})
        next_run = datetime.datetime.now() + self.period

default_scheduler = scheduler()
def every(interval):
    default_scheduler.every(interval=interval)


def run_pending():
    default_scheduler.run_pending()