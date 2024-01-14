import logging
import functools
import datetime
import time
import re
import pytz
import random
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
        ret = job.run()
        if isinstance(ret, CancelJob) or ret is CancelJob:
            self.cancel_job(job)

    def get_jobs(self, tag=None):
        if tag is None:
            return self.jobs[:]
        return [job for job in self.jobs if tag in job.tags]

    def get_next_run(self, tag=None):
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

    def clear(self, tag=None):
        if tag is None:
            logger.debug("Deleteing all jobs")
            del self.jobs[:]
        else:
            logger.debug(f"Deleteing all tagged {tag}")
            self.jobs[:] = (job for job in self.jobs if tag not in job.tags)

    def run_all(self, delay_seconds=0):
        logger.debug(
            f"Runnig all {len(self.jobs)} with {delay_seconds} delay in between")
        for job in self.jobs[:]:
            self._job_run(job)
            time.sleep(delay_seconds)

    def cancel_job(self, job):
        try:
            logger.debug(f"canceling job {str(job)}")
            self.jobs.remove(job)
        except ValueError:
            logger.debug("Cancelong failed")


class Job:
    """ this class receives the jobs and determines the job type """

    def __init__(self, interval, scheduler=None):
        self.interval = interval  # فاصبه زمانی
        self.jub_func = None  # فانکشنی که باید در فواصل زمانی مشحص شده اجرا کنیم
        self.unit = None  # واحد زمانی - ثانیه-دقیقه-ساعت
        self.period = None  # مثال -> هر ۱۰ ثانیه
        self.next_run = None  # زمان بعدی اجرای جاب را در این متغیر ذخیزه میکنیم
        self.last_run = None  # مشخص میکند هر جاب اخرین بار کی اجرا شده
        self.cancel_after = None  # زمان یا تاریخی که میخوایم جاب بعد از ان انجام نشود
        self.latest = None
        self.start_day = None
        self.at_time = None
        self.at_time_zone = None
        self.tags = set()
        self.scheduler = scheduler

    def __lt__(self, other):
        return self.next_run < other.next_run

    def __str__(self):
        if hasattr(self.job_func, "__name__"):
            job_func_name = self.job_func.__name__
        else:
            job_func_name = repr(self.job_func)

        return "Job(interval={}, unit={}, do={}, args={}, kwargs={})".format(
            self.interval,
            self.unit,
            job_func_name,
            "()" if self.job_func is None else self.job_func.args,
            "{}" if self.job_func is None else self.job_func.keywords
        )

    def __repr__(self):
        def is_repr(j):
            return not isinstance(j, Job)

        timestats = f"last_run: {self.last_run}, next_run: {self.next_run}"
        if hasattr(self.job_func, "__name__"):
            job_func_name = self.job_func.__name__
        else:
            job_func_name = repr(self.job_func)

        if self.job_func is not None:
            args = [repr(x) if is_repr(x) else str(x)
                    for x in self.job_func.args]
            kwargs = ["%s=%s" % (k, repr(v))
                      for k, v in self.job_func.keywords.items()]
            call_repr = job_func_name + "(" + ", ".join(args + kwargs) + ")"
        else:
            call_repr = "[None]"

        if self.at_time is not None:
            return "Every %s %s at %s do %s %s" % (
                self.interval,
                self.unit[:-1] if self.interval == 1 else self.unit,
                self.at_time,
                call_repr,
                timestats
            )
        else:
            ftm = (
                "Every %(interval)s"
                + ("to %(latest)s" if self.latest is not None else "")
                + "%(unit)s do %(call_repr)s %(timestats)s"
            )
            return ftm % dict(
                interval=self.interval,
                latest=self.latest,
                unit=(self.unit[:-1] if self.interval == 1 else self.unit),
                call_repr=call_repr,
                timestats=timestats
            )

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

    @property
    def saturday(self):
        if self.interval != 1:
            raise IntervalError(
                "scheduling .saturday() job is only allowed for weekly jobs."
            )
        self.start_day = "saturday"
        return self.weeks

    def to(self, latest):
        self.latest = latest
        return self

    def at(self, time_str, tz=None):
        if self.unit in ['days', "hours", "minutes"] and not self.start_day:
            raise ScheduleValueError(
                "Invalid unit. valid unit are `days` `hours` `minutes`")

        if tz is not None:
            if isinstance(tz, str):
                self.at_time_zone = pytz.timezone(tz)
            elif isinstance(tz, pytz.BaseTzInfo):
                self.at_time_zone = tz
            else:
                raise ScheduleValueError(
                    "time zone must be string or pytz.timezone object")

        if not isinstance(time_str, str):
            raise TypeError("at() should be pased a string")

        if self.unit == "days" or self.start_day:
            if not re.match(r"^[0-2]\d:[0-5]\d(:[0-5]\d)?$", time_str):
                raise ScheduleValueError(
                    "Invalid time format for a dayly job.valid format is HH:MM(:SS)?")

        if self.unit == "hours":
            if not re.match(r"^([0-2]\d)?:[0-5]\d$", time_str):
                raise ScheduleValueError(
                    "Invalid time format for a hourly job.valid format is (HH)?:MM")

        if self.unit == "minutes":
            if not re.match(r"^:[0-5]\d$", time_str):
                raise ScheduleValueError(
                    "Invalid time format for a minutely.job valid format is :MM")

        time_values = time_str.split(":")
        if len(time_values) == 3:
            hour, minutes, second = time_values
        elif len(time_values) == 2 and self.unit == "minutes":
            hour, minute = 0, 0
            _, second = time_values
        elif len(time_values) == 2 and self.unit == "hours" and len(time_values[0]):
            hour = 0
            minute, second = time_values
        else:
            hour, minute = time_values
            second = 0

        if self.unit == "days" or self.start_day:
            hour = int(hour)
            if not (0 <= hour <= 23):
                raise ScheduleValueError(
                    "Invalid number of hours.hour must be between 0 to 23")
        elif self.unit == "hours":
            hour = 0
        elif self.unit == "minutes":
            hour, minutes = 0, 0

        hour = int(hour)
        minute = int(minute)
        second = int(second)

        self.at_time = datetime.time(hour, minute, second)
        return self

    def until(self, until_time):
        if isinstance(until_time, datetime.datetime):
            self.cancel_after = until_time
        elif isinstance(until_time, datetime.timedelta):
            self.cancel_after = datetime.datetime.now() + until_time
        elif isinstance(until_time, datetime.time):
            self.cancel_after = datetime.datetime.combine(
                datetime.datetime.now(), until_time)
        elif isinstance(until_time, str):
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
                cancel_after = cancel_after.replace(
                    year=now.year, month=now.month, day=now.day)
            self.cancel_after = cancel_after
        else:
            raise TypeError(
                "until just take string/datetime.datetime/datetime.time/timedelta parameter")

        if self.cancel_after < datetime.datetime.now():
            raise ScheduleValueError(
                "can not schedule to run until a time in the past")

        return self

    def tag(self, *tags):
        if not all(isinstance(tag, Hashable) for tag in tags):
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

        if self.latest is not None:
            if not self.latest >= self.interval:
                raise ScheduleError("latest is grather than interval")
            interval = random.randint(self.interval, self.latest)
        else:
            interval = self.interval

        self.period = datetime.timedelta(**{self.unit: interval})
        self.next_run = datetime.datetime.now() + self.period

        if self.start_day is not None:
            if self.unit != "weeks":
                raise ScheduleValueError("unit  should be weeks")

            weekdays = ("monday", "tuesday", "wednsday",
                        "thursday", "friday", "saturday", "sunday")

            if self.start_day not in weekdays:
                raise ScheduleValueError(
                    "invalid start day.valid start day are".format(weekdays))

            weekday = weekdays.index(self.start_day)
            days_ahead = weekday - self.next_run.weekday()
            if days_ahead <= 0:
                days_ahead += 7

            self.next_run += datetime.timedelta(days_ahead) - self.period

            if self.at_time is not None:
                if self.unit not in ("days", "hours", "minutes") and self.start_day is None:
                    raise ScheduleValueError(
                        "invalid unit without specifying start day")

                kwargs = {"second": self.at_time.second, "microsecond": 0}
                if self.unit is "days" and self.start_day is not None:
                    kwargs["hour"] = self.at_time.hour
                if self.unit in ["days", "hours"] and self.start_day is not None:
                    kwargs["minute"] = self.at_time.minute
                self.next_run = self.next_run.replace(**kwargs)

                if self.at_time_zone is not None:
                    self.next_run = self.at_time_zone.localize(
                        self.next_run).astimezone().replace(tzinfo=None)

            if self.start_day is not None and self.at_time is not None:
                if (self.next_run - datetime.datetime.now()).days >= 7:
                    self.next_run -= self.period

    def _is_overdue(self, when):
        return self.cancel_after is not None and when > self.cancel_after

    def _decode_datetime_str(self, datetime_str, formats):
        for f in formats:
            try:
                return datetime.datetime.strptime(datetime_str, f)
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

# decorator function


def repeat(job, *args, **kwargs):
    def _schedule_decorator(decorated_function):
        job.do(decorated_function, *args, **kwargs)
        return decorated_function
    return _schedule_decorator
