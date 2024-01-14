import unittest
import datetime
import schedule
from schedule import every, repeat, ScheduleError, ScheduleValueError, IntervalError


class SchedulerTest(unittest.TestCase):
    def setUp(self):
        schedule.clear()

    def test_time_units(self):
        # assert every().seconds.unit == "seconds"
        # assert every().minutes.unit == "minutes"
        # assert every().hours.unit == "hours"
        # assert every().days.unit == "days"
        # assert every().weeks.unit == "weeks"

        # ---
        job_instance = schedule.Job(interval=2)

        with self.assertRaises(IntervalError):
            job_instance.second
        with self.assertRaises(IntervalError):
            job_instance.minute
        with self.assertRaises(IntervalError):
            job_instance.hour
        with self.assertRaises(IntervalError):
            job_instance.day
        with self.assertRaises(IntervalError):
            job_instance.week

        # ---

        with self.assertRaisesRegex(
            IntervalError, (r"scheduling \.saturday\(\) job is only allowed for weekly jobs\.")
        ):
            job_instance.saturday

        # ---

        job_instance.unit = "chert o pert"
        self.assertRaises(ScheduleValueError, job_instance.at, "1:0:0")
        self.assertRaises(ScheduleValueError, job_instance._schedule_next_run)

        # ---

        job_instance.unit = "days"
        job_instance.start_day = 1
        self.assertRaises(ScheduleValueError, job_instance._schedule_next_run)

        # ---

        job_instance.unit = "weekdays"
        job_instance.start_day = "chet o pert"
        self.assertRaises(ScheduleValueError, job_instance._schedule_next_run)

        # ---

        job_instance.unit = "days"
        self.assertRaises(ScheduleValueError, job_instance.at, "25:00:00")
        self.assertRaises(ScheduleValueError, job_instance.at, "00:65:00")
        self.assertRaises(ScheduleValueError, job_instance.at, "00:00:65")