import schedule
import time

def job():
    print("im working ....")



schedule.every(5).seconds.do(job)


while True:
    schedule.run_pending()
    time.sleep(1)