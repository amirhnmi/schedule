import schedule

import time

@schedule.repeat(schedule.every(5).seconds)
def job():
    print("im working ....")


#schedule.every(5).seconds.do(job).tag("amir")



while True:
    schedule.run_pending()
    # print(schedule.get_next_run("amir"))
    #print(schedule.idle_seconds())
    time.sleep(1)