import datetime
from threading import Timer
import time


def always_run():
    print('Always Run: ', datetime.datetime.utcnow())
    Timer(60, always_run, []).start()


def run():
    current_time = datetime.datetime.utcnow()

    print('Run: ', current_time)

    # So more stuffs

    next_one_minute = current_time + datetime.timedelta(minutes=1)
    time.sleep(5)
    diff_sec = (next_one_minute - datetime.datetime.utcnow()).total_seconds()
    # print(diff_sec)
    if diff_sec > 0:
        time.sleep(diff_sec)

    always_run()


def main():
    utc_now = datetime.datetime.utcnow()
    sec = int(utc_now.strftime('%S'))
    print(sec)

    print("Current Time: ", utc_now.strftime('%H:%M:%S'))
    start_date_time = utc_now - datetime.timedelta(hours=23, minutes=58, seconds=60 + sec)
    end_date_time = utc_now + datetime.timedelta(seconds=59 - sec)
    print('Start Date Time: ', start_date_time)
    print('End Date Time: ', end_date_time)
    delay = 65 - sec
    should_run_time = utc_now + datetime.timedelta(seconds=delay)
    print("Should Run: ", should_run_time)
    t = Timer(delay, run)
    t.start()


if __name__ == '__main__':
    main()
