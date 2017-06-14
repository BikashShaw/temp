import datetime
from threading import Timer


def run():
    print('Run: ', datetime.datetime.utcnow())


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
