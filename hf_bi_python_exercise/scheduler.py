import os
import schedule
import time

def run_main():
    os.system("python3 main.py")

schedule.every(60).minutes.do(run_main)

while True:
    schedule.run_pending()
    time.sleep(1)