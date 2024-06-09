import os
import schedule
import time

def run_main():
    """
    Runs the main.py script using the python3 command.
    """
    os.system("python3 ./hf_bi_python_exercise/main.py")

schedule.every(1).minutes.do(run_main)

while True:
    schedule.run_pending()
    time.sleep(1)