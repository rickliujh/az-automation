#!/usr/bin/env python

"""
This script acts as a simple scheduler that automatically executes the command 
listed in the file.

CSV file format:
0     |           1             |           2                 |            3
id    |         period          |         cmd0                |           cmd1
0     | M1741796390-1741803820,T1741796390-1741803820 | az aks start -r example ... | az aks stop -r example ...

[Column 0] id for identity the task, only for operator to identify the task, the 
scheduler will not use this value.

[Column 1] running period of the resource should be operational, scheduler will 
scan this value and keep the resource in the period operational and stop any 
resources that is outside of period.
    format: [day][startedAt-endedAt],...
        Use capital latter to represent day, and immediate followed by running 
        period of that day, from startedAt to endedAt, separated by "-", preceding
        group can be repeated by using "," as delimiter.
        day: M as Monday, T as Tuesday, W as Wednesday, R as Thursday, F as 
                Friday, S as Saturday, U as Sunday
        startedAt: unix timestamp
        endedAt: unix timestamp

[Column 2] cmd0 is the shell command for activate the resource

[Column 3] cmd1 is the shell command for deactivate the resource
"""

import re
import csv
import time
import subprocess
from datetime import date

FILE="schedulebook.csv"
NUM2DAY = {
    1:"M",
    2:"T",
    3:"W",
    4:"R",
    5:"F",
    6:"S",
    7:"U",
}
REX = "^([MTWRFSU]\d{10}-\d{10})(,([MTWRFSU]\d{10}-\d{10}))*$"

def today(period):
    if not re.match(REX, period):
        raise ValueError("invalid formart for running period")
        
    groups = period.spliti(',')
    day = NUM2DAY[date.today().weekday()]
    for g in groups:
        if g[0] == day:
            return g
    return None
    

def main():
    try:
        now = time.time()
        print(f"starting scan at {now}")

        with open(FILE, mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                id = row['0']
                period = row['1']
                cmd0 = row['2']
                cmd1 = row['3']
                print(f"reading row: {id} {period} {cmd0} {cmd1}")

                timestamps = today(period)
                if timestamps is None:
                    subprocess.Popen(cmd1, shell=True)        

                startedAt = timestamps[0].strip()
                endedAt = timestamps[1].strip()
                if now > endedAt:
                    subprocess.Popen(cmd1, shell=True)        
                else:
                    subprocess.Popen(cmd0, shell=True)        

        print(f"scan finished")
    except e:
        print(e)

if __name__ == "__main__":
    main()

