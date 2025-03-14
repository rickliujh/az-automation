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

import unittest

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
REX = r"^([MTWRFSU]\d{10}-\d{10})(,([MTWRFSU]\d{10}-\d{10}))*$"

def today(period, day):
    if not re.match(REX, period):
        raise ValueError("invalid formart for running period")
        
    groups = period.split(',')
    
    for g in groups:
        print(g)
        if g[0] == NUM2DAY[day]:
            tsa = g[1:].split('-')
            return (tsa[0], tsa[1])
    return None

class TestScheduler(unittest.TestCase):
    def test_should_return_timestamps(self):
        period1 = "M1741796390-1741803820"
        period2 = "M1741796390-1741803820,T1741796391-1741803819"
        day = 1
        self.assertEqual(today(period1, day), ("1741796390","1741803820"))
        self.assertEqual(today(period2, day), ("1741796390","1741803820"))

    def test_should_raise_ValueError(self):
        period1 = "M1741796390,1741803820,T1741796391-1741803819"
        period2 = "Y1741796390-1741803820,T1741796391-1741803819"
        period3 = "Y1741796390-1741803820,T17417963-1741803819"
        day = 1
        with self.assertRaisesRegex(ValueError, "invalid formart for running period"):
           today(period1, day) 
        with self.assertRaisesRegex(ValueError, "invalid formart for running period"):
           today(period2, day) 
        with self.assertRaisesRegex(ValueError, "invalid formart for running period"):
           today(period3, day) 
            

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

                timestamps = today(period, date.today().weekday())
                if timestamps is None:
                    subprocess.Popen(cmd0, shell=True)        
                    continue

                startedAt, endedAt = timestamps
                if now > endedAt:
                    subprocess.Popen(cmd1, shell=True)        
                else:
                    subprocess.Popen(cmd0, shell=True)        

        print(f"scan finished")
    except e:
        print(e)

if __name__ == "__main__":
    main()

