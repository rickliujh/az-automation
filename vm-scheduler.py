#!/usr/bin/env python

"""
This script acts as a simple scheduler that automatically executes the command 
listed in the file. This script DO NOT guarantee the single execution of the 
command. The idempotent of the execution should be handled by command it invokes

CSV table will look like this
id    |          period          |         cmd0                |           cmd1
------|--------------------------|-----------------------------|--------------------------
0     | M07:00-16:00,T09:00-18:00 | az aks start -r example ... | az aks stop -r example ...

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
        startedAt: HH:MM
        endedAt: HH:MM

[Column 2] cmd0 is the shell command for activate the resource

[Column 3] cmd1 is the shell command for deactivate the resource
"""

import re
import sys
import csv
import time as t
import subprocess
from datetime import datetime, time, date

import unittest

FILE= sys.argv[1]
NUM2DAY = {
    1:"M",
    2:"T",
    3:"W",
    4:"R",
    5:"F",
    6:"S",
    7:"U",
}
REX = r"^([MTWRFSU]\d\d:\d\d-\d\d:\d\d)(,([MTWRFSU]\d\d:\d\d-\d\d:\d\d))*$"
NOW = datetime.now()

def getPeriod(period, day):
    if not re.match(REX, period):
        raise ValueError("invalid formart for running period")
        
    groups = period.split(',')
    for g in groups:
        if g[0] == NUM2DAY[day]:
            timestrs = g[1:].split('-')

            sh, sm= map(int, timestrs[0].split(':'))
            start = datetime.combine(NOW.date(), time(hour=int(sh),minute=int(sm)))

            eh, em= map(int, timestrs[1].split(':'))
            end = datetime.combine(NOW.date(), time(hour=int(eh),minute=int(em)))

            return (start, end)
    return None

def main():
    try:
        print(f"starting scan at {NOW}")

        with open(FILE) as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in reader:
                id = row[0]
                period = row[1]
                cmd0 = row[2]
                cmd1 = row[3]
                print(f"scanning [{id}]: period:{period}, cmd0:{cmd0}, cmd1:{cmd1}")

                p = getPeriod(period, date.today().weekday())
                action = cmd1
                if p is not None:
                    startedAt, endedAt = p
                    if NOW >= startedAt and NOW < endedAt:
                        action = cmd0

                print(f"action to be taken [{id}]: {action}")
                subprocess.Popen(action, shell=True)        

            print(f"scan finished")
    except ValueError as e:
        print(e)

class TestScheduler(unittest.TestCase):
    def test_should_return_timestamps(self):
        period1 = "M07:00-16:00"
        period2 = "M07:00-16:00,T09:00-18:00"
        day = 1
        self.assertEqual(
            getPeriod(period1, day), 
            (
                datetime.combine(NOW.date(), time(hour=7,minute=0)),
                datetime.combine(NOW.date(), time(hour=16,minute=0)),
            )
        )
        self.assertEqual(
            getPeriod(period1, day), 
            (
                datetime.combine(NOW.date(), time(hour=7,minute=0)),
                datetime.combine(NOW.date(), time(hour=16,minute=0)),
            )
        )

    def test_should_raise_ValueError(self):
        period1 = "M07:00,16:00,T09:00-18:00"
        period2 = "Y07:00,16:00,T09:00-18:00"
        period3 = "M07:00,16:00,T9:00-18:00"
        day = 1
        with self.assertRaisesRegex(ValueError, "invalid formart for running period"):
           getPeriod(period1, day) 
        with self.assertRaisesRegex(ValueError, "invalid formart for running period"):
           getPeriod(period2, day) 
        with self.assertRaisesRegex(ValueError, "invalid formart for running period"):
           getPeriod(period3, day) 


if __name__ == "__main__":
    main()

