#!/usr/bin/env python

"""
This script acts as a simple scheduler that automatically executes the command listed in the file.
CSV file format:
0     |           1             |           2                 |            3
id    |         period          |         cmd0                |           cmd1
0     | 1741796390 - 1741803820 | az aks start -r example ... | az aks stop -r example ...

Column 0 id for identity the task, only for operator to identify the task, the 
scheduler will not use this value.
Column 1 period of the resource should be operational, scheduler will scan this
value and keep the resource in the period operational and stop any resources that 
is outside of period.
Column 2 cmd0 is the shell command for activate the resource
Column 3 cmd1 is the shell command for deactivate the resource
"""

import csv
import time
import subprocess

FILE="schedulebook.csv"

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
        timestamps = period.spliti('-')
        if len(timestamps) != 2:
            print("skiping invalid row")
        startedAt = timestamps[0].strip()
        endedAt = timestamps[1].strip()

        now = time.time()
        if now > endedAt:
            subprocess.Popen(cmd1, shell=True)        
        else:
            subprocess.Popen(cmd0, shell=True)        

print(f"scan finished")
