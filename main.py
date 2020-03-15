# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 13:56:21 2020

@author: MichaelEK
"""
import os
import argparse
import pandas as pd
import numpy as np
import yaml
from process_waps import process_waps
from process_allocation import process_allo
from process_limits import process_limits
from aggregate_allocation import agg_allo

#########################################
### Get todays date-time

pd.options.display.max_columns = 10
#run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
#print(run_time_start)

########################################
### Read in parameters
print('---Read in parameters')

#base_dir = os.path.realpath(os.path.dirname(__file__))
#
#with open(os.path.join(base_dir, 'parameters-dev.yml')) as param:
#    param = yaml.safe_load(param)

parser = argparse.ArgumentParser()
parser.add_argument('yaml_path')
args = parser.parse_args()

with open(args.yaml_path) as param:
    param = yaml.safe_load(param)

########################################
### Run the process

print('---Process the Waps')
waps = process_waps(param)

print('---Process the Allocation')
allo1 = process_allo(param)

print('---Process the Limits')
gw_combo1, sw_limits = process_limits(param)

print('---Aggregate the Allocation')
agg1 = agg_allo(param, sw_limits)
