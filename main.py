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
from process_use_types import process_use_types
from utils import json_filters, get_json_from_api

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
#d
parser = argparse.ArgumentParser()
parser.add_argument('yaml_path')
args = parser.parse_args()

with open(args.yaml_path) as param:
    param = yaml.safe_load(param)

########################################
### Pull out the limits data

json_lst1 = get_json_from_api(param['misc']['PlanLimits']['api_url'], param['misc']['PlanLimits']['api_headers'])
json_lst = json_filters(json_lst1, only_operative=True)

########################################
### Run the process

print('---Process the Waps')
waps = process_waps(param, json_lst)

print('---Process use types')
permit_use = process_use_types(param)

print('---Process the Allocation')
allo = process_allo(param)

print('---Process the Limits')
gw_combo1, sw_limits = process_limits(param, json_lst)

print('---Aggregate the Allocation')
gw_agg, sw_agg = agg_allo(param, sw_limits, allo)
