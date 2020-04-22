# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 10:44:50 2020

@author: MichaelEK
"""
import types
import json
import os
import yaml
from pdsql import mssql, util
import pandas as pd
from pdsf import sflake as sf

##########################################
### Parameters

base_dir = os.path.split(os.path.realpath(os.path.dirname(__file__)))[0]

with open(os.path.join(base_dir, 'parameters-dev.yml')) as param:
    param = yaml.safe_load(param)

other_schema = 'waterdatarepo_pp'

tables = {'waps': ['Wap'], 'allo_calc': ['RecordNumber', 'HydroGroup', 'AllocationBlock', 'Wap'], 'gw_zone_allo': ['SpatialUnitId', 'AllocationBlock'], 'sw_zone_allo': ['SpatialUnitId', 'AllocationBlock', 'Month'], 'gw_limits': ['ManagementGroupId', 'AllocationBlock'], 'sw_limits': ['ManagementGroupId', 'AllocationBlock', 'Month'], 'permit_use': ['RecordNumber']}

#########################################
### Comparisons

## Make object to contain the source data
diff_dict = {}

for t in tables:
    p = param['source data'][t]
    print(p['table'])
    if p['schema'] != 'public':
        stmt = 'select * from "{schema}"."{table}"'.format(schema=p['schema'], table=p['table'])
    else:
        stmt = 'select * from "{table}"'.format(table=p['table'])
    s1 = sf.read_table(p['username'], p['password'], p['account'], p['database'], p['schema'], stmt).drop('EffectiveFromDate', axis=1)
    n1 = sf.read_table(p['username'], p['password'], p['account'], p['database'], other_schema, stmt).drop('EffectiveFromDate', axis=1)

    # Compare dfs
    comp_dict = util.compare_dfs(s1, n1, on=tables[t])

    if comp_dict['diff'].empty & comp_dict['new'].empty & comp_dict['remove'].empty:
        print('All good')
    else:
        print('problems...')
        diff_dict[t] = comp_dict
