# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 16:17:36 2020

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql
from pdsf import sflake as sf
import yaml
import json
import types

pd.options.display.max_columns = 10

##############################################
### Parameters

sources = ['waps', 'allo_calc', 'gw_zone_allo', 'sw_zone_allo', 'gw_limits', 'sw_limits', 'dw_well_details']

output_path = r'C:\ecan\shared\projects\water_data_2020\consents'
name_format = '{server}-{db}-{table}-{date}.csv'

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters-dev.yml')) as param:
    param = yaml.safe_load(param)



###############################################
### Extract

run_time_start = pd.Timestamp.today().strftime('%Y%m%d%H%M%S')

## Make object to contain the source data
db = types.SimpleNamespace()

for t in sources:
    p = param['source data'][t]
    print(p['table'])
    if not 'col_names' in p:
        p['col_names'] = ['*']
    if 'account' in p:
        if p['schema'] != 'public':
            stmt = 'select {cols} from "{schema}"."{table}"'.format(schema=p['schema'], table=p['table'], cols=json.dumps(p['col_names'])[1:-1])
        else:
            stmt = 'select {cols} from "{table}"'.format(table=p['table'], cols=json.dumps(p['col_names'])[1:-1])
        stmt = stmt.replace('"*"', '*')
        temp1 = sf.read_table(p['username'], p['password'], p['account'], p['database'], p['schema'], stmt)
        temp1.to_csv(os.path.join(output_path, name_format.format(server='snowflake', db=p['database'], table=p['table'], date=run_time_start)))
#        setattr(db, p['table'], sf.read_table(p['username'], p['password'], p['account'], p['database'], p['schema'], stmt))
    else:
        temp1 = mssql.rd_sql(p['server'], p['database'], p['table'], p['col_names'], username=p['username'], password=p['password'])
        temp1.to_csv(os.path.join(output_path, name_format.format(server=p['server'], db=p['database'], table=p['table'], date=run_time_start)))
#        setattr(db, p['table'], mssql.rd_sql(p['server'], p['database'], p['table'], p['col_names'], username=p['username'], password=p['password']))




















