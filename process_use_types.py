# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 16:19:11 2020

@author: MichaelEK
"""
import types
from pdsql import mssql
import pandas as pd
import numpy as np
import json
from pdsf import sflake as sf

##########################################


def process_use_types(param):
    """

    """
    run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
    print(run_time_start)

    print('--Reading in source data...')
    db = types.SimpleNamespace()

    for t in param['misc']['UseProcessing']['tables']:
        p = param['source data'][t]
        print(p['table'])
        if 'account' in p:
            if p['schema'] != 'public':
                stmt = 'select {cols} from "{schema}"."{table}"'.format(schema=p['schema'], table=p['table'], cols=json.dumps(p['col_names'])[1:-1])
            else:
                stmt = 'select {cols} from "{table}"'.format(table=p['table'], cols=json.dumps(p['col_names'])[1:-1])
            setattr(db, t, sf.read_table(p['username'], p['password'], p['account'], p['database'], p['schema'], stmt))
        else:
            setattr(db, t, mssql.rd_sql(p['server'], p['database'], p['schema'] +'.'+ p['table'], p['col_names'], username=p['username'], password=p['password']))

    ## Clean up data
    permit_use1 = db.accela_use.copy()
    permit_use1['RecordNumber'] = permit_use1['RecordNumber'].str.upper().str.strip()
    permit_use1['Activity'] = permit_use1['Activity'].str.title().str.strip()
    permit_use1['UseType'] = permit_use1['UseType'].str.strip()
    permit_use2 = permit_use1.drop_duplicates().copy()
#    permit_use2 = permit_use1.drop_duplicates(['RecordNumber']).drop('Activity', axis=1).copy()

    ## Check that all use types exist in the mapping table
    uses1 = db.use_mapping.Accela.unique()
    uses2 = permit_use2['UseType'].unique()
    mis_uses = uses2[~np.in1d(uses2, uses1)]
    if mis_uses.shape[0] > 0:
        raise ValueError('Missing some use types')

    ## Combine other use types
    permit_use2.rename(columns={'UseType': 'Accela'}, inplace=True)
    permit_use3 = pd.merge(permit_use2, db.use_mapping, on='Accela')
    permit_use4 = pd.merge(permit_use3, db.use_priorities, on='WaitakiTable5')
    permit_use4.sort_values('Rank', inplace=True)
    permit_use5 = permit_use4.drop_duplicates(['RecordNumber']).drop(['Activity', 'Rank'], axis=1).copy()

    ## Save
    print('--Save results')
    permit_use5['EffectiveFromDate'] = run_time_start
    out_param = param['source data']['permit_use']
    sf.to_table(permit_use5, out_param['table'], out_param['username'], out_param['password'], out_param['account'], out_param['database'], out_param['schema'], True)

    return permit_use5, db.use_mapping.copy()
















