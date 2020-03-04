# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 15:42:23 2020

@author: MichaelEK
"""
import pandas as pd
import numpy as np
import json
from pdsf import sflake as sf
from utils import split_months


def agg_allo(param, sw_limits):
    """

    """
    run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
    print(run_time_start)

    #######################################
    ### Read in source data

    p = param['source data']['allo_calc']

    stmt = 'select * from "{schema}"."{table}"'.format(schema=p['schema'], table=p['table'])

    rv6 = sf.read_table(p['username'], p['password'], p['account'], p['database'], p['schema'], stmt)

    #######################################
    ### Aggregation

    ## Set bool filters
    waitaki_su = sw_limits.loc[sw_limits.PlanName == 'Waitaki Catchment', 'SpatialUnitId'].unique()

    gw_bool = (rv6.HydroGroup == 'Groundwater') | rv6.Combined
    sw_bool = (rv6.HydroGroup == 'Surface Water') & (~rv6.Combined | rv6['SpatialUnitId'].isin(waitaki_su))

    ## Filter for active consents
    active_bool = rv6.ConsentStatus.isin(['Issued - Active', 'Issued - Inactive', 'Issued - s124 Continuance'])
    in_process_bool = rv6.ConsentStatus.isin(['Application in Process', 'Application Waiting s88', 'Applicant Reviewing', 'Application on Hold', 'Obj or Appeal in Process', 'Application In Process']) & rv6.ApplicationStatus.isin(['New Consent'])

    # GW
    gw1 = rv6[gw_bool & active_bool].copy()
    gw2 = rv6[gw_bool & in_process_bool].copy()
    zone1 = gw1.groupby(['SpatialUnitId', 'AllocationBlock'])[['AllocatedAnnualVolume']].sum()
    zone2 = gw2.groupby(['SpatialUnitId', 'AllocationBlock'])[['AllocatedAnnualVolume']].sum()

    zone1.rename(columns={'AllocatedAnnualVolume': 'AllocatedVolume'}, inplace=True)
    zone2.rename(columns={'AllocatedAnnualVolume': 'NewAllocationInProgress'}, inplace=True)

    zone3 = pd.concat([zone1, zone2], axis=1).reset_index()
#    zone3.rename(columns={'GwSpatialUnitId': 'SpatialUnitId'}, inplace=True)

    # SW
    sw_active1 = rv6[sw_bool & active_bool].copy()
    sw_process1 = rv6[sw_bool & in_process_bool].copy()

    index1 = ['SpatialUnitId', 'AllocationBlock', 'FromMonth']
    month_col = 'FromMonth'
    calc_col = 'AllocatedRate'

    sw_active2 = split_months(sw_active1, index1, month_col, calc_col)
    sw_process2 = split_months(sw_process1, index1, month_col, calc_col)
    sw_process2.rename(columns={'AllocatedRate': 'NewAllocationInProgress'}, inplace=True)

    sw2 = pd.merge(sw_active2, sw_process2, on=['SpatialUnitId', 'AllocationBlock', 'Month'], how='left')
#    sw2.rename(columns={'SwSpatialUnitId': 'SpatialUnitId'}, inplace=True)

    ## Save results
    print('Save results')

    # GW summary
    zone3['EffectiveFromDate'] = run_time_start
    out_param = param['source data']['gw_zone_allo']
    sf.to_table(zone3, out_param['table'], out_param['username'], out_param['password'], out_param['account'], out_param['database'], out_param['schema'], True)

    # SW summary
    sw2['EffectiveFromDate'] = run_time_start
    out_param = param['source data']['sw_zone_allo']
    sf.to_table(sw2, out_param['table'], out_param['username'], out_param['password'], out_param['account'], out_param['database'], out_param['schema'], True)























