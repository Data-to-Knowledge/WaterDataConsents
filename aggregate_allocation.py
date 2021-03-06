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


def agg_allo(param, allo, use_mapping):
    """

    """
    run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
    print(run_time_start)

    #######################################
    ### Read in source data

    # p = param['source data']['allo_calc']

    # stmt = 'select * from "{schema}"."{table}"'.format(schema=p['schema'], table=p['table'])

    # rv6 = sf.read_table(p['username'], p['password'], p['account'], p['database'], p['schema'], stmt)
    rv6 = allo.copy()

    #######################################
    ### Aggregation

    ## Set bool filters
    # waitaki_su = sw_limits.loc[sw_limits.PlanName == 'Waitaki Catchment', 'SpatialUnitId'].unique()

    gw_bool = (rv6.HydroGroup == 'Groundwater') | rv6.Combined
    sw_bool = (rv6.HydroGroup == 'Surface Water') | (rv6.Combined & ~(rv6.AllocationBlock.isin(use_mapping['WaitakiTable5'].unique())))

    ## Filter for active consents
    active_lst = ['Issued - Active', 'Issued - Inactive', 'Issued - s124 Continuance']
    in_process_lst = ['Application in Process', 'Application Waiting s88', 'Applicant Reviewing', 'Application on Hold', 'Obj or Appeal in Process', 'Application In Process']
    ApplicationStatus_lst = ['New Consent']

    # GW
    gw = rv6[gw_bool].copy()
    gw['SpatialUnitId'] = gw['GwSpatialUnitId']

    gw1 = gw[gw.ConsentStatus.isin(active_lst)].copy()
    gw2 = gw[gw.ConsentStatus.isin(in_process_lst) & gw.ApplicationStatus.isin(ApplicationStatus_lst)].copy()

    zone1 = gw1.groupby(['SpatialUnitId', 'AllocationBlock'])[['AllocatedAnnualVolume']].sum()
    zone2 = gw2.groupby(['SpatialUnitId', 'AllocationBlock'])[['AllocatedAnnualVolume']].sum()

    zone1.rename(columns={'AllocatedAnnualVolume': 'AllocatedVolume'}, inplace=True)
    zone2.rename(columns={'AllocatedAnnualVolume': 'NewAllocationInProgress'}, inplace=True)

    gw3 = pd.concat([zone1, zone2], axis=1).reset_index()
#    zone3.rename(columns={'GwSpatialUnitId': 'SpatialUnitId'}, inplace=True)

    # SW
    sw = rv6[sw_bool].copy()
    sw['SpatialUnitId'] = sw['SwSpatialUnitId']
    sw.loc[sw.AllocationBlock.isin(use_mapping['WaitakiTable5'].unique()), 'AllocationBlock'] = 'A'

    sw_active1 = sw[sw.ConsentStatus.isin(active_lst)].copy()
    sw_process1 = sw[sw.ConsentStatus.isin(in_process_lst) & sw.ApplicationStatus.isin(ApplicationStatus_lst)].copy()

    sw_active_rate1 = sw_active1[~sw_active1.SpatialUnitId.str.contains('CWAZ')].copy()
    sw_active_vol1 = sw_active1[sw_active1.SpatialUnitId.str.contains('CWAZ')].copy()

    sw_process_rate1 = sw_process1[~sw_process1.SpatialUnitId.str.contains('CWAZ')].copy()
    sw_process_vol1 = sw_process1[sw_process1.SpatialUnitId.str.contains('CWAZ')].copy()

    index1 = ['SpatialUnitId', 'AllocationBlock', 'Month']
    calc_col_rate = 'AllocatedRate'

    sw_active_rate2 = split_months(sw_active_rate1, index1, calc_col_rate)
    sw_process_rate2 = split_months(sw_process_rate1, index1, calc_col_rate)
    sw_process_rate2.rename(columns={'AllocatedRate': 'NewAllocationInProgress'}, inplace=True)

    sw_rate2 = pd.merge(sw_active_rate2, sw_process_rate2, on=['SpatialUnitId', 'AllocationBlock', 'Month'], how='left')

    calc_col_vol = 'AllocatedAnnualVolume'

    sw_active_vol2 = split_months(sw_active_vol1, index1, calc_col_vol)
    sw_active_vol2.rename(columns={'AllocatedAnnualVolume': 'AllocatedRate'}, inplace=True)
    sw_process_vol2 = split_months(sw_process_vol1, index1, calc_col_vol)
    sw_process_vol2.rename(columns={'AllocatedAnnualVolume': 'NewAllocationInProgress'}, inplace=True)

    sw_vol2 = pd.merge(sw_active_vol2, sw_process_vol2, on=['SpatialUnitId', 'AllocationBlock', 'Month'], how='left')

    sw2 = pd.concat([sw_vol2, sw_rate2])

    ## Save results
    print('Save results')

    # GW summary
    gw3['EffectiveFromDate'] = run_time_start
    out_param = param['source data']['gw_zone_allo']
    sf.to_table(gw3, out_param['table'], out_param['username'], out_param['password'], out_param['account'], out_param['database'], out_param['schema'], True)

    # SW summary
    sw2['EffectiveFromDate'] = run_time_start
    out_param = param['source data']['sw_zone_allo']
    sf.to_table(sw2, out_param['table'], out_param['username'], out_param['password'], out_param['account'], out_param['database'], out_param['schema'], True)

    return gw3, sw2




















