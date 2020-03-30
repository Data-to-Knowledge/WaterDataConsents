# -*- coding: utf-8 -*-
"""
Created on Thu Jun  7 11:41:44 2018

@author: MichaelEK
"""
import pandas as pd
import numpy as np
from pdsf import sflake as sf
from utils import process_limit_data, assign_notes, extract_spatial_units


def process_limits(param, json_lst):
    """

    """

    run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
    print(run_time_start)

    #######################################
    ### Read in source data and update accela tables in ConsentsReporting db
    print('--Reading in source data...')

    hydro_units, sg1 = extract_spatial_units(json_lst)
    l_data1, t_data1, units = process_limit_data(json_lst)
#    sg = assign_notes(sg1).drop_duplicates('spatialId').drop('HydroGroup', axis=1)
    sg = assign_notes(sg1)
    sg.notes = sg.notes.str[:300]

    ##################################################
    ### GW limits
    print('--Process GW limits')

    # gw_sg = sg[sg.HydroGroup == 'Groundwater'].drop_duplicates('spatialId').drop('HydroGroup', axis=1)
    gw_sg = sg[sg.HydroGroup == 'Groundwater'].drop('HydroGroup', axis=1)

    gw_combo1 = pd.merge(gw_sg, t_data1.drop(['fromMonth', 'toMonth'], axis=1), on='id')
    gw_combo1.rename(columns={'id': 'ManagementGroupId', 'spatialId': 'SpatialUnitId', 'Allocation Block': 'AllocationBlock', 'name': 'Name', 'planName': 'PlanName', 'planSection': 'PlanSection', 'planTable': 'PlanTable', 'limit': 'AllocationLimit', 'units': 'Units', 'notes': 'Notes'}, inplace=True)

    ## Save results
    print('Save results')

    gw_combo1['EffectiveFromDate'] = run_time_start
    out_param = param['source data']['gw_limits']
    sf.to_table(gw_combo1, out_param['table'], out_param['username'], out_param['password'], out_param['account'], out_param['database'], out_param['schema'], True)

    ##################################################
    ### SW limits
    print('--Process SW limits')

    # sw_sg = sg[sg.HydroGroup == 'Surface Water'].drop_duplicates('spatialId').drop('HydroGroup', axis=1)
    sw_sg = sg[sg.HydroGroup == 'Surface Water'].drop('HydroGroup', axis=1)
    t_data2 = t_data1.drop(['units', 'fromMonth', 'toMonth', 'limit'], axis=1).drop_duplicates(['id', 'Allocation Block'])

    sw_combo1 = pd.merge(sw_sg, l_data1, on='id')
    sw_combo2 = pd.merge(sw_combo1, t_data2, on=['id', 'Allocation Block'])
    sw_combo2.rename(columns={'id': 'ManagementGroupId', 'spatialId': 'SpatialUnitId', 'Allocation Block': 'AllocationBlock', 'name': 'Name', 'planName': 'PlanName', 'planSection': 'PlanSection', 'planTable': 'PlanTable', 'Limit': 'AllocationLimit', 'units': 'Units', 'notes': 'Notes'}, inplace=True)

    ## Save results
    print('Save results')

    sw_combo2['EffectiveFromDate'] = run_time_start
    out_param = param['source data']['sw_limits']
    sf.to_table(sw_combo2, out_param['table'], out_param['username'], out_param['password'], out_param['account'], out_param['database'], out_param['schema'], True)

    return gw_combo1, sw_combo2


