# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 16:19:11 2020

@author: MichaelEK
"""
import os
import argparse
import types
from pdsql import mssql
from datetime import datetime
import yaml
import itertools
import types
import pandas as pd
import numpy as np
import json
from pdsf import sflake as sf

##########################################


def process_use_types(param):
    """

    """

    ## Split into WAPs by take type equivelant
    wu3 = wu2.copy()
    wu3['take_type'] = wu3['UseType'].str.replace('Use', 'Take')
    wu4 = pd.merge(wu3, mon_min_max1, on=['RecordNumber', 'take_type'])
    wu4['ConsentedMultiDayVolume'] = wu4['ConsentedMultiDayVolume'].divide(wu4['wap_count'], 0).round()
    wu4['ConsentedRate'] = wu4['ConsentedRate'].divide(wu4['wap_count'], 0).round(2)
    wu4.drop(['wap_count', 'take_type'], axis=1, inplace=True)

    ## Convert Use types to broader categories
    types_cat = {}
    for key, value in param['misc']['use_types_codes'].items():
        for string in value:
            types_cat[string] = key
    types_check = np.in1d(wu4.WaterUse.unique(), list(types_cat.keys())).all()
    if not types_check:
        raise ValueError('Some use types are missing in the parameters file. Check the use type table and the parameters file.')
    wu4.WaterUse.replace(types_cat, inplace=True)
    wu4['WaterUse'] = wu4['WaterUse'].astype('category')

    ## Join to get the IDs and filter WAPs
    wu5 = pd.merge(wu4, act_types1[['ActivityID', 'ActivityName']], left_on='UseType', right_on='ActivityName').drop(['UseType', 'ActivityName'], axis=1)
    wu5 = pd.merge(wu5, wap_site, on='WAP').drop('WAP', axis=1)

    ## Drop duplicate uses
    wu5.WaterUse.cat.set_categories(param['misc']['use_types_priorities'], True, inplace=True)
    wu5 = wu5.sort_values('WaterUse')
    wu6 = wu5.drop_duplicates(['RecordNumber', 'ActivityID', 'SiteID']).copy()
















