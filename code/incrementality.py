# -*- coding: utf-8 -*-
"""
Created on Mon May 08 15:25:46 2017

@author: thyde
"""

import pyodbc
import xlwings as xw
from pandas.io import sql
import pandas as pd


wb = xw.Book('U:\\incremental_test.xlsm')
sht = wb.sheets['incremental']

def update_query(initials,startdate,enddate,list1,list2,list3):
    if list1:
        bau_template='''SELECT
  COUNT(DISTINCT visits) AS visits
  , COUNT(DISTINCT visitors) AS visitors
  , SUM(conversions) AS conversions
  , SUM(totalrevenue) AS revenue
  , AVG(CASE
  	WHEN totalrevenue > 0 THEN LN(totalrevenue)
  	ELSE NULL
  	END) AS avg_rev
  , STDDEV_POP(CASE
  	WHEN totalrevenue > 0 THEN LN(totalrevenue)
  	ELSE NULL
  	END) AS sd_rev
FROM tmpTable
WHERE testgroupname in ({test_group1})
''' 
    if list2:
        test_template = '''SELECT
  COUNT(DISTINCT visits) AS visits
  , COUNT(DISTINCT visitors) AS visitors
  , SUM(conversions) AS conversions
  , SUM(totalrevenue) AS revenue
  , AVG(CASE
  	WHEN totalrevenue > 0 THEN LN(totalrevenue)
  	ELSE NULL
  	END) AS avg_rev
  , STDDEV_POP(CASE
  	WHEN totalrevenue > 0 THEN LN(totalrevenue)
  	ELSE NULL
  	END) AS sd_rev
FROM tmpTable
WHERE testgroupname in ({test_group2})
''' 
    if list3:
        control_template='''SELECT
  COUNT(DISTINCT visits) AS visits
  , COUNT(DISTINCT visitors) AS visitors
  , SUM(conversions) AS conversions
  , SUM(totalrevenue) AS revenue
  , AVG(CASE
  	WHEN totalrevenue > 0 THEN LN(totalrevenue)
  	ELSE NULL
  	END) AS avg_rev
  , STDDEV_POP(CASE
  	WHEN totalrevenue > 0 THEN LN(totalrevenue)
  	ELSE NULL
  	END) AS sd_rev
FROM tmpTable
WHERE testgroupname in ({test_group3})
''' 

    x1=','.join(list1)
    x2=','.join(list2)
    x3=','.join(list3)
    update_bau=""
    update_test=""
    update_control=""
    if list1:
        update_bau = bau_template.format(test_group1=x1)
    if list2:
        update_test = test_template.format(test_group2=x2)
    if list3:
        update_control = control_template.format(test_group3=x3)

    #vertica = pyodbc.connect("DSN=Vertica")
    #data = sql.read_sql(update_template, vertica)
    return update_template,update_bau,update_test,update_control

def groups(cell):
    group=sht.range(cell).value
    return group


def main():

    initials = sht.range('B1').value
    test = list()
    bau = list()
    control = list()
    for i in range(1,21):
        cell = 'B{}'.format(i+5)
        resp = 'A{}'.format(i+5)
        group = groups(cell)
        if group=='TEST':
            test.append('\'{}\''.format(groups(resp)))
        elif group=='CONTROL':
            control.append('\'{}\''.format(groups(resp)))
        else:
            bau.append('\'{}\''.format(groups(resp)))
    startdate = sht.range('B3').value
    enddate = sht.range('B4').value
    query,select_bau,select_test,select_control=update_query(initials,startdate,enddate,bau,test,control)

    cursor=vertica.cursor()
    cursor.execute(query)
    df_bau=pd.DataFrame()
    df_test=pd.DataFrame()
    df_control=pd.DataFrame()
    if select_bau:
        df_bau = sql.read_sql(select_bau,vertica)
        sht.range('B30').options(index=False,header=False).value=df_bau
    if select_test:
        df_test = sql.read_sql(select_test,vertica)
        sht.range('B31').options(index=False,header=False).value=df_test
    if select_control:
        df_control = sql.read_sql(select_control,vertica)
        sht.range('B32').options(index=False,header=False).value=df_control
             

main()


