#!/usr/bin/env python
# coding: utf-8

# In[ ]:
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import os
import pandas as pd
import numpy as np
import datetime as dt
from collections import Counter
import random
import warnings

warnings.filterwarnings('ignore')

# AWS
import boto3
from botocore.exceptions import ClientError
from io import StringIO


def readfromS3(filename):
    path = 's3://lendingclubdatastore/'
    bucket = path + filename  # already created on S3
    client = boto3.resource('s3')
    df = pd.read_csv(bucket)
    return df


def removeOutliers(loans):
    loans = loans[loans['loan_status'].isin(['Fully Paid', 'Default', 'Charged Off'])]
    loans = loans[loans['int_rate'] <= 25]
    loans = loans[loans['loan_amnt'] <= 35000]
    return loans


def replaceValues(loans):
    loans = loans.replace({'loan_status': 'Charged Off'}, 'Default')
    loans = loans.replace({'term': ' 36 months'}, 36)
    loans = loans.replace({'term': ' 60 months'}, 60)

    loans = loans.replace({'emp_length': '5 years'}, 5)
    loans = loans.replace({'emp_length': '10+ years'}, 12)
    loans = loans.replace({'emp_length': '4 years'}, 4)
    loans = loans.replace({'emp_length': '3 years'}, 3)
    loans = loans.replace({'emp_length': '1 year'}, 1)
    loans = loans.replace({'emp_length': '< 1 year'}, 0.5)
    loans = loans.replace({'emp_length': '8 years'}, 8)
    loans = loans.replace({'emp_length': '2 years'}, 2)
    loans = loans.replace({'emp_length': '6 years'}, 6)
    loans = loans.replace({'emp_length': '9 years'}, 9)
    loans = loans.replace({'emp_length': '7 years'}, 7)

    return loans


# noinspection PyUnresolvedReferences
def handleNullValues(loans):
    loans = loans.drop(['id', 'sub_grade', 'emp_title', 'url', 'desc', 'purpose', 'title', 'zip_code', 'addr_state',
                        'last_pymnt_d', 'next_pymnt_d', 'last_credit_pull_d', 'sec_app_earliest_cr_line',
                        'hardship_start_date', 'hardship_end_date', 'payment_plan_start_date',
                        'debt_settlement_flag_date', 'settlement_date'], axis=1)

    member_id = []

    for i in range(1, len(loans.index) + 1):
        member_id.append(i)

    loans.loc[np.isnan(loans['member_id']), 'member_id'] = member_id

    emp_length = int(loans['emp_length'].mean(skipna=True))
    loans.loc[np.isnan(loans['emp_length']), 'emp_length'] = emp_length

    dti_mean = loans['dti'].mean(skipna=True)
    loans.loc[np.isnan(loans['dti']), 'dti'] = dti_mean

    mts_record_mean = loans['mths_since_last_record'].mean(skipna=True)
    loans.loc[np.isnan(loans['mths_since_last_record']), 'mths_since_last_record'] = mts_record_mean

    revol_util = loans['revol_util'].mean(skipna=True)
    loans.loc[np.isnan(loans['revol_util']), 'revol_util'] = revol_util

    collections_12_mths_ex_med = loans['collections_12_mths_ex_med'].mean(skipna=True)
    loans.loc[np.isnan(loans['collections_12_mths_ex_med']), 'collections_12_mths_ex_med'] = revol_util

    loans.loc[np.isnan(loans['annual_inc_joint']), 'annual_inc_joint'] = 0

    loans.loc[np.isnan(loans['dti_joint']), 'dti_joint'] = 0

    loans['verification_status_joint'] = loans['verification_status_joint'].replace(np.NaN, 'NA')

    tot_coll_amt = loans['tot_coll_amt'].mean(skipna=True)
    loans.loc[np.isnan(loans['tot_coll_amt']), 'tot_coll_amt'] = tot_coll_amt

    tot_cur_bal = loans['tot_cur_bal'].mean(skipna=True)
    loans.loc[np.isnan(loans['tot_cur_bal']), 'tot_cur_bal'] = tot_cur_bal

    loans.loc[np.isnan(loans['open_acc_6m']), 'open_acc_6m'] = 0
    loans.loc[np.isnan(loans['open_act_il']), 'open_act_il'] = 0
    loans.loc[np.isnan(loans['open_il_12m']), 'open_il_12m'] = 0
    loans.loc[np.isnan(loans['open_il_24m']), 'open_il_24m'] = 0

    mths_since_rcnt_il = loans['mths_since_rcnt_il'].mean(skipna=True)
    loans.loc[np.isnan(loans['mths_since_rcnt_il']), 'mths_since_rcnt_il'] = mths_since_rcnt_il

    total_bal_il = loans['total_bal_il'].mean(skipna=True)
    loans.loc[np.isnan(loans['total_bal_il']), 'total_bal_il'] = total_bal_il

    il_util = loans['il_util'].mean(skipna=True)
    loans.loc[np.isnan(loans['il_util']), 'il_util'] = il_util

    open_rv_12m = loans['open_rv_12m'].mean(skipna=True)
    loans.loc[np.isnan(loans['open_rv_12m']), 'open_rv_12m'] = open_rv_12m

    open_rv_24m = loans['open_rv_24m'].mean(skipna=True)
    loans.loc[np.isnan(loans['open_rv_24m']), 'open_rv_24m'] = open_rv_24m

    max_bal_bc = loans['max_bal_bc'].mean(skipna=True)
    loans.loc[np.isnan(loans['max_bal_bc']), 'max_bal_bc'] = max_bal_bc

    all_util = loans['all_util'].mean(skipna=True)
    loans.loc[np.isnan(loans['all_util']), 'all_util'] = all_util

    total_rev_hi_lim = loans['total_rev_hi_lim'].mean(skipna=True)
    loans.loc[np.isnan(loans['total_rev_hi_lim']), 'total_rev_hi_lim'] = total_rev_hi_lim

    inq_fi = loans['inq_fi'].mean(skipna=True)
    loans.loc[np.isnan(loans['inq_fi']), 'inq_fi'] = inq_fi

    total_cu_tl = loans['total_cu_tl'].mean(skipna=True)
    loans.loc[np.isnan(loans['total_cu_tl']), 'total_cu_tl'] = total_cu_tl

    inq_last_6mths = loans['inq_last_6mths'].mean(skipna=True)
    loans.loc[np.isnan(loans['inq_last_6mths']), 'inq_last_6mths'] = inq_last_6mths

    mths_since_last_delinq = loans['mths_since_last_delinq'].mean(skipna=True)
    loans.loc[np.isnan(loans['mths_since_last_delinq']), 'mths_since_last_delinq'] = mths_since_last_delinq

    mths_since_last_major_derog = loans['mths_since_last_major_derog'].mean(skipna=True)
    loans.loc[
        np.isnan(loans['mths_since_last_major_derog']), 'mths_since_last_major_derog'] = mths_since_last_major_derog

    inq_last_12m = loans['inq_last_12m'].mean(skipna=True)
    loans.loc[np.isnan(loans['inq_last_12m']), 'inq_last_12m'] = inq_last_12m

    acc_open_past_24mths = loans['acc_open_past_24mths'].mean(skipna=True)
    loans.loc[np.isnan(loans['acc_open_past_24mths']), 'acc_open_past_24mths'] = acc_open_past_24mths

    avg_cur_bal = loans['avg_cur_bal'].mean(skipna=True)
    loans.loc[np.isnan(loans['avg_cur_bal']), 'avg_cur_bal'] = avg_cur_bal

    bc_open_to_buy = loans['bc_open_to_buy'].mean(skipna=True)
    loans.loc[np.isnan(loans['bc_open_to_buy']), 'bc_open_to_buy'] = bc_open_to_buy

    bc_util = loans['bc_util'].mean(skipna=True)
    loans.loc[np.isnan(loans['bc_util']), 'bc_util'] = bc_util

    chargeoff_within_12_mths = loans['chargeoff_within_12_mths'].mean(skipna=True)
    loans.loc[np.isnan(loans['chargeoff_within_12_mths']), 'chargeoff_within_12_mths'] = chargeoff_within_12_mths

    mo_sin_old_il_acct = loans['mo_sin_old_il_acct'].mean(skipna=True)
    loans.loc[np.isnan(loans['mo_sin_old_il_acct']), 'mo_sin_old_il_acct'] = mo_sin_old_il_acct

    mo_sin_old_rev_tl_op = loans['mo_sin_old_rev_tl_op'].mean(skipna=True)
    loans.loc[np.isnan(loans['mo_sin_old_rev_tl_op']), 'mo_sin_old_rev_tl_op'] = mo_sin_old_rev_tl_op

    mo_sin_rcnt_rev_tl_op = loans['mo_sin_rcnt_rev_tl_op'].mean(skipna=True)
    loans.loc[np.isnan(loans['mo_sin_rcnt_rev_tl_op']), 'mo_sin_rcnt_rev_tl_op'] = mo_sin_rcnt_rev_tl_op

    mo_sin_rcnt_tl = loans['mo_sin_rcnt_tl'].mean(skipna=True)
    loans.loc[np.isnan(loans['mo_sin_rcnt_tl']), 'mo_sin_rcnt_tl'] = mo_sin_rcnt_tl

    mort_acc = loans['mort_acc'].mean(skipna=True)
    loans.loc[np.isnan(loans['mort_acc']), 'mort_acc'] = mort_acc

    mths_since_recent_bc = loans['mths_since_recent_bc'].mean(skipna=True)
    loans.loc[np.isnan(loans['mths_since_recent_bc']), 'mths_since_recent_bc'] = mths_since_recent_bc

    mths_since_recent_bc_dlq = loans['mths_since_recent_bc_dlq'].mean(skipna=True)
    loans.loc[np.isnan(loans['mths_since_recent_bc_dlq']), 'mths_since_recent_bc_dlq'] = mths_since_recent_bc_dlq

    mths_since_recent_inq = loans['mths_since_recent_inq'].mean(skipna=True)
    loans.loc[np.isnan(loans['mths_since_recent_inq']), 'mths_since_recent_inq'] = mths_since_recent_inq

    mths_since_recent_revol_delinq = loans['mths_since_recent_revol_delinq'].mean(skipna=True)
    loans.loc[np.isnan(
        loans['mths_since_recent_revol_delinq']), 'mths_since_recent_revol_delinq'] = mths_since_recent_revol_delinq

    num_accts_ever_120_pd = loans['num_accts_ever_120_pd'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_accts_ever_120_pd']), 'num_accts_ever_120_pd'] = num_accts_ever_120_pd

    num_actv_bc_tl = loans['num_actv_bc_tl'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_actv_bc_tl']), 'num_actv_bc_tl'] = num_actv_bc_tl

    num_actv_rev_tl = loans['num_actv_rev_tl'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_actv_rev_tl']), 'num_actv_rev_tl'] = num_actv_rev_tl

    num_rev_accts = loans['num_rev_accts'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_rev_accts']), 'num_rev_accts'] = num_rev_accts

    num_bc_sats = loans['num_bc_sats'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_bc_sats']), 'num_bc_sats'] = num_bc_sats

    num_bc_tl = loans['num_bc_tl'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_bc_tl']), 'num_bc_tl'] = num_bc_tl

    num_il_tl = loans['num_il_tl'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_il_tl']), 'num_il_tl'] = num_il_tl

    num_op_rev_tl = loans['num_op_rev_tl'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_op_rev_tl']), 'num_op_rev_tl'] = num_op_rev_tl

    num_rev_tl_bal_gt_0 = loans['num_rev_tl_bal_gt_0'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_rev_tl_bal_gt_0']), 'num_rev_tl_bal_gt_0'] = num_rev_tl_bal_gt_0

    num_sats = loans['num_sats'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_sats']), 'num_sats'] = num_sats

    num_tl_120dpd_2m = loans['num_tl_120dpd_2m'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_tl_120dpd_2m']), 'num_tl_120dpd_2m'] = num_tl_120dpd_2m

    num_tl_30dpd = loans['num_tl_30dpd'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_tl_30dpd']), 'num_tl_30dpd'] = num_tl_30dpd

    num_tl_90g_dpd_24m = loans['num_tl_90g_dpd_24m'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_tl_90g_dpd_24m']), 'num_tl_90g_dpd_24m'] = num_tl_90g_dpd_24m

    num_tl_op_past_12m = loans['num_tl_op_past_12m'].mean(skipna=True)
    loans.loc[np.isnan(loans['num_tl_op_past_12m']), 'num_tl_op_past_12m'] = num_tl_op_past_12m

    pct_tl_nvr_dlq = loans['pct_tl_nvr_dlq'].mean(skipna=True)
    loans.loc[np.isnan(loans['pct_tl_nvr_dlq']), 'pct_tl_nvr_dlq'] = pct_tl_nvr_dlq

    percent_bc_gt_75 = loans['percent_bc_gt_75'].mean(skipna=True)
    loans.loc[np.isnan(loans['percent_bc_gt_75']), 'percent_bc_gt_75'] = percent_bc_gt_75

    pub_rec_bankruptcies = loans['pub_rec_bankruptcies'].mean(skipna=True)
    loans.loc[np.isnan(loans['pub_rec_bankruptcies']), 'pub_rec_bankruptcies'] = pub_rec_bankruptcies

    tax_liens = loans['tax_liens'].mean(skipna=True)
    loans.loc[np.isnan(loans['tax_liens']), 'tax_liens'] = tax_liens

    total_il_high_credit_limit = loans['total_il_high_credit_limit'].mean(skipna=True)
    loans.loc[np.isnan(loans['total_il_high_credit_limit']), 'total_il_high_credit_limit'] = total_il_high_credit_limit

    tot_hi_cred_lim = loans['tot_hi_cred_lim'].mean(skipna=True)
    loans.loc[np.isnan(loans['tot_hi_cred_lim']), 'tot_hi_cred_lim'] = tot_hi_cred_lim

    total_bal_ex_mort = loans['total_bal_ex_mort'].mean(skipna=True)
    loans.loc[np.isnan(loans['total_bal_ex_mort']), 'total_bal_ex_mort'] = total_bal_ex_mort

    total_bc_limit = loans['total_bc_limit'].mean(skipna=True)
    loans.loc[np.isnan(loans['total_bc_limit']), 'total_bc_limit'] = total_bc_limit

    loans.loc[np.isnan(loans['revol_bal_joint']), 'revol_bal_joint'] = 0

    # sec_app_inq_last_6mths = loans['sec_app_inq_last_6mths'].mean(skipna = True)
    loans.loc[np.isnan(loans['sec_app_inq_last_6mths']), 'sec_app_inq_last_6mths'] = 0

    # sec_app_revol_util = loans['tot_cur_bal']/loans['tot_hi_cred_lim']
    loans.loc[np.isnan(loans['sec_app_revol_util']), 'sec_app_revol_util'] = 0

    # sec_app_open_act_il = loans['sec_app_open_act_il'].mean(skipna = True)
    loans.loc[np.isnan(loans['sec_app_open_act_il']), 'sec_app_open_act_il'] = 0

    # sec_app_num_rev_accts = loans['sec_app_num_rev_accts'].mean(skipna = True)
    loans.loc[np.isnan(loans['sec_app_num_rev_accts']), 'sec_app_num_rev_accts'] = 0

    # sec_app_chargeoff_within_12_mths = loans['sec_app_chargeoff_within_12_mths'].mean(skipna = True)
    loans.loc[np.isnan(loans['sec_app_chargeoff_within_12_mths']), 'sec_app_chargeoff_within_12_mths'] = 0

    # sec_app_collections_12_mths_ex_med = loans['sec_app_collections_12_mths_ex_med'].mean(skipna = True)
    loans.loc[np.isnan(loans['sec_app_collections_12_mths_ex_med']), 'sec_app_collections_12_mths_ex_med'] = 0

    # sec_app_mths_since_last_major_derog = loans['sec_app_mths_since_last_major_derog'].mean(skipna = True)
    loans.loc[np.isnan(loans['sec_app_mths_since_last_major_derog']), 'sec_app_mths_since_last_major_derog'] = 0

    # sec_app_num_rev_accts = loans['sec_app_num_rev_accts'].mean(skipna = True)
    loans.loc[np.isnan(loans['sec_app_num_rev_accts']), 'sec_app_num_rev_accts'] = 0

    loans.loc[np.isnan(loans['sec_app_mort_acc']), 'sec_app_mort_acc'] = 0

    loans.loc[np.isnan(loans['sec_app_open_acc']), 'sec_app_open_acc'] = 0

    loans['hardship_type'] = loans['hardship_type'].replace(np.NaN, 'NA')

    loans['hardship_reason'] = loans['hardship_reason'].replace(np.NaN, 'NA')

    loans['hardship_status'] = loans['hardship_status'].replace(np.NaN, 'NA')

    deferral_term = loans['deferral_term'].mean(skipna=True)
    loans.loc[np.isnan(loans['deferral_term']), 'deferral_term'] = deferral_term

    # hardship_amount = loans['hardship_amount'].mean(skipna = True)
    loans.loc[np.isnan(loans['hardship_amount']), 'hardship_amount'] = 0

    # hardship_length = loans['hardship_length'].mean(skipna = True)
    loans.loc[np.isnan(loans['hardship_length']), 'hardship_length'] = 0

    # hardship_dpd = loans['hardship_dpd'].mean(skipna = True)
    loans.loc[np.isnan(loans['hardship_dpd']), 'hardship_dpd'] = 0

    loans['hardship_loan_status'] = loans['hardship_loan_status'].replace(np.NaN, 'NA')

    orig_projected_additional_accrued_interest = loans['orig_projected_additional_accrued_interest'].mean(skipna=True)
    loans.loc[np.isnan(loans[
                           'orig_projected_additional_accrued_interest']), 'orig_projected_additional_accrued_interest'] = orig_projected_additional_accrued_interest

    loans.loc[np.isnan(loans['hardship_payoff_balance_amount']), 'hardship_payoff_balance_amount'] = 0

    loans.loc[np.isnan(loans['hardship_last_payment_amount']), 'hardship_last_payment_amount'] = 0

    loans['settlement_status'] = loans['settlement_status'].replace(np.NaN, 'NA')

    loans.loc[np.isnan(loans['settlement_amount']), 'settlement_amount'] = 0

    loans.loc[np.isnan(loans['settlement_percentage']), 'settlement_percentage'] = 0

    loans.loc[np.isnan(loans['settlement_term']), 'settlement_term'] = 0

    print(loans.columns[loans.isnull().any()])

    return loans


def featureEng(loans):
    loans['return'] = (((loans['total_pymnt'] - loans['funded_amnt']) / loans['funded_amnt']) * (
            12 / loans['term'])) * 100
    return loans


def uploadToS3(result, filename):
    try:
        bucket = 'lendingclubdatastore'  # already created on S3
        csv_buffer = StringIO()
        result.to_csv(csv_buffer, index=False)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())
    except ClientError as e:
        logging.error(e)
        return False
    return True


dag = DAG('data_cleaning_feature_engineering',
          description='Data Cleaning Lending Club Loan dataset',
          schedule_interval='0 12 * * *',
          start_date=datetime(2020, 7, 19), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

read_from_S3 = PythonOperator(task_id='read_from_s3', python_callable=readfromS3,
                              op_kwargs={'filename': 'loan.csv'}, dag=dag)

remove_outlier_operator = PythonOperator(task_id='remove_outliers', op_kwargs={'loans': read_from_S3},
                                         python_callable=removeOutliers, dag=dag)

replace_values = PythonOperator(task_id='replace_values', python_callable=replaceValues,
                                op_kwargs={'loans': remove_outlier_operator}, dag=dag)

handle_nulls = PythonOperator(task_id='handle_nulls', python_callable=handleNullValues,
                              op_kwargs={'loans': replace_values}, dag=dag)

faeture_eng = PythonOperator(task_id='faeture_eng', python_callable=featureEng,
                             op_kwargs={'loans': handle_nulls}, dag=dag)

upload = PythonOperator(task_id='upload_to_S3', python_callable=uploadToS3,
                        op_kwargs={'loans': faeture_eng}, dag=dag)

dummy_operator >> read_from_S3 >> replace_values >> handle_nulls >> faeture_eng >> upload

