#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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


# def uploadToS3(result, filename):
#     try:
#         bucket = 'lendingclubdatastore' # already created on S3
#         csv_buffer = StringIO()
#         result.to_csv(csv_buffer)
#         s3_resource = boto3.resource('s3')
#         s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())
#     except ClientError as e:
#         logging.error(e)
#         return False
#     return True

def readfromS3(filename):
    path = 's3://lendingclubdatastore/'
    bucket = path + filename  # already created on S3
    client = boto3.resource('s3')
    df = pd.read_csv(bucket)
    return df


def removeOutliers(loan_data_frame):
    loans = loan_data_frame
    loans = loans[loans['loan_status'].isin(['Fully Paid', 'Default', 'Charged Off'])]
    loans = loans[loans['int_rate'] <= 25]
    loans = loans[loans['loan_amnt'] <= 35000]
    return loans.shape


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


#     uploadToS3(loans, 'temp.csv')
#     print(loans.shape)
#     print(max(loans['int_rate']))
#     print(max(loans['loan_amnt']))


# In[ ]:


dag = DAG('data_cleaning_feature_engineering', description='Data Cleaning Lending Club Loan dataset',
          schedule_interval='0 12 * * *',
          start_date=datetime(2020, 7, 13), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

read_from_S3 = PythonOperator(task_id='read_from_s3', python_callable=readfromS3,
                              op_kwargs={'filename': 'loans_data.csv'}, dag=dag)

replace_values = PythonOperator(task_id='replace_values', python_callable=replaceValues,
                                op_kwargs={'loans': read_from_S3}, dag=dag)

# remove_outlier_operator = PythonOperator(task_id='remove_outliers', op_kwargs={'loan_data_frame': read_from_S3},
#                                          python_callable=removeOutliers, dag=dag)

dummy_operator >> read_from_S3 >> replace_values
