import FinanceDataReader as fdr
import requests
import time
import os
import pandas as pd
from datetime import datetime as dt
import boto3
from io import StringIO
from botocore.client import Config
from dateutil.relativedelta import *

def get_csv_from_S3(key_id : str, secret_key_id : str, bucket_name : str, path : str)-> str: # df = dataFrame
    try:
        sesssion = boto3.Session(key_id, secret_key_id)
        s3_resource = sesssion.resource('s3')
        bucket = s3_resource.Bucket(bucket_name)
        findData = bucket.objects.filter(Prefix=path)
        if len(list(filter(bool, [obj.key.split('/')[1].split('.')[0] for obj in findData]))) > 1:
            raise Exception('동일한 이름의 파일이 2개 이상 존재합니다.')
        else:
            for obj in bucket.objects.filter(Prefix=path):
                key = obj.key
                if key.split('/')[1] == '': continue
                body = obj.get()['Body']
                csv_string = body.read().decode('utf-8')
                df = pd.read_csv(StringIO(csv_string))
            return df
    except Exception as e:
        return e

def upload_df_to_S3(df, key_id : str, secret_key_id : str, bucket_name : str, s3_path : str): # df = dataFrame
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_resource = boto3.resource(
            's3',
            aws_access_key_id=key_id,
            aws_secret_access_key=secret_key_id,
            config=Config(signature_version='s3v4')
        )
        code = s3_resource.Object(bucket_name, s3_path).put(Body = csv_buffer.getvalue())
        if code['ResponseMetadata']['HTTPStatusCode'] == 200:
            return f'\tComplete uploading data to {s3_path} in AWS S3'
        else:
            raise Exception('\nHTTPStautsCode is not 200\nPlease See below error message\n'+code)
    except Exception as e:
        return '\n'+e

def convert_date_format(df, col : str):
    for idx, i in enumerate(df[col]):
        df.loc[idx,col] = df.loc[idx, col].strftime('%Y-%m-%d')
    return df

def get_newest_date(df):
    return df.iloc[-1,0]

def onedaylater(date: str) -> str:
    covert_date = dt.strptime(date, '%Y-%m-%d').date()
    result = covert_date + relativedelta(days=+1)
    return result
ric_list = ['FCPOc1','FCPOc2','FCPOc3', # 팜유1: Bursa Malaysia Crude Palm Oil Commodity Future Continuation 1
            'Sc1', 'Sc2', 'Sc3', # 대두1: CBoT Soybeans Composite Commodity Future Continuation
            'BOc1','BOc2','BOc3', # 대두유: CBoT Soybean Oil Composite Commodity Future Continuation
            'CLc1','CLc2','CLc3', # 국제유가1: NYMEX Light Sweet Crude Oil (WTI) Electronic Energy Future Continuation
            # 'DBLc1','DBLc2','DBLc3',# 국제유가2: NYMEX Mini Dubai Crude Oil (Platts) Electronic Energy Future Continuation 1
            'LCOc1','LCOc2','LCOc3', # 국제유가3: ICE Europe Brent Crude Electronic Energy Future
            'Wc1','Wc2','Wc3', # 소맥1: CBoT Wheat Composite Commodity Future Continuation 1
            ## FX
            'USD/MYR', # 1 USD -> 링기트
            'USD/KRW', # 1 USD -> KRW
            'USD/EUR', # 1 USD -> EUR
            'USD/CNY', # 1 USD -> CNY
            'USD/JPY', # 1 USD -> JPY
            ## Index ##
            'DJI', # Dow Jones Index (DJI)
            'VIX', # CBOE 변동성 지수 (공포지수)
            'KS11', # 코스피
            'US500', # S&P 500 index
            ## 상품 선물 원자재 ##
            'NG', # NG 천연가스 선물 (NYMEX)
            'ZG', # 금 선물 (ICE)
            'ZW', # 소맥 선물 (ICE)
            'ZC', # 옥수수 선물 (ICE)
            'ZL', # 대두유 선물
            'ZS', # 대두 선물
            'ZM', # 대두박 선물
            'ZI', # 은 선물 (ICE)
            'SB', # 설탕 선물(ICE)
            'HG', # 구리 선물 (COMEX)
            'CL', # WTI유 선물 (NYMEX)
            ## 채권 수익률
            'US1MT=X','US3MT=X', 'US6MT=X', 'US1YT=X', # 1,3,6,12개월 만기 미국국채 수익률
            'US10YT=X',# 10년 만기 미국국채 수익률
            ## FRED
            'NASDAQCOM', # 나스닥 지수
            'HSN1F', #주택 판매 지수는 주택시장 활성화 정도를 나타냅니다. 불황으로 주가가 하락할 때 하락에 앞서 주택 판매 지수가 하락하는 모습을 보입니다.
            'M2SL', # M2 통화량은 시중 돈이 얼마나 많이 풀렸는지(유동성)을 보여주는 지표입니다.
            'UNRATE', # 실업률은 불황에는 후행지표, 경기회복에 동행지표로 작동. https://fred.stlouisfed.org/series/UNRATE
            'BAMLH0A0HYM2', # 하이일드 채권 스프레드: 하이일드 채권은 낮은 신용등급 회사에 투자하는 고위험,고수익 채권입니다 (하이일드 채권 스프레드 = 하이일드 채권 수익률 – 국채 수익률)
            'CPIAUCSL', #소비자 물가지수(Consumer Price Index, CPI): 미국 경제에서 인플레이션을 측정하는 두 가지 방식 중 하나, Consumer Price Index for All Urban Consumers: All Items in U.S. City Average
            'DCOILWTICO',#Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma
            'VIXCLS',  #CBOE Volatility Index: VIX (VIXCLS)
            'DCOILBRENTEU',# Crude Oil Prices: Brent - Europe (DCOILBRENTEU)
           ]
start_time = time.time()
# S3 업로드 권한을 가진 계정의 key, secret +  S3 bucket name + 폴더
ACCESS_KEY_ID = '[keyID]'
ACCESS_SECRET_KEY = '[skeyID]'
BUCKET_NAME = '[buket_name]'
folder_name = '[folder_csv_path]'
no_updated_list = []
for ric in ric_list:
    df_old = get_csv_from_S3(ACCESS_KEY_ID, ACCESS_SECRET_KEY, BUCKET_NAME, folder_name + '/' + ric.replace('/','to') + '.csv')
    sdate = onedaylater(get_newest_date(df_old))
    if ric in ['NASDAQCOM','HSN1F','M2SL','UNRATE','BAMLH0A0HYM2','CPIAUCSL','DCOILWTICO','VIXCLS','DCOILBRENTEU']:
        # df_new = fdr.DataReader(ric, start = sdate, end = dt.today().date(), data_source='fred')
        # print(df_new.index[-1])
        continue
    else:
        df_new = fdr.DataReader(symbol = ric, start = sdate)
    if len(df_new) == 0:
        no_updated_list.append(ric)
        print(f'{ric} is empty')
    else:
        df_new.reset_index(inplace=True)
        df_new.iloc[:,0] = df_new.iloc[:,0].astype(str)
        print(ric, '\t','Start day = ', sdate,'\t', 'End day = ', get_newest_date(df_new), end = '', sep = '')
        ric = ric.replace('/','to')
        df_sum = pd.concat([df_old, df_new]).reset_index(drop=True)
        df_sum.to_csv(f'FinanceData/{ric}.csv', index=False) # 백업용
        print(upload_df_to_S3(df_sum, ACCESS_KEY_ID, ACCESS_SECRET_KEY, BUCKET_NAME, folder_name + '/' + ric + '.csv'))
print('no_updated_list:' , no_updated_list)
print("--- %s seconds ---" % (time.time() - start_time))
