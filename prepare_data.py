import os
import time
from configparser import ConfigParser
from bs4 import BeautifulSoup
import pandas as pd
import requests
import warnings
import boto3
import logging
from botocore.exceptions import ClientError
warnings.filterwarnings('ignore')


#Read config.ini file
config_object = ConfigParser()
config_object.read("config.ini")

#Get the password
crawl_config = config_object["CRAWLCONFIG"]
file_config = config_object["FILECONFIG"]
bucket_name=config_object['S3']['bucket_name']
'''
    Data process functions 
'''

# get raw html data from url 
def get_html_table(city='Helsinki', page_n=1):
    
    print('get_html_table() city: '+city+' page:'+str(page_n))
    url = (f'https://asuntojen.hintatiedot.fi/haku/?c='+ city +'&cr=1&h=1&h=2&h=3&t=2&l=2&z='+ str(page_n) +
           '&search=1&sf=0&so=a&renderType=renderTypeTable&submit=Next+page')   
    webhtml = requests.get(url).text
    soup = BeautifulSoup(webhtml, 'lxml')
    table_body = soup.find_all('tbody', attrs={'class':['odd','even']})
    return table_body


# get page data and find if there is next page
def find_next_page(city='Helsinki', page_now=1, next_page=False, data=[]):
    d_size = len(data)
    table_body = get_html_table(city=city, page_n=page_now)
    # handle the data
    for ele in table_body[1:]:
        # handle city has no data
        if ele.find_all(text='There are fewer than three results, so no results are displayed.'):
            print('no data for city',city)
            break
        else:
            rows = ele.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                data.append([ele for ele in cols])
    print('find_next_page', page_now, 'next_page: ', next_page, 'data size: ',len(data)- d_size)

    # find if the page contains next page submit button
    for sub_ele in table_body:
        tag_np = sub_ele.find('input', attrs={'type':'submit','value':'Next page'})
        if tag_np:
            next_page = True
            return next_page, data

    return False, data


# GET DATA FROM HTML : asuntojen.hintatiedot.fi
def get_city_df(city ='Helsinki',p_now = 1,nxt_page = False):
    DATA = []
    nxt_page, DATA = find_next_page(city=city, page_now=p_now, next_page=nxt_page,data=DATA)
    while nxt_page:
        p_now += 1
        nxt_page, DATA = find_next_page(city=city, page_now=p_now, next_page=nxt_page, data=DATA)
    df = pd.DataFrame(DATA)
    df['timestamp'] = time.strftime("%Y-%m")
    return df


def clean_raw_data(df,city):
    df_clean = df.copy()
    # Rename columns
    org_columns = ['area','layout','type','size','price','unitprice','year','floor',
                   'elevator','condition','land','elec','timestamp']
    df_clean.columns = org_columns
    # Data Type
    # handle size, if size is string
    if df_clean['size'].dtype.kind == 'O':
        df_clean['size'] = df_clean['size'].str.replace(',','.')
    # to numeric
    num_cols = ['size','price','unitprice','year']
    for col in num_cols:
        df_clean[col]= df_clean[col].astype(float)
    # the rest column to string
    str_cols = list(set(org_columns) - set(num_cols))
    for col in str_cols:
        df_clean[col]= df_clean[col].astype(str)
    # df_raw.isnull().sum()
    # drop if price is empty
    df_clean = df_clean.dropna(subset=['price'])

    if df.shape[0] - df_clean.shape[0] > 20:
        print('! CITY '+ city + ' >20 rows have no price, check out layout column to add data')

    # numeric cols fill nan with median
    df_clean[num_cols] = df_clean[num_cols].fillna(df_clean[num_cols].median())

    return df_clean


def save_df_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)


def save_df(df, base_path, file_path, file_name):
    """Save a df to parquet and Upload to S3

    :param df: DataFrame
    :param base_path: root path in local for saving
    :param file_path: file path after root for saving
    :param file_name: file name
    :return: 
    """
    os.makedirs(file_path, exist_ok=True)
    df.columns = df.columns.astype(str)
    file_dir = base_path + file_path + file_name
    df.to_parquet(file_dir)
    save_df_to_s3(file_dir, bucket_name, file_path + file_name)


'''
Handling Features

'''

def add_rooms(df):
    rooms = df['layout'].str.split('[ \,,+]',expand=True)[0]
    df['rooms'] = rooms.str.extract('(\d+)').astype(float)
    df['rooms'] = df['rooms'].fillna(1.0)
    return df


def transfer_land(df):
    own = df['land'].str.contains('own')
    df.loc[own,'land'] = 'own'

    rent = df['land'].str.contains('rent')
    df.loc[rent,'land'] = 'rent'

    df.loc[~rent & ~own,'land'] = 'others'
    return df


def add_floor_features(df):

    # house current floor, fill na with mode, most freq
    floor = df['floor'].str.split('/').str[0].replace("", "0").astype(float)
    df['floor_no'] = floor.fillna(floor.mode()[0])

    # total floors in the building, if null, fill with current floor
    top = df['floor'].str.split('/').str[-1].replace("", "0").astype(float)
    df['high'] = top.fillna(df['floor_no'])

    # if current > total floor, swap them
    wrong_floor = df['floor_no'] > df['high']
    should_be_floor_no = df.loc[wrong_floor, 'high']
    should_be_high = df.loc[wrong_floor, 'floor_no']

    df.loc[wrong_floor, 'high'] = should_be_high
    df.loc[wrong_floor, 'floor_no'] = should_be_floor_no

    # floor condition, top or ground
    df['is_top'] = (df['floor_no'] == df['high'])
    df['is_ground'] = (df['floor_no'] == 1)

    df['high_ratio'] =(df['floor_no'].astype(float)/df['high'].astype(float)).round(2)
    df['high_ratio'] = df['high_ratio'].fillna(0)

    return df


def feature_extract(df, city):

    df_fea= df.copy()

    # -------- ADD rooms float, 1st sec from layout, the first number -------- #
    df_fea = add_rooms(df_fea)

    #-------- ADD storage BOOL, from layout if vh -------- #
    df_fea['storage'] = df_fea['layout'].str.contains("vh")

    #-------- ADD sauna BOOL, from layout if s  -------- #
    df_fea['sauna'] = df_fea['layout'].str.contains(r'(?<![^\W_])s(?![^\W_])|(?<![^\W_])sauna(?![^\W_])', regex=True)

    #-------- ADD balcony BOOL, from layout if parveke  -------- #
    df_fea['balcony'] = df_fea['layout'].str.contains("parveke")

    #-------- UPDATE land,own rent others -------- #
    df_fea = transfer_land(df_fea)

    #-------- ADD floor fea, is_top and is_ground , if is the top floor from floor  -------- #
    df_fea = add_floor_features(df_fea)

    #-------- UPDATE electricity info, letter + year -------- #
    df_fea['elec_year'] = df_fea['elec'].str.extract('(\d+)', expand=False).astype(float)
    df_fea['elec_year'] = df_fea['elec_year'].fillna(df_fea['elec_year'].median())
    df_fea['elec_type'] = df_fea['elec'].str.extract('([a-zA-Z ]+)', expand=False)

    df_fea['city'] = city
   
    return df_fea


def get_type_column(df, data_type=object) -> list:

    re_cols = []
    for col in df.columns:
        if df[col].dtype == data_type:
            re_cols.append(col)

    print(len(re_cols), data_type, 'features in use', re_cols)
    return re_cols


def handling_null(df):
    """
        number : fill mode / fill median
        string: fill unknown
        bool: map 0,1
    """

    # Find all string columns
    object_columns = get_type_column(df, object)

    # fill all nan values with NONE
    # string cols fill nan, default convert to string 'unkown'
    df[object_columns] = df[object_columns].fillna('unkown')
    df[object_columns] = df[object_columns].replace("", "unkown") 
    
    d = {'yes': 1, 'no': 0}
    df['elevator'] = df['elevator'].map(d)

    return df
   

def main(city):

    PAGE_NOW = int(crawl_config["page_now"])
    TIME_STAMP = time.strftime("%Y-%m")
    FILE_PATH = file_config["base_path"]
    FILE_NAME = city + "_" + TIME_STAMP + '.parquet'
    RAW_FILE_PATH = file_config["raw_file_path"]
    CLEAN_FILE_PATH = file_config["clean_file_path"]
    FEA_FILE_PATH= file_config["fea_file_path"]
    
    # Crawling Data from Internet
    df_raw = get_city_df(city =city,p_now = PAGE_NOW,nxt_page = False)

    if df_raw.size != 0:
        # writing raw data to file
        save_df(df_raw, FILE_PATH, RAW_FILE_PATH, FILE_NAME)

        # Clean Raw Data
        df_clean = clean_raw_data(df_raw,city)
        save_df(df_clean,FILE_PATH, CLEAN_FILE_PATH, FILE_NAME)

        # Feature Extract
        df_fea = feature_extract(df_clean,city)
        df_fea = handling_null(df_fea)
        save_df(df_fea, FILE_PATH, FEA_FILE_PATH, FILE_NAME)


if __name__=="__main__":
    # Get Cities from configuration
    CITY_LIST = crawl_config["cities"]
    city_list = CITY_LIST.split(",")
    for city in city_list:
        main(city)
