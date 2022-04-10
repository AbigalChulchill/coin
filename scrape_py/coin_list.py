import requests
import re
import datetime
import os
import csv
import json
import pandas as pd 

URL = 'https://coinmarketcap.com/'

def http_get(url):
    txt = requests.get(url).text
    try:
        data = re.findall(r'<script id="__NEXT_DATA__" type="application/json">(.*)</script>', txt)[0]
        return json.loads(data)
    except:
        print('抓取数据错误！')
        return {}


def parase(data):
    try:
        return data['props']['initialState']['cryptocurrency']['listingLatest']['data']
    except:
        print('解析数据错误！')
        return []

# def save(data, root_dir='C:\\workspace\\temp\\data'):
#     scrape_date = datetime.datetime.today().strftime('%Y-%m-%d')
#     file_path = os.path.join(root_dir, scrape_date+'.csv')
#     # print(file_path)
#     with open(file_path, "w", newline='') as f:
#         writer = csv.writer(f)
#         writer.writerows(data)

def save(res, root_dir='C:\\workspace\\temp\\data'):
    scrape_date = datetime.datetime.today().strftime('%Y-%m-%d')
    file_path = os.path.join(root_dir, scrape_date+'.csv')
    columnnames = res[0]['keysArr']+['auditInfoList']
    data = res[1:]
    df = pd.DataFrame(data, columns = columnnames)
    df['scrape_date']= scrape_date
    df.to_csv(file_path, index = False)


if __name__ == "__main__":
    json_data = http_get(URL)
    list_data = parase(json_data)
    save(list_data)




