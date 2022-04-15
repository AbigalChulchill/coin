import requests
import pandas as pd
from setting import root_dir
import os
import time
from _datetime import datetime

URL = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/chart?id=%s&range=1D'

HEADER =  {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0" ,
    "Accept": "application/json, text/plain, */*" ,
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2" ,
    "Accept-Encoding": "gzip, deflate, br" ,
    "Referer": "https://coinmarketcap.com/" ,
    "x-request-id": "c283332411764550ac3a760ef70e2ce8" ,
    "platform": "web" ,
    "Origin": "https://coinmarketcap.com" ,
    "Connection": "keep-alive" ,
    "Sec-Fetch-Dest": "empty" ,
    "Sec-Fetch-Mode": "cors" ,
    "Sec-Fetch-Site": "same-site",
    "Pragma": "no-cache" ,
    "Cache-Control": "no-cache" ,
    "TE": "trailers"
 }

def get_ids():
    csv_path = os.path.join(root_dir, 'coinmarketcap/temp/ids.csv')
    df = pd.read_csv(csv_path)
    return df['id'].tolist()

def http_get(url):
    return requests.get(url,headers=HEADER, timeout=50 ).json()

def parse(res, id):
    output = []
    for key, value in res.get('data').get('points').items():
        output.append({
            'id': id,
            'ts': key,
            'price': value['v'][0],
            'vol24h': value['v'][1]
        })
    return output

def scrape(func_parms,source_parms, task_parms):
    id_lst = get_ids()
    data = []
    for id in id_lst:
        url = URL%str(id)
        res = http_get(url)
        data += parse(res, id)
    df = pd.DataFrame(data)
    df['scrape_batch']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data_path = os.path.join(root_dir, source_parms['source'], 'data_source/%(job)s_%(ts)s.csv' % source_parms)
    df.to_csv(data_path, index=False)




if __name__ == "__main__":
    # print(get_ids())
    # func_parms = {}
    # source_parms = {'source': 'coinmarketcap', 'job': 'coinmarketcap_detail', 'ts': int(time.time())}
    # task_parms = {}
    # scrape(func_parms, source_parms, task_parms)
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
