import requests
import pandas as pd
import datetime
from setting import root_dir
import os
import time


URL = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing'

params = {
    "start": 1,
    "limit":100,
    "sortBy":"market_cap",
    "sortType":"desc",
    "convert":"USD,BTC,ETH",
    "cryptoType":"all",
    "tagType":"all",
    "audited":"false",
    "aux":"ath,atl,high24h,low24h,num_market_pairs,cmc_rank,date_added,max_supply,circulating_supply,total_supply,volume_7d,volume_30d,self_reported_circulating_supply,self_reported_market_cap"
}


HEADER = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://coinmarketcap.com/",
    "x-request-id": "cfc289a2e6f34570a2ed543f20f0b99e",
    "platform": "web",
    "Origin": "https://coinmarketcap.com",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "TE": "trailers"
}

def http_get(url, params):
    res = requests.get(url=URL, params= params, headers=HEADER, timeout=30).json()
    return res

def parse(res):
    output = []
    clist = res['data']['cryptoCurrencyList']
    for row in clist:
        row_ = {
            "id": row.get("id"),
            "name": row.get("name"),
            "symbol": row.get("symbol"),
            "slug": row.get("slug"),
            "cmcRank": row.get("cmcRank"),
            "marketPairCount": row.get("marketPairCount"),
            "circulatingSupply": row.get("circulatingSupply"),
            "selfReportedCirculatingSupply": row.get("selfReportedCirculatingSupply"),
            "totalSupply": row.get("totalSupply"),
            "ath": row.get("ath"),
            "atl": row.get("atl"),
            "high24h": row.get("high24h"),
            "low24h": row.get("low24h"),
            "isActive": row.get("isActive"),
            "lastUpdated": row.get("lastUpdated"),
            "dateAdded": row.get("dateAdded"),
            "scrape_date": datetime.date.today().strftime('%Y-%m-%d')
        }
        output.append(row_)
    return output

def scrape(func_parms,source_parms, task_parms):
    data = []
    start = 1
    while True:
        res = http_get(url=URL, params= params)
        data += parse(res)
        print(start)
        start += 100
        params.update({"start": start})
        totalcount = int(res.get('data').get('totalCount'))
        if start > min(totalcount, 100):
            break
    df = pd.DataFrame(data)
    data_path = os.path.join(root_dir, source_parms['source'],'data_source/%(job)s_%(ts)s.csv'%source_parms)
    df.to_csv(data_path, index=False)
    df['id'].to_csv(os.path.join(root_dir, source_parms['source'], 'temp/ids.csv'), index=False)


if __name__ == "__main__":
    print(0)
    task_parms = {'type': 'python', 'py_path': 'scrape/coinmarketcap_list.py'}
    source_parms = {'source': 'coinmarketcap', 'job': 'coinmarketcap_list', 'ts': int(time.time())}
    scrape({}, source_parms, task_parms)
    # print(requests.get(url=URL, params= params).json())

