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

def http_get(url, params):
    res = requests.get(url=URL, params= params).json()
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
        start += 100
        params.update({"start": start})
        totalcount = int(res.get('data').get('totalCount'))
        if start > totalcount or totalcount > 10000:
            break
    df = pd.DataFrame(data)
    data_path = os.path.join(root_dir, source_parms['source'],'data_source/%(job)_%(ts)s.csv')
    df.to_csv(data_path, index=False)


if __name__ == "__main__":
    print(0)
    # task_parms = {'type': 'python', 'py_path': 'scrape/coinmarketcap_list.py'}
    # source_parms = {'source': 'coinmarketcap', 'job': 'coinmarketcap_list', 'ts': int(time.time())}
    # scrape({}, source_parms, task_parms)
    # print(requests.get(url=URL, params= params).json())

