create table coin.ods_coinmarketcap_list_temp (like coin.ods_coinmarketcap_list);

copy coin.ods_coinmarketcap_list_temp from 
'%(root_dir)s\\%(scrape_date)s.csv'
;

Delete From coin.ods_coinmarketcap_list
using coin.ods_coinmarketcap_list_temp 
where coin.ods_coinmarketcap_list.scrape_date = coin.ods_coinmarketcap_list_temp.scrape_date;

Insert into coin.ods_coinmarketcap_list 
select * 
from coin.ods_coinmarketcap_list_temp;

drop table coin.ods_coinmarketcap_list_temp;



