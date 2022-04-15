create table coin.ods_coinmarketcap_list_%(ts)s (like coin.ods_coinmarketcap_list);

copy coin.ods_coinmarketcap_list_%(ts)s from
'%(root_dir)s\\%(source)s\\data_source\\%(job)s_%(ts)s.csv'
delimiter ',' csv header;
;

Delete From coin.ods_coinmarketcap_list
using coin.ods_coinmarketcap_list_%(ts)s
where coin.ods_coinmarketcap_list.scrape_date = coin.ods_coinmarketcap_list_%(ts)s.scrape_date;

Insert into coin.ods_coinmarketcap_list
select *
from coin.ods_coinmarketcap_list_%(ts)s;

drop table coin.ods_coinmarketcap_list_%(ts)s;