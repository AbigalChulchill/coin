create table coin.ods_coinmarketcap_detail_%(ts)s (like coin.ods_coinmarketcap_detail);

copy coin.ods_coinmarketcap_detail_%(ts)s from
'%(root_dir)s\\%(source)s\\data_source\\%(job)s_%(ts)s.csv'
delimiter ',' csv header;
;

Delete From coin.ods_coinmarketcap_detail
using coin.ods_coinmarketcap_detail_%(ts)s
where coin.ods_coinmarketcap_detail.scrape_batch = coin.ods_coinmarketcap_detail_%(ts)s.scrape_batch;

Insert into coin.ods_coinmarketcap_detail
select *
from coin.ods_coinmarketcap_detail_%(ts)s;

drop table coin.ods_coinmarketcap_detail_%(ts)s;