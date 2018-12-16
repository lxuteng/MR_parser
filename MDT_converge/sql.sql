select 
round(avg(SC_RSRP),2) avg_RSRP
,max(SC_RSRP) max_RSRP
,min(SC_RSRP) min_RSRP
,count(ENB_CELLID) count_num
,ENB_CELLID
,CELL_NAME
from mdt
where 
lon between &1 and &2
and lat between &3 and &4
group by ENB_CELLID
order by count_num desc,avg_RSRP desc
limit 0,1
