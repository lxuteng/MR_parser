select 
mdt.DAY,mdt.TIME,mdt.ECID,mdt.ENBID,mdt.ENB_CELLID,cellname.CellName CELL_NAME,mdt.lon,mdt.lat,mdt.SC_RSRP,mdt.LteScRSRQ,mdt.LteScSinrUL,mdt.LteScTadv,mdt.SAMPLES
from mdt,cellname
where 
mdt.ENB_CELLID = cellname.CellId






