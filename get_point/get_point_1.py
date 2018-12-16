import csv
import sqlite3
import pandas
import os

path = r"D:\code\MRparse\MR_parser\get_point\全网道路经纬度信息.csv"
# conn = sqlite3.connect(r'E:\mr\MDT\mdt+\db.db',check_same_thread=False)
conn = sqlite3.connect(r':memory:',check_same_thread=False)

df = pandas.read_csv(path, encoding='gbk')
df.to_sql('mdt', conn, if_exists='append', index=False)
#
# path1 = r"E:\mr\MDT\cellname.csv"
# df1 = pandas.read_csv(path1, encoding='utf-8')
# df1.to_sql('cellname', conn, if_exists='append', index=False)


f = open(r"D:\code\MRparse\MR_parser\get_point\sql_DT_湘桥.sql", encoding='utf-8-sig')
sql_scr = f.read()
cu = conn.cursor()
cu.execute(sql_scr)
f = open(r'E:\mr\MDT\mdt+\DT_湘桥.csv','w', newline='')
f_csv = csv.writer(f)
f_csv.writerow('DAY,TIME,ECID,ENBID,ENB_CELLID,CELL_NAME,lon,lat,SC_RSRP,LteScRSRQ,LteScSinrUL,LteScTadv,SAMPLES,ue所在位置超出小区方向,ue方向角'.split(','))
f_csv.writerows(cu.fetchall())
