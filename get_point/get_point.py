import csv
import sqlite3
import pandas
import os

path = r"D:\code\MRparse\MR_parser\get_point\road.csv"
# conn = sqlite3.connect(r'E:\mr\MDT\mdt+\db_grid.db',check_same_thread=False)
conn = sqlite3.connect(r':memory:',check_same_thread=False)
df = pandas.read_csv(path, encoding='utf-8')
df.to_sql('mdt', conn, if_exists='append', index=False)
#
# path1 = r"E:\mr\MDT\cellname.csv"
# df1 = pandas.read_csv(path1, encoding='utf-8')
# df1.to_sql('cellname', conn, if_exists='append', index=False)


f = open(r"D:\code\MRparse\MR_parser\get_point\sql_DT_潮安.sql", encoding='utf-8-sig')
sql_scr = f.read()
cu = conn.cursor()
cu.execute(sql_scr)
f = open(r'E:\mr\MDT\mdt+\DT_潮安.csv','w', newline='')
f_csv = csv.writer(f)
f_csv.writerows(cu.fetchall())
