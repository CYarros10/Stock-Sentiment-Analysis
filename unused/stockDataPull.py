import json
import requests
import cx_Oracle as co;
import pandas as pd;

connection = co.connect('CIODashboardUser/password@129.158.70.193:1521/PDB1.gse00013232.oraclecloud.internal');
cursor = connection.cursor();

symbol = 'MSFT'
interval = '1min'
dataType = 'csv'


##############################
# Grab data from alpha vantage
##############################

df = pd.read_csv('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='+symbol+'&interval='+interval+'&apikey=1HAYLUHGCB6E0VXC&datatype='+dataType)
df['stockticker'] = symbol


#######################
# Clean up timestamp
#######################

times = []
dates = []
for i in df['timestamp']:
    dates.append(i.split()[0])
    times.append(i.split()[1])
df['entrydate'] = dates
df['entrytime'] = times

df = df.sort_values(by=['entrydate','entrytime'])


###############################
# Insert results
###############################

#print(df.head())
rows = [tuple(x) for x in df.values]
print(rows[0])
cursor.executemany("insert /*+ ignore_row_on_dupkey_index(stockdata, stockdata_pk) */ into stockdata (timestamp, open,high,low,close,volume,stockticker,entrydate,entrytime) values (:1, :2, :3, :4, :5, :6, :7, :8, :9)", rows)
connection.commit()

#cursor.executemany( "merge into stockdata s",
#                    "using (select 1 :1 timestamp from dual ) sd",
#                    "on (sd.timestamp = s.timestamp)",
#                    "when not matched then insert(timestamp,open,high,low,close,volume,stockticker,entrydate,entrytime)",
#                    "values (:1, :2, :3, :4, :5, :6, :7, :8, :9)", rows)


##########################
# Save results to csv file
##########################
#fd = open('results/stockTickerResults.csv', 'w')
#df.to_csv(fd, index=True, encoding='utf-8', header=True)
#fd.close()
