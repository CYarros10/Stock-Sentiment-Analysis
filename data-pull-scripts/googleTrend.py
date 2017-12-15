from pytrends.request import TrendReq
import pandas as pd
import cx_Oracle as co;

###############################
# Establish Oracle DB Connection
###############################
connection = co.connect('CIODashboardUser/password@129.158.70.193:1521/PDB1.gse00013232.oraclecloud.internal');
cursor = connection.cursor();


###############################
# Pull data from google API
###############################

# Login to Google. Only need to run this once, the rest of requests will use the same session.
pytrend = TrendReq()

# Create payload and capture API tokens. Only needed for interest_over_time(), interest_by_region() & related_queries()
pytrend.build_payload(kw_list=['$MSFT', 'MSFT', 'Microsoft Stock'], timeframe='now 1-H')

# Interest Over Time
interest_over_time_df = pytrend.interest_over_time()

df = pd.DataFrame(pd.DataFrame.from_dict(interest_over_time_df, orient='columns'))

fd = open('results/rawData.csv', 'w')
df.to_csv(fd, index=True, encoding='utf-8')
fd.close()


###############################
# Insert results into database
###############################


df = pd.read_csv('results/rawData.csv')


###############################
# Clean up timestamps
###############################
times = []
dates = []
for i in df['date']:
    dates.append(i.split()[0])
    times.append(i.split()[1])
df['entrydate'] = dates
df['entrytime'] = times
df.rename(columns={'date': 'timestamp'}, inplace=True)

df = df.sort_values(by=['entrydate','entrytime'])

df.drop('isPartial',axis=1, inplace=True)

###############################
# Insert results
###############################

print(df.head())
rows = [tuple(x) for x in df.values]
print(rows)
#cursor.executemany("insert /*+ ignore_row_on_dupkey_index(googletrenddata, googletrenddata_pk) */ into googletrenddata (timestamp,msft_tickervalue,msft_phrasevalue,microsoftstock_phrasevalue,entrydate,entrytime) values (:1, :2, :3, :4, :5, :6)", rows)
#connection.commit()