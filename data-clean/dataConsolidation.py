
import pandas as pd
import cx_Oracle as co


connection = co.connect('CIODashboardUser/password@129.158.70.193:1521/PDB1.gse00013232.oraclecloud.internal')
cursor = connection.cursor()

##########################################################
# Open an existing file and overwrite it. If it does not
# exist, create it.
##########################################################
def saveCSV(df, fileName):
    fd = open(fileName, 'w')
    df.to_csv(fd, index=True, encoding='utf-8', header=True)
    fd.close()

##########################################################
# Create an Oracle Database connection and insert the
# dataframe contents via the desired query
##########################################################
def insertToOracleDatabase(df, query):

    rows = [tuple(x) for x in df.values]
    cursor.executemany(query, rows)
    connection.commit()

def getSentimentScore(timeStamp, ticker):
    querystring = "SELECT * FROM (SELECT SENTIMENT FROM twitterdata WHERE TO_DATE(TIMESTAMP, 'YYYY-MM-DD,HH24:MI:SS') <= TO_DATE('"+str(timeStamp)+"', 'YYYY-MM-DD,HH24:MI:SS') and SEARCHVALUE= '"+str(ticker)+"' ORDER BY TIMESTAMP DESC) WHERE ROWNUM <= 5";
    cursor.execute(querystring);

    df_sentimentScore = pd.read_sql(querystring, con=connection);

    if df_sentimentScore.shape[0] > 0:
        return df_sentimentScore.sum()["SENTIMENT"];
    else:
        return 0


def getGoogleTrendScore(timeStamp, ticker):
    querystring = "SELECT * FROM (SELECT PHRASE_VALUE FROM GOOGLETRENDDATA WHERE TO_DATE (TIMESTAMP, 'YYYY-MM-DD,HH24:MI:SS') <= TO_DATE('"+str(timeStamp)+"', 'YYYY-MM-DD,HH24:MI:SS') AND PHRASE = '"+str(ticker)+"' ORDER BY timestamp DESC) WHERE ROWNUM <= 1";
    #print(querystring)
    cursor.execute(querystring);

    df_googleTrendScore = pd.read_sql(querystring, con=connection);

    if df_googleTrendScore.shape[0] > 0:
        return df_googleTrendScore.sum()["PHRASE_VALUE"];
    else:
        return 0





# retrieve app cost history
querystring = "SELECT * FROM STOCKDATA";
cursor.execute(querystring);
df = pd.read_sql(querystring, con=connection);


openPrices = []
closePrices = []
timeStamps = []
tickers = []
sentimentScores = []
googleTrendScores = []
actions = []

for index, row in df.iterrows():
    openPrice = row.OPEN
    highPrice = row.HIGH
    closePrice = row.CLOSE
    volume = row.VOLUME
    timeStamp = row.TIMESTAMP
    ticker = row.STOCKTICKER
    sentimentScore = getSentimentScore(str(timeStamp), str(ticker))
    googleTrendScore = getGoogleTrendScore(str(timeStamp), str(ticker))

    if (googleTrendScore > 0 and sentimentScore > 0):
        openPrices.append(openPrice)
        closePrices.append(closePrice)
        timeStamps.append(timeStamp)
        tickers.append(ticker)
        sentimentScores.append(sentimentScore)
        googleTrendScores.append(googleTrendScore)

        if (closePrice < openPrice):
            action = "SELL"
        elif (closePrice > openPrice):
            action = "BUY"
        else:
            action = "HOLD"
        actions.append(action)

        print(index)


df = pd.DataFrame(columns=['OPEN','SENTIMENT','GOOGLETRENDSCORE','ACTION','CLOSE','TIMESTAMP','TICKER'])

df["OPEN"] = openPrices
df["CLOSE"] = closePrices
df["SENTIMENT"] = sentimentScores
df["ACTION"] = actions
df["GOOGLETRENDSCORE"] = googleTrendScores
df["TIMESTAMP"] = timeStamps
df["TICKER"] = tickers


fd = open('results/rawDataConsolidation.csv', 'w')
df.to_csv(fd, index=True, encoding='utf-8')
fd.close()



#    if (googleTrendScore > 0 and sentimentScore > 0):
#        print(googleTrendScore, sentimentScore, ticker)
#        insertQuery = "INSERT /*+ ignore_row_on_dupkey_index(stocksentimentdata, stocksentimentdata_pk) */  INTO STOCKSENTIMENTDATA VALUES (:1,:2,:3,:4,:5,:6,:7)";
#        cursor.execute(insertQuery, (openPrice, highPrice, volume, sentimentScore, googleTrendScore, closePrice, ticker, timeStamp))
#        connection.commit()



connection.close()
cursor.close()
