from alpha_vantage.cryptocurrencies import CryptoCurrencies
import matplotlib.pyplot as plt


##########################################################
# Open an existing file and overwrite it. If it does not
# exist, create it.
##########################################################
def saveCSV(df, fileName):
    fd = open(fileName, 'w')
    df.to_csv(fd, index=True, encoding='utf-8', header=True)
    fd.close()

cc = CryptoCurrencies(key='1HAYLUHGCB6E0VXC', output_format='pandas')
data, meta_data = cc.get_digital_currency_intraday(symbol='ETH', market='USD', interval='1min')

data["CryptoCurrency"] = "ETH"

print(data.head())

saveCSV(data, "test.csv");