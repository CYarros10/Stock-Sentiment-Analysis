import pandas as pd
import sklearn
from sklearn import model_selection
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
from sklearn.datasets import load_boston
from sklearn.linear_model import LinearRegression


boston = load_boston()

print(boston.feature_names)

# Load dataset
df = pd.read_csv('results/rawDataConsolidation.csv')
bos = pd.DataFrame(boston.data)
bos['PRICE'] = boston.target

X = bos.drop('PRICE', axis = 1)

#print(X)

# mse = mean squared error
#lm = LinearRegression()
#lm.fit(X[['PTRATIO']], bos.PRICE)
#mseFull = np.mean((bos.PRICE - lm.predict(X)) ** 2)
#msePTRATIO = np.mean((bos.PRICE - lm.predict(X[['PTRATIO']])) ** 2)
#print(mseFull)
#print(msePTRATIO)



X_train, X_test, Y_train, Y_test = sklearn.model_selection.train_test_split(X, bos.PRICE, test_size=0.33, random_state = 5)


lm = LinearRegression()
lm.fit(X_train, Y_train)
pred_train = lm.predict(X_train)
pred_test = lm.predict(X_test)

print("Fit a model X_train, and calculate MSE with Y_train:", np.mean((Y_train - lm.predict(X_train)) ** 2))
print("Fit a model X_train, and calculate MSE with X_test, Y_test:", np.mean((Y_test-lm.predict(X_test)) ** 2))
print(X_train)
print(Y_train)
print(pred_train)
print(pred_test)