from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import KFold
from matplotlib import pyplot as plt
from sklearn.datasets import load_iris
from sklearn.svm import SVC
import numpy as np

from sklearn.cross_validation import train_test_split
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import classification_report

from sklearn.cross_validation import cross_val_predict
import matplotlib.pyplot as plt

from sklearn import metrics

from sklearn.metrics import roc_curve, auc, roc_auc_score 
from sklearn.datasets import load_svmlight_file

data = load_svmlight_file("./MachineLearning/DS3.libsvm")

X = data[0]
y = data[1]
 
#Normalization
#Subtract the mean for each feature
X -= np.mean(X, axis=0)
#Divide each feature by its standard deviation
X /= np.std(X, axis=0)


estimator = LogisticRegression()
y_pred = cross_val_predict(estimator, X, y, cv=10)
print(classification_report(y, y_pred))
print (metrics.confusion_matrix(y, y_pred))
print (estimator.get_params())

print ("AUC score " , roc_auc_score(y, y_pred))
