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

from sklearn import tree
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
#from StringIO import StringIO



def reduceDimension(inputData) :
    reduceDim = []
    for r in range(len(inputData)) :
        data = np.convolve(inputData[r],[0.5,0.5], 'valid');
        reduceDim.append(data[::2]);
    return reduceDim;



data = load_svmlight_file("./TestData/DS3.libsvm")

X = data[0].toarray()
y = data[1]
labels = []

#Normalization
#Subtract the mean for each feature
#X -= np.mean(X, axis=0)
#X *= 100

#X = reduceDimension(X)

r = 0
m = len(X[0])
for k in range(20) :
    if (y[k] == 1) :
        plt.plot(X[k+r],'' )
    else    :
        plt.plot(X[k+r],'--'  )

    labels.append(r'$y = %i (%i)$' % (y[k+r], k+r))
 

plt.show()

