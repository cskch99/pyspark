from sklearn.datasets import load_iris
from sklearn.svm import SVC
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import classification_report
from sklearn.model_selection import cross_val_predict
from sklearn import metrics
from sklearn.metrics import roc_curve, auc, roc_auc_score 
from sklearn.datasets import load_svmlight_file

import pickle
import sys
  

def test_model(y, X):

    sysInfo = sys.version_info.major
    modelFileName = "./model" + str(sysInfo) + ".pkl";
    with open(modelFileName, 'rb') as f:
        clf = pickle.load( f)
    
    y_pred = clf.predict(X);
    print(classification_report(y, y_pred))

    print ("AUC score " , roc_auc_score(y, y_pred))


def reduceDimension(inputData) :
    reduceDim = []
    for r in range(len(inputData)) :
        data = np.convolve(inputData[r],[0.5,0.5], 'valid');
        reduceDim.append(data[::2]);
    return reduceDim;

def main(argv):

    if (len(argv) == 0) :
        print ("test.py {fileName}");
        return ;

    fileName = argv[0];
    data = load_svmlight_file(fileName);
#"./MachineLearning/DS3.libsvm")

    X = data[0]
    y = data[1]
 
    # reduce the dimension by average out every 2 item
    X = reduceDimension(X.toarray());
    # reduce the dimension by average out every 2 item

    #Normalization
    #Subtract the mean for each feature
    X -= np.mean(X, axis=0)
    #Divide each feature by its standard deviation
    X /= np.std(X, axis=0)

 
    test_model(y,X)

if __name__ == "__main__":
   main(sys.argv[1:])