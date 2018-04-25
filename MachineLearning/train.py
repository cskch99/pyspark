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
 

def approx_model(X_train, X_test, y_train, y_test  ):
    # Set the parameters by cross-validation
    tuned_parameters = [{'kernel': ['rbf'] ,
                         'C': [1, 10, 20 ]},
                        {'kernel': ['linear'], 'C': [1, 10,20,30,40]}]


    scores = ['precision' ]
    for score in scores:
        print("# Tuning hyper-parameters for %s" % score)
        print()

        clf = GridSearchCV(SVC(C=1), tuned_parameters, cv=5, scoring=score)
        clf.fit(X_train, y_train)


#        sysInfo = sys.version_info.major
#        modelFileName = "./model" + str(sysInfo) + ".pkl";
#        with open(modelFileName, 'wb') as f:
#            pickle.dump(clf, f)

        print("Store parameter found on development set:") 

        print("Best parameters set found on development set:")
        print()
        print(clf.best_estimator_)
        print()
        print("Grid scores on development set:")
        print()
        for params, mean_score, scores in clf.grid_scores_:
            print("%0.3f (+/-%0.03f) for %r"
                  % (mean_score, scores.std() / 2, params))
        print()

        print("Detailed classification report:")
        print()
        print("The model is trained on the full development set.")
        print("The scores are computed on the full evaluation set.")
        print()
        y_true, y_pred = y_test, clf.predict(X_test)
        print(classification_report(y_true, y_pred))

        print(metrics.confusion_matrix(y_true, y_pred))

        print ("AUC score " , roc_auc_score(y_true, y_pred))
        print()

 
 


def build_model(X_train,  y_train ):
    # Set the parameters by cross-validation
    tuned_parameters = [{'kernel': ['rbf'] ,
                         'C': [20 ]},
                        {'kernel': ['linear'], 'C': [1, 10,20,30,40]}]


    score = 'precision' 

    print("# Tuning hyper-parameters for %s" % score)
    print()

    clf = GridSearchCV(SVC(C=1), tuned_parameters, cv=5, scoring=score)
    clf.fit(X_train, y_train)


    sysInfo = sys.version_info.major
    modelFileName = "./model" + str(sysInfo) + ".pkl";
    with open(modelFileName, 'wb') as f:
        pickle.dump(clf, f)

    print("Store parameter found on development set:") 

    print("Best parameters set found on development set:")
    print()
    print(clf.best_estimator_)
    print()
    print("Grid scores on development set:")
    print()
    for params, mean_score, scores in clf.grid_scores_:
        print("%0.3f (+/-%0.03f) for %r"
                % (mean_score, scores.std() / 2, params))
    print()
 
 

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
        print ("train.py {fileName}");
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

 
    print ("* This will train the model and store the model data for python version  ", sys.version_info.major)
    print (X.shape)
     
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=1)


    approx_model(X_train, X_test, y_train, y_test )

    build_model(X, y)

if __name__ == "__main__":
   main(sys.argv[1:])