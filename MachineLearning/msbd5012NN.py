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
from sklearn.neural_network import MLPClassifier


data = load_svmlight_file("./MachineLearning/DS3.libsvm")

X = data[0]
y = data[1]
 

#Normalization
#Subtract the mean for each feature
X -= np.mean(X, axis=0)
#Divide each feature by its standard deviation
X /= np.std(X, axis=0)

estimator = MLPClassifier(activation='relu', alpha=1e-05, batch_size='auto',
       beta_1=0.9, beta_2=0.999, early_stopping=False,
       epsilon=1e-08, hidden_layer_sizes=(25), learning_rate='constant',
       learning_rate_init=0.001, max_iter=200, momentum=0.9,
       nesterovs_momentum=True, power_t=0.5, random_state=1, shuffle=True,
       solver='lbfgs', tol=0.0001, validation_fraction=0.1, verbose=False,
       warm_start=False)

y_pred = cross_val_predict(estimator, X, y, cv=10)
print(classification_report(y, y_pred))
#print metrics.confusion_matrix(y, y_pred)

print ("AUC score " , roc_auc_score(y, y_pred))

# Split the dataset in two equal parts
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.4, random_state=1)



a = 0
if a == 0 :
    # Set the parameters by cross-validation
    tuned_parameters = [{'hidden_layer_sizes': [(10),(15),(20),(25)], 'epsilon': [1e-08, 1e-07,1e-06,1e-07]}]


    scores = ['precision' ,'precision']

    for score in scores:
        print("# Tuning hyper-parameters for %s" % score)
        print()

        clf = GridSearchCV(MLPClassifier(activation='relu', alpha=1e-05, batch_size='auto',
               beta_1=0.9, beta_2=0.999, early_stopping=False,
               epsilon=1e-08, hidden_layer_sizes=(10),  learning_rate='constant',
               learning_rate_init=0.001, max_iter=200, momentum=0.9,
               nesterovs_momentum=True, power_t=0.5, random_state=1, shuffle=True,
               solver='lbfgs', tol=0.0001, validation_fraction=0.1, verbose=False,
               warm_start=False), tuned_parameters, cv=5, scoring=score)
        clf.fit(X_train, y_train)

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
