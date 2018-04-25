
import numpy as np
import random as random


class OneAction():
    def __init__(self,  action, targetState, reward) :
        self.action = action
        self.targetState = targetState
        self.reward = reward;

class OneRule() :
    def __init__(self, fromState, oneAction) :
        self.fromState = fromState;
        self.oneAction = oneAction;

class RuleCollection():
    def __init__(self, fromState) :
        self.fromState = fromState;
        self.rules = [];

    def addRule(self, oneAction):
        oneRule = OneRule(self.fromState, oneAction)
        self.rules.append(oneRule);

    def getRules(self) :
        return self.rules;

    def getState(self) :
        return self.fromState;

    def findRule(self,targetState):
        for l in range(len(self.rules)) :
            aRule = self.rules[l];
            if (aRule.oneAction.targetState == targetState) :
                return aRule;

        print("Unknown state", targetState);
        return "";

def CalculateValueOfState(allRules, policy) :

    totalEqn = 0;
    linearEqn = {};
    revLinearEqn = {};
    for ruleNo in range(len(allRules)) :
        ruleCollection = allRules[ruleNo]
        fromState = ruleCollection.getState()
        linearEqn [fromState] = totalEqn;
        revLinearEqn[totalEqn] = fromState;
        totalEqn += 1;
       
    arrayOfEqn = [[0 for col in range(totalEqn)] for row in range(totalEqn)]
    result = [0 for col in range(totalEqn)]

    for ruleNo in range(len(allRules)) :
        ruleCollection = allRules[ruleNo]
        fromState = ruleCollection.getState()
        targetAction, targetState = policy[fromState];
        targetRule = ruleCollection.findRule(targetState);
        
        fromStateId = linearEqn[fromState];
        targetStateId = linearEqn[targetState];
        arrayOfEqn[fromStateId][fromStateId] = 1;
        arrayOfEqn[fromStateId][targetStateId] = -0.9;
        result[fromStateId] = targetRule.oneAction.reward;

#    print (arrayOfEqn, result);
    a = np.array(arrayOfEqn);
    b = np.array(result);
    x = np.linalg.solve(a, b);


    stateValue = {};
    for ruleNo in range(len(x)) :
        stateName = revLinearEqn[ruleNo];
        point = x[ruleNo];
        stateValue[stateName] = point;

    return stateValue;

def ValueIterationByPolicy(allRules,policy , stopAt = 0.01, initStateValue = {}) :

#    policy = {};
    stateValues = {};
    actionOfStates = {};
#init the state as 0
    for k in range(len(allRules)) :
        ruleCollection = allRules[k];
        fromState = ruleCollection.getState()
        if ( fromState in initStateValue) :
            stateValues[fromState] = initStateValue[fromState]
        else    :
            stateValues[fromState] = 0;

    for itr in range(1000) :
        diffInValue = 0;
        for ruleNo in range(len(allRules)) :
            ruleCollection = allRules[ruleNo]
            fromState = ruleCollection.getState()
            ruleOfState = ruleCollection.getRules();
            valueOfState = -999999999;
            targetState = policy[fromState][1];
            targetRule = ruleCollection.findRule(targetState);
            singleAction = targetRule.oneAction;
            valueOfState = singleAction.reward + 0.9 * stateValues[singleAction.targetState]

            diffInValue = max(diffInValue, abs(stateValues[fromState]- valueOfState))
            stateValues[fromState] = valueOfState;

        print("iteration", itr, stateValues);
        if (diffInValue < stopAt) :
            return stateValues;


def FindBetterPolicy(allRules, policy, stateValues) :

    haveUpdate = False;
    newPolicy = {};

#    stateValues = CalculateValueOfState(allRules, policy);
    for ruleNo in range(len(allRules)) :
        ruleCollection = allRules[ruleNo]
        fromState = ruleCollection.getState()
        ruleOfState = ruleCollection.getRules();
        valueOfState = stateValues[fromState];
# do we have a better rule?
        newPolicy[fromState] = policy[fromState]


        haveBetterRule = False;
        for k in range(len(ruleOfState)):
            singleAction = ruleOfState[k].oneAction;
            valueOfRule = singleAction.reward + 0.9 * stateValues[singleAction.targetState]
            if (valueOfRule > valueOfState + 0.01) :
                haveUpdate = True;
                haveBetterRule = True;
                betterAction = singleAction;
                print ("Replace action " , fromState , " to take action (", singleAction.action, ",", singleAction.targetState, ") because better rule" , valueOfRule , ">", valueOfState);
                valueOfState = valueOfRule;
                newPolicy[fromState] = ( singleAction.action, singleAction.targetState );

    return (haveUpdate, newPolicy);

def PolicyIteration(allRules, policy) :

    stateValues = CalculateValueOfState(allRules, policy); 
    for k in range(100) :
        print ("Round ", k)
        haveUpdate, policy = FindBetterPolicy(allRules, policy, stateValues);
        if (not haveUpdate) :

            print("** no change **");
            print("Final policy", policy);
            print ("Final marks", CalculateValueOfState(allRules, policy)); 
            return True
        
        print("** value iteration **");
        stateValues = CalculateValueOfState(allRules, policy); 
        print(stateValues);
#        stateValues = ValueIterationByPolicy(allRules,policy,  0.01, stateValues);    

def ValueIteration(allRules,stopAt = 0.01, initStateValue = {}) :

    policy = {};
    stateValues = {};
    actionOfStates = {};
#init the state as 0
    for k in range(len(allRules)) :
        ruleCollection = allRules[k];
        fromState = ruleCollection.getState()
        if ( fromState in initStateValue) :
            stateValues[fromState] = initStateValue[fromState]
        else    :
            stateValues[fromState] = 0;

    for itr in range(1000) :
        diffInValue = 0;
        for ruleNo in range(len(allRules)) :
            ruleCollection = allRules[ruleNo]
            fromState = ruleCollection.getState()
            ruleOfState = ruleCollection.getRules();
            valueOfState = -999999999;

            for k in range(len(ruleOfState)):
                singleAction = ruleOfState[k].oneAction;
                valueOfRule = singleAction.reward + 0.9 * stateValues[singleAction.targetState]
                if (valueOfRule > valueOfState) :
                    valueOfState = valueOfRule;
                    bestAction = singleAction.action;
                    bestTarget = singleAction.targetState;

            policy[fromState] = (bestAction, bestTarget);
            diffInValue = max(diffInValue, abs(stateValues[fromState]- valueOfState))
            stateValues[fromState] = valueOfState;

        print("iteration", itr, ". MaxDiff" , diffInValue);
        print(policy);
        print(stateValues);
        print("------");
        if (diffInValue < stopAt) :
            return stateValues;

#alpha for non-deterministic case, 1= completely replace
def QLearning(allRules, alpha = 1) :
    policy = {};
    stateValues = {};
    actionOfStates = {};
    stateNumber = {}
#init the state as 0
    for k in range(len(allRules)) :
        ruleCollection = allRules[k];
        fromState = ruleCollection.getState()
        stateValues[fromState] = 0;
        stateNumber[fromState] = k;

#initialize QTable
    QTable = {};
    for f in range(len(allRules)) :
        ruleCollection = allRules[f];
        fromState = ruleCollection.getState()
        ruleOfState = ruleCollection.getRules();

        for k in range(len(ruleOfState)):
            singleAction = ruleOfState[k].oneAction;
            QTable[(fromState, singleAction.action, singleAction.targetState)] = 0
#initialize QTable



    stateNo = 0;
    ruleCollection = allRules[stateNo]

    for itr in range(10000) :

        fromState = ruleCollection.getState()
        ruleOfState = ruleCollection.getRules();
        valueOfState = -999999999;

        takeRuleNo = random.randint(0, len(ruleOfState)-1);

        singleAction = ruleOfState[takeRuleNo].oneAction;
        targetState = singleAction.targetState;
        reward = singleAction.reward;
        targetStateNo = stateNumber[targetState];
        targetRuleCollection = allRules[targetStateNo]   

        targetRuleOfState = targetRuleCollection.getRules();

        maxQValue = -99999999;
        for k in range(len(targetRuleOfState)) :

            targetAction = targetRuleOfState[k].oneAction;
            valueOfRule = QTable[(targetState, targetAction.action, targetAction.targetState)];
            if (valueOfRule > maxQValue) :
                maxQValue = valueOfRule

        QTable[(fromState, singleAction.action, targetState)] = (1-alpha) * QTable[(fromState, singleAction.action, targetState)]  + (alpha) * (reward + 0.9 * maxQValue);
        ruleCollection = targetRuleCollection

    print(QTable);
    return QTable;


def main() :

    allRules = [];
 
    s1Rule = RuleCollection("S1");
    s1Rule.addRule( OneAction("right", "S2", 0))
    s1Rule.addRule( OneAction("down", "S4", 10))
    allRules.append(s1Rule);

    s2Rule = RuleCollection("S2");
    s2Rule.addRule( OneAction("left", "S1", 0))
    s2Rule.addRule( OneAction("down", "S3", 5))
    allRules.append(s2Rule);

    s3Rule = RuleCollection("S3");
    s3Rule.addRule( OneAction("up", "S2", 0))
    s3Rule.addRule( OneAction("left", "S4", 1))
    allRules.append(s3Rule);

    s4Rule = RuleCollection("S4");
    s4Rule.addRule( OneAction("up", "S1", 0))
    s4Rule.addRule( OneAction("right", "S3", 1))
    allRules.append(s4Rule);


    policy = {};
    policy["S1"] = ( "right", "S2");
    policy["S2"] = ( "down", "S3");
    policy["S3"] = ( "up", "S2");
    policy["S4"] = ( "right", "S3");

 
    print("--- Policy Iteration ---");
    PolicyIteration(allRules, policy);

    print("--- Value Iteration ---");
    ValueIteration(allRules,0.1);


#   print(CalculateValueOfState(allRules, policy)); 
    print("--- QLearning ---");
    QTable = QLearning(allRules,0.9);

    return True;
    
main();
