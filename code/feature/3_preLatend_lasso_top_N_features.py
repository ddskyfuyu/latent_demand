#!/usr/env/bin python
#coding:utf-8
import sys
import time
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_hastie_10_2
from sklearn.cross_validation import train_test_split
from sklearn import linear_model

def readXY(file1,file2,dimensions):
    for index in range(len(dimensions)):
        dimensions[index] += 1
    dimensions = set(dimensions)

    f1 = open(file1)
    x_train = []
    y_train = []
    for line in f1:
        line = line.strip()
        items = line.split(' ')
        if len(items) != 1952:
            continue

        label = int(items[0])
        y_train.append(label)

        val = []
        for index in range(1,len(items)):
            if index in dimensions:
                val.append(int(items[index].split(':')[1]))
        x_train.append(val)
    f1.close()

    f2 = open(file2)
    x_test = []
    y_test = []
    for line in f2:
        line = line.strip()
        items = line.split(' ')
        if len(items) != 1952:
            continue

        label = int(items[0])
        y_test.append(label)

        val = []
        for index in range(1,len(items)):
            if index in dimensions:
                val.append(int(items[index].split(':')[1]))
        x_test.append(val)
    f2.close()

    return x_train,y_train,x_test,y_test

def guiyihua(x_train,x_test):
    for row in range(len(x_train)):
        for col in range(len(x_train[row])):
            max_val = max(x_train[row])
            min_val = min(x_train[row])

            x_train[row][col] = (x_train[row][col] - min_val) * 1.0 / (max_val - min_val)

    for row in range(len(x_test)):
        for col in range(len(x_test[row])):
            max_val = max(x_test[row])
            min_val = min(x_test[row])

            x_test[row][col] = (x_test[row][col] - min_val) * 1.0 / (max_val - min_val)
    return x_train,x_test

def gbdt(x_train,x_test,y_train,y_test):
    #fit estimator
    est = GradientBoostingClassifier(n_estimators=10,max_depth=3)
    #est = RandomForestClassifier(n_estimators=10,max_depth=3)
    est.fit(x_train,y_train)

    #predict class labels
    pred = est.predict(x_test)

    acc = est.score(x_test,y_test)
    
    #score on test data
    #错误率
    tp = 0; fp = 0; tn = 0; fn = 0
    cor = 0
    for i in range(pred.shape[0]):
        if pred[i] == y_test[i]:
            cor += 1
            if y_test[i] == 0:
                tn += 1
            else:
                tp += 1
        else:
            if y_test[i] == 0:
                fn += 1
            else:
                fp += 1
    
    #正确率
    Precision = tp * 1.0 / (tp + fp)
    Recall = tp * 1.0 / (tp + fn)
    #F_score = 2 * Precision * Recall / (Precision + Recall)
    #print 'acc: %.4f' % acc
    print 'tp: ' + str(tp)
    print 'fp: ' + str(fp)
    print 'tn: ' + str(tn)
    print 'fn: ' + str(fn)
    print '################'
    #print 'Precision: %.4f' % Precision
    #print 'Recall: %.4f' % Recall
    #print 'F-score: %.4f' % F_score
    #print 'feature_importances: ' + str(est.feature_importances_)
    
    return acc
def lr(x_train,x_test,y_train,y_test):
    fn = 0; fp = 0; tp = 0; tn = 0
    regr = linear_model.LogisticRegression(C=1,penalty='l2')
    regr.fit(x_train,y_train)

    pred = regr.predict_proba(x_test)
    cor = 0
    for i in range(pred.shape[0]):
        if np.argmax(pred[i]) == y_test[i]:
            cor += 1
            if y_test[i] == 0:
                tn += 1
            else:
                tp += 1
        elif y_test[i] == 0:
            fn += 1
        elif y_test[i] == 1:
            fp += 1
    acc = cor * 1.0 / pred.shape[0]
    return acc
    #acc = regr.score(x_test,y_test)

def lasso(x_train,x_test,y_train,y_test):
    for i in range(1,11):
        regr = linear_model.Lasso(alpha=i*0.0001)
        regr.fit(x_train,y_train)
        
        features = regr.coef_
        acc = regr.score(x_test,y_test)
        pre_res = regr.predict(x_test)
        tp = sum([1 for index in range(len(y_test)) if y_test[index] == pre_res[index] and y_test[index] == 1])
        fp = sum([1 for index in range(len(y_test)) if y_test[index] != pre_res[index] and y_test[index] == 1])
        tn = sum([1 for index in range(len(y_test)) if y_test[index] == pre_res[index] and y_test[index] == 0])
        fn = sum([1 for index in range(len(y_test)) if y_test[index] != pre_res[index] and y_test[index] == 0])
        print 'tp: ' + str(tp)
        print 'fp: ' + str(fp)
        print 'tn: ' + str(tn)
        print 'fn: ' + str(fn)
        print 'lasso features: '
        for index in range(len(features)):
            if features[index] != 0:
                print '     ' + str(index) + ': ' + str(features[index])
        print 'lasso acc: %.4f' % acc

def process():
    #load data
    dimensions = [858,372,1219,671,57,882,872,240,374,795,828,607,625,674,867,146,1476,1247,31,1143,534,954,658,
                1485,314,106,280,919,177,348,591,669,646,683,194,640,586,865,891,287,471,63,32,1186,656,160,1157,191,107,
                957,492,952,781,1181,1814,1173,1007,1115,976,418,410,1411,1199,1671,144,1123,67,400,14,22,298,1576,836,462,476,
                1648,810,1819,537,600,660,1368,517,850,1358,1908,1198,1066,1796,489,1349,416,163,1527,1324,434,507,1183,165,
                1689,686,1401]
    #topN = int(sys.argv[1])
    #x_train,y_train,x_test,y_test = readXY('train.data','test.data',dimensions[:topN])
    #print len(x_train)
    #print len(y_train)

    
    #循环多次取平均值
    topN = 10
    while topN <= 100:
        x_train,y_train,x_test,y_test = readXY('train.data','test.data',dimensions[:topN])
        acc = 0
        iter_num = 1
        for i in range(iter_num):
            #x_train,x_test,y_train,y_test = train_test_split(x,y,test_size=0.2)

            #归一化处理
            #x_train,x_test = guiyihua(x_train,x_test)

            #选择算法，进行训练预测
            #GBDT算法
            print 'topN: ' + str(topN)
            acc += gbdt(x_train,x_test,y_train,y_test)

            #逻辑回归算法
            #acc += lr(x_train,x_test,y_train,y_test)

            #lasso算法
            #lasso(x_train,x_test,y_train,y_test)

        print 'gbdt acc: %.4f' % (acc * 1.0 / iter_num)
        topN += 10
    
if __name__ == '__main__':
    start = time.time()
    process()
    end = time.time()
    print 'It takes %f s.' % (end-start)
