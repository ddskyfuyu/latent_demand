#coding:utf-8
from time import ctime
import sys
from pandas import Series, DataFrame
import pandas as pd 
import numpy as np
import random

def process():
    fname = sys.argv[1]
    tpercent = float(sys.argv[2])
    df = pd.read_table(fname,sep=r' ')
    print df.index
    rand_list = random.sample(np.arange(len(df)),int(tpercent * len(df)))
    train_list = np.array(rand_list)
    test_list = np.setdiff1d(np.arange(len(df)),train_list)
    
    res = df[df.index.isin(train_list)]
    res.to_csv('res_train_'+fname.split('.')[0]+'.data',sep=' ', index=False)

    res = df[df.index.isin(test_list)]
    res.to_csv('res_test_'+fname.split('.')[0]+'.data',sep=' ', index=False)
    
if __name__ == '__main__':
    print 'Start at ' + ctime()
    process()
    print 'End at ' + ctime()