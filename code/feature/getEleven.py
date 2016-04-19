#coding:utf-8

import sys
#import numpy as np

def filtProcess(str1,str2,goal_cate):
    item_sum_thre = int(sys.argv[3])
    media_sum_thre = int(sys.argv[4])
    #array1 = np.array(map(int,str1.split(' ')))
    #array2 = np.array(map(int,str2.split(' ')))
    array1 = map(int,str1.split(' '))
    array2 = map(int,str2.split(' '))

    if sum(array1) < item_sum_thre and sum(array2) < media_sum_thre and array1[goal_cate - 1] == 0:
        return False
    else:
        return True

goal_cate = int(sys.argv[1])
filt = sys.argv[2]
if filt == 'filt':
    filt_flag = 1
else:
    filt_flag = 0

for line in sys.stdin:
    line = line.strip()
    if line == '':
        continue

    #line = np.array(line.split(','))
    #if line.size != 5:
    #    continue
    line = line.split(',')
    if len(line) != 5:
        continue

    gid = line[0]
    item_11 = line[1]
    item_12 = line[2]
    media_11 = line[3]
    media_12 = line[4]

    if filt_flag == 1:
        if filtProcess(item_11,media_11,goal_cate):
            print gid + '\t' + item_12.split(' ')[goal_cate - 1] + '\t' + item_11 + ' ' + media_11
    else:
        print gid + '\t' + item_12.split(' ')[goal_cate - 1] + '\t' + item_11 + ' ' + media_11