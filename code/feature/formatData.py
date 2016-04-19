#coding:utf-8
import sys

goal_cate = int(sys.argv[1])
num_thre = int(sys.argv[2])
up_thre = float(sys.argv[3])
down_thre = float(sys.argv[4])

for line in sys.stdin:
    line = line.strip()
    line = line.split('\t')

    if len(line) != 3:
        continue

    gid = line[0]
    item_12 = int(line[1])
    feature = line[2]

    num = 1
    feature = feature.split(' ')
    item_11 = int(feature[goal_cate - 1])
    retFeature = []
    for index in range(len(feature)):
        retFeature.append(str(num)+':'+feature[index])
        num += 1
    feature = ' '.join(retFeature)

    if item_11 < num_thre:
        if item_12 >= num_thre:
            label = 1
        else:
            label = 0
    else:
        if item_12 >= up_thre * item_11:
            label = 1
        elif item_12 <= down_thre * item_11:
            label = 1
        else:
            label = 0

    print str(label) + ' ' + feature
