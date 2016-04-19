#coding:utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def init():
    """
    初始化电商类目名称与索引号映射
    :return:
    """
    cate_dict = {}
    with open("comCateIndex.txt","r") as f:
        for line in f:
            line = line.strip()
            if line == '':
                continue
            line = line.split(',')
            cate = line[0]
            index = int(line[1])
            cate_dict[cate] = index
    return cate_dict

def categoryIndexMapping(cateDict):
    """
    将电商类目映射成索引号
    :param cateDict: 电商类目映射文件的字典
    :return:
    """
    for line in sys.stdin:
    #for line in open("./comCate.txt","r"):
        line = line.strip()
        if line == '':
            continue
        lineArray = line.split("#")
        if len(lineArray) < 3:
            continue
        cid = lineArray[0].strip()
        iid = lineArray[1].strip()
        categoryStr = lineArray[2].strip()
        categoryArray = categoryStr.split(",")
        length = len(categoryArray)
        category = "$".join(categoryArray[0 : length - 1])
        if category in cateDict:
            cateIndex = cateDict[category]
            print("#".join([cid,iid,str(cateIndex)]))


if __name__ == "__main__":
    cateDict = init()
    categoryIndexMapping(cateDict)