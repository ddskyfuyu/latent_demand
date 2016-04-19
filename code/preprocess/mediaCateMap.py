#coding:utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def init(commerceAmount):
    """
    初始化媒体类目名称与索引号映射
    commerceAmount:电商品类的个数，媒体品类在此基础上相加
    :return:
    """
    comAmount = int(commerceAmount)
    cate_dict = {}
    with open("mediaCateIndex.txt","r") as f:
        for line in f:
            line = line.strip()
            if line == '':
                continue
            line = line.split(',')
            cate = line[0]
            index = int(line[1]) + comAmount
            cate_dict[cate] = index
    return cate_dict

def categoryIndexMapping(cateDict):
    """
    将媒体类目名称与索引号进行映射
    :param cateDict: 媒体类别映射字典
    :return:
    """
    for line in sys.stdin:
    #for line in open("./mediaCate.txt","r"):
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
        for cate in categoryArray:
            cate = cate.strip()
            if cate:
                cateArray = cate.split("|")
                length = len(cateArray)
                if length > 1:
                    for elem in cateArray:
                        if elem in cateDict:
                            cateIndex = cateDict[elem]
                            print("#".join([cid,iid,str(cateIndex)]))
                else:
                    if cate in cateDict:
                        cateIndex = cateDict[cate]
                        print("#".join([cid,iid,str(cateIndex)]))


if __name__ == "__main__":
    #commerceAmount = 977
    commerceAmount = sys.argv[1]
    cateDict = init(commerceAmount)
    categoryIndexMapping(cateDict)