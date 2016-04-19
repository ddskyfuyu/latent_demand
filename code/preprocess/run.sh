#============================================================
#作者: baoquan.zhang
#邮箱: baoquan.zhang@baifendian.com
#脚本功能: 为潜在需求构建样本数据
#更新日期: 2016.4.18
#============================================================

#输入路径
inputDir=/user/bre/baoquan.zhang/latentData/reduceOut/part-r-*

#输出路径
outputDir=/user/bre/baoquan.zhang/latentData/label/output

#训练数据的月份
modelingMonth="201511"

#测试数据的月份
predictMonth="201512"

#测试的类目标号
predictCateIndex=807

#所有的样本数据的月份
allMonth="201511,201512"

#电商类目的个数
comCateAmount=977

#媒体类目的个数
mediaCateAmount=947

hadoop fs -rm -r $outputDir
hadoop jar latentDataLabel.jar $inputDir $outputDir $modelingMonth $predictMonth $predictCateIndex $allMonth $comCateAmount $mediaCateAmount
