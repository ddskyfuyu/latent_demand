#--------start configuration----------------------
START_DATE="2015-11-01"
END_DATE="2015-12-31"
MONTH="201511,201512"
COM_CATE_AMOUNT=977
MEDIA_CATE_AMOUNT=947
OUTPUT_DIR=/user/bre/baoquan.zhang/latentData/reduceOut
#---------end configuration------------------------

LATENT_DATA_DIR=/user/bre/baoquan.zhang/latentData
#1、创建媒体全体类目名称的hive表
TABLE_MEDIA_A="mediaa"
#hive -e "CREATE TABLE IF NOT EXISTS $TABLE_MEDIA_A (cid string, iid string, category string) PARTITIONED BY(cate_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '#';"
#hive -e "insert overwrite table $TABLE_MEDIA_A partition(cate_type='3') select nb_cid, nb_iid, np_bfdcatveclist from news_from_hbase where np_bfdcatveclist !='empty';"
echo "------------table mediaa insert data OK !-------------"

#2、将mediaa表中类目名称，根据映射文件映射成索引类目号
MEDIAA_INPUT_DIR=/warehouse/$TABLE_MEDIA_A/cate_type=3/
MEDIAA_OUTPUT_DIR=$LATENT_DATA_DIR/$TABLE_MEDIA_A
#hadoop fs -rm -r $MEDIAA_OUTPUT_DIR
#$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar \
#   -D stream.non.zero.exit.is.failure=false \
#   -input $MEDIAA_INPUT_DIR \
#   -output $MEDIAA_OUTPUT_DIR \
#   -mapper "python mediaCateMap.py ${COM_CATE_AMOUNT}" \
#   -reducer "cat" \
#   -numReduceTasks 1000 \
#   -file mediaCateMap.py \
#   -file mediaCateIndex.txt
echo "------------talbe mediaa category mapping OK!----------------"

#将类目映射文件加载到hive表中，方便下一步两链接
TABLE_MEDIA_B="mediab"
#hive -e "CREATE TABLE IF NOT EXISTS $TABLE_MEDIA_B (cid string, iid string, cateindex string) PARTITIONED BY(cate_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '#';"
#hive -e "load data inpath '$LATENT_DATA_DIR/$TABLE_MEDIA_A/part-*' OVERWRITE INTO table $TABLE_MEDIA_B partition(cate_type='3');"
echo "----------- table mediab load data OK!------------------------"

#从用户行为表中提取需要的数据
TABLE_MEDIA_C="mediac"
hive -e "CREATE TABLE IF NOT EXISTS $TABLE_MEDIA_C (gid string, customer string, iid string, l_date string) PARTITIONED BY(cate_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '#';"
hive -e "insert overwrite table $TABLE_MEDIA_C partition(cate_type='3') select gid, customer, if(iid is NULL or iid='',obj_item_id,iid) as item_id, l_date from raw_kafka_input_dt1 where l_date between '$START_DATE' and '$END_DATE' and ((iid is not NULL and iid <> '') or (obj_item_id <> '' and obj_item_id is not NULL));"
echo "------------table mediac insert data OK! ----------------------mediac is user behave--------"

TABLE_MEDIA_D="mediad"
hive -e "CREATE TABLE IF NOT EXISTS $TABLE_MEDIA_D (gid string, cateindex string, l_date string) PARTITIONED BY(cate_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '#';"
hive -e "insert overwrite table $TABLE_MEDIA_D partition(cate_type='3') select gid, cateindex, l_date from mediab ,mediac where mediab.cid=mediac.customer and mediab.iid=mediac.iid;"
echo "-------------table mediad join OK! --------------------------"

#创建电商类目名称的hive表
TABLE_COM_A="coma"
#hive -e "CREATE TABLE IF NOT EXISTS $TABLE_COM_A (cid string, iid string, category string) PARTITIONED BY(cate_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '#';"
#hive -e "insert overwrite table $TABLE_COM_A partition(cate_type='3') select ip_cid, ip_iid, ip_categorynamenewlist from item_from_hbase;"
echo "---------------table coma insert data OK!------------------"

#将coma表中类目名称映射成类目号
COMA_INPUT_DIR=/warehouse/$TABLE_COM_A/cate_type=3/
COMA_OUTPUT_DIR=$LATENT_DATA_DIR/$TABLE_COM_A
#hadoop fs -rm -r $COMA_OUTPUT_DIR
#$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar \
#   -D stream.non.zero.exit.is.failure=false \
#   -input $COMA_INPUT_DIR \
#   -output $COMA_OUTPUT_DIR \
#   -mapper "python comCateMap.py" \
#   -reducer "cat" \
#   -numReduceTasks 500 \
#   -file comCateMap.py \
#   -file comCateIndex.txt
echo "---------------table coma category mapping OK!-----------------"

#将类目映射文件加载到hive表中，方便下一步两链接
TABLE_COM_B="comb"
#hive -e "CREATE TABLE IF NOT EXISTS $TABLE_COM_B (cid string, iid string, cateindex string) PARTITIONED BY(cate_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '#';"
#hive -e "load data inpath '$LATENT_DATA_DIR/$TABLE_COM_A/part-*' OVERWRITE INTO table $TABLE_COM_B partition(cate_type='3');"
echo "-----------------table comb load data OK!----------------------"

#电商用户行为表,与用户行为表mediac表链表
TABLE_COM_C="comc"
hive -e "CREATE TABLE IF NOT EXISTS $TABLE_COM_C (gid string, cateindex string, l_date string) PARTITIONED BY(cate_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '#';"
hive -e "insert overwrite table $TABLE_COM_C partition(cate_type='3') select gid, cateindex, l_date from mediac, comb where comb.cid=mediac.customer and comb.iid=mediac.iid;"
echo "------------------table comc join OK!--------------------------"

#生成电商媒体最终数据
hadoop fs -rm -r $OUTPUT_DIR
COM_INPUT_DIR=/warehouse/comc/cate_type=3/
MEDIA_INPUT_DIR=/warehouse/mediad/cate_type=3/
REDUCE_NUM=1000
hadoop jar latentDataPreparation.jar $COM_INPUT_DIR $MEDIA_INPUT_DIR $OUTPUT_DIR $MONTH $COM_CATE_AMOUNT $MEDIA_CATE_AMOUNT $REDUCE_NUM
echo "------------------final data prepare OK!------------------------"



