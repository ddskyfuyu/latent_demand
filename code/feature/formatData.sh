if [ $# -lt 5 ];then
    echo 'ERROR! Five parameters should be defined!'
    exit 1
fi
echo $1
echo $2
echo $3
echo $4
echo $5

goal_cate=$1
input_dir="/user/bre/guangming.li/latend/data/datasplit/source/part-00000"
output_dir=$2
num_thre=$3
up_thre=$4
down_thre=$5

hadoop fs -rm -r $output_dir
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar \
    -D stream.non.zero.exit.is.failure=false \
    -input $input_dir \
    -output $output_dir \
    -mapper "python formatData.py ${goal_cate} ${num_thre} ${up_thre} ${down_thre}" \
    -reducer "cat" \
    -file formatData.py 

