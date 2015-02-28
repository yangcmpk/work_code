#/bin/bash
. /etc/profile

cd ~/svn/trunk
. env.sh
cd -

the_date=`date +%Y-%m-%d`

if [ $# = 1 ];then
    the_date=`date -d $1 +%Y-%m-%d`
fi

if [ ! -e result ];then
    mkdir result
fi
if [ ! -e log ];then
    mkdir log
fi

# mysql中存的时间，用来进行查询 由于表中既有昨天的数据，又有两天前的，又有四天前的，又有九十天前的，所以存的时间是计算时间，也就是“今天”
# unix时间戳用作MySQL主键
date_id=`date -d "${the_date}" +%s`

tmp_date=`date +%s`
# 入MySQL之前存结果
# file_name="result_after_done_"${the_date}"_"${tmp_date}
file_name="result_after_done_"${the_date}


# bash run_hql.sh ${the_date} > result/result_${the_date}_${tmp_date} 2>log/error.log_${the_date}_${tmp_date}
bash run_hql.sh ${the_date} >> result/result_${the_date} 2>log/error.log_${the_date}_${tmp_date}

if [ -e result/result_${the_date} ];then
# 结果转化成 kv 模式，方便后续使用
    cat result/result_${the_date} | awk 'BEGIN {FS="\t";OFS="\t"} { if (NF==2) {print $1,$2} if(NF == 3) {print $1"+"$2,$3} if(NF==7) {print $1"+"$2,$3","$4","$5","$6","$7} if(NF==5) {print $1,$2","$3","$4","$5}}' > result/${file_name}
fi

if [ -e result/${file_name} ];then
    python put_resutl_2_mysql.py ${file_name} ${the_date} ${date_id}
fi


