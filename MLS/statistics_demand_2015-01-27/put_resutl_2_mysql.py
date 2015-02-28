#!/usr/local/bin/python
#encoding:utf-8

import sys
import os
import time
import datetime
import string
from common.statutil import get_stat_db, get_config_db

'''
获取从hive中得到的结果，组织成MySQL语句插入到MySQL中

'''
# 评论字段的列表，使用这个的原因是，字典会自动排序，列的顺序不可控，用这个来进行确认。可以方便的控制输出
# all_data_list_comment = ['goods_initiative_comment','goods_all_comment','order_all_comment','goods_favourable_comment','leimu_comment+NULL','leimu_comment+女包','leimu_comment+女装','leimu_comment+女鞋','leimu_comment+家居','leimu_comment+美妆','leimu_comment+配饰','leimu_haoping_comment+NULL','leimu_haoping_comment+女包','leimu_haoping_comment+女装','leimu_haoping_comment+女鞋','leimu_haoping_comment+家居','leimu_haoping_comment+美妆','leimu_haoping_comment+配饰','DSR+未设定','DSR+衣服','DSR+鞋子','DSR+包包','DSR+配饰','DSR+美妆','DSR+家居']
all_data_list_comment = ['goods_initiative_comment','goods_all_comment','order_all_comment','goods_favourable_comment','leimu_comment+女包','leimu_comment+女装','leimu_comment+女鞋','leimu_comment+家居','leimu_comment+美妆','leimu_comment+配饰','leimu_haoping_comment+女包','leimu_haoping_comment+女装','leimu_haoping_comment+女鞋','leimu_haoping_comment+家居','leimu_haoping_comment+美妆','leimu_haoping_comment+配饰','DSR+衣服','DSR+鞋子','DSR+包包','DSR+配饰','DSR+美妆','DSR+家居','DSR+全站']
# 其中DSR 顺序 发货分、描述分、质量分、服务分、平均分。
#     avg(fast),avg(accord),avg(quality),avg(attitude),avg((fast+accord+quality+attitude)/4)
# 退货 的列表
all_data_list_refund = ['mount_youliyou','mount_wuliyou','mount_youliyou_success','mount_wuliyou_success','money_youliyou','money_wuliyou','money_youliyou_success','money_wuliyou_success','mount_youliyou_success_90day','mount_wuliyou_success_90day','mount_all_order_90day','money_youliyou_success_90day','money_wuliyou_success_90day','money_all_order_90day','mount_youliyou_fahuo','mount_youliyou_miaoshu','mount_youliyou_zhiliang','mount_youliyou_fahuo_success','mount_youliyou_miaoshu_success','mount_youliyou_zhiliang_success','money_youliyou_fahuo','money_youliyou_miaoshu','money_youliyou_zhiliang','money_youliyou_fahuo_success','money_youliyou_miaoshu_success','money_youliyou_zhiliang_success','mount_youliyou_fahuo_success_90day','mount_youliyou_miaoshu_success_90day','mount_youliyou_zhiliang_success_90day','money_youliyou_fahuo_success_90day','money_youliyou_miaoshu_success_90day','money_youliyou_zhiliang_success_90day']
# 发货 的列表 全表的表头，还有需要写入四天前和两天前的列表
# all_data_list_send = ['send_before_refund','all_order_fahuo_xiayu_xiadan_72hour','wuliu_dayu_chengjiao_xiaoyu_72','avg_sendtime_paytime','avg_expresstime_paytime','wu_wuliu','wuliu_zaoyu_chengjiao']
all_data_list_send_four_day_ago = ['send_before_refund','send_time']
# mysql的列名

# 存全部数据都的字典
all_data_dict = {}
# 日期经过转化成UNIXtime，作为主键
id = 0
# 时间，MySQL中查找使用 传的不是日志时间，而是真实时间，也就是日志时间的后一天。在脚本里计算2天前4天前这样的记录时间
insert_date = ''
one_day_ago_datetime = ''
two_day_ago_datetime = ''
four_day_ago_datetime = ''
# 结果文件名
file_name = ''

def init_data_dict ():
    # 用列表初始化字典，全部数据设置为 -1 ，这样结果里如果有 -1 ，证明之前的计算逻辑出了问题了
    for i in all_data_list_comment:
        all_data_dict[i] = "0"
    for i in all_data_list_refund:
        all_data_dict[i] = "0"
    for i in all_data_list_send_four_day_ago:
        all_data_dict[i] = "0"
    all_data_dict["DSR+鞋子"]="0,0,0,0,0"
    all_data_dict["DSR+衣服"]="0,0,0,0,0"
    all_data_dict["DSR+包包"]="0,0,0,0,0"
    all_data_dict["DSR+配饰"]="0,0,0,0,0"
    all_data_dict["DSR+美妆"]="0,0,0,0,0"
    all_data_dict["DSR+家居"]="0,0,0,0,0"
    all_data_dict["DSR+全站"]="0,0,0,0,0"
    all_data_dict["send_before_refund"]="0,0,0"
    all_data_dict["send_time"]="0,0,0,0"

def get_data_from_file():
    # 从文件中获取出全部数据，存入字典中
    path_file_name = "result/%s" %(file_name)
    f = open(path_file_name)
    for i in f.readlines():
        # print i.strip("\n").split("\t")[0]
        # print i.strip("\n").split("\t")[1]
        k,v = i.strip("\n").split("\t")
        if k in all_data_dict.keys():
            all_data_dict[k]=v
        else:
            print "key : " + k + " not in list"
    pass

# 插入的表名字、值、作为主键的时间戳、时间字段
def exec_stat_sql(table_name,all_values,true_insert_date):
    try:
        cur = get_config_db('dolphin_stat',1)
        true_id = int( time.mktime(time.strptime(true_insert_date,'%Y-%m-%d')) )
        delete_sql = "delete from %s where id = %s" % (table_name,true_id)
        insert_sql = "insert into %s () values(%s,'%s',%s)" % (table_name,true_id,true_insert_date,all_values)
        print delete_sql
        cur.execute(delete_sql)
        print insert_sql
        cur.execute(insert_sql)
    except Exception, e:
        print "execute ERROR, Exception ", e
        # time.sleep(10)

# 插入的表名字、值、作为主键的时间戳、时间字段
def exec_stat_sql_only_insert(table_name,all_values,true_insert_date):
    try:
        cur = get_config_db('dolphin_stat',1)
        true_id = int( time.mktime(time.strptime(true_insert_date,'%Y-%m-%d')) )
        insert_sql = "insert into %s () values(%s,'%s',%s)" % (table_name,true_id,true_insert_date,all_values)
        print insert_sql
        cur.execute(insert_sql)
    except Exception, e:
        print "execute ERROR, Exception ", e
        # time.sleep(10)

# 更新表的SQL。all_values 必须是key = value 这样的形式
def exec_stat_update_sql(table_name,all_values,true_insert_date):
    try:
        cur = get_config_db('dolphin_stat',1)
        true_id = int( time.mktime(time.strptime(true_insert_date,'%Y-%m-%d')) )
        update_sql = "update %s set %s where id = %s" % (table_name,all_values,true_id)
        print update_sql
        cur.execute(update_sql)
    except Exception, e:
        print "execute ERROR, Exception ", e
        # time.sleep(10)


def show_all_data():
    # 按三个表分别拼数据然后写入MySQL
    print "==============评价 table t_dolphin_stat_comment"
    all_keys=""
    all_values=""
    for i in all_data_list_comment:
        if (all_keys != ""):
            all_keys += ","
            all_values += ","
        all_keys += i
        all_values += all_data_dict[i]
        #  print i,all_data_dict[i]
    # print all_keys
    # print all_values
    exec_stat_sql('t_dolphin_stat_comment',all_values,one_day_ago_datetime)
    # print sql
    print "==============退款 table t_dolphin_stat_refund"
    all_keys=""
    all_values=""
    for i in all_data_list_refund:
        if (all_keys != ""):
            all_keys += ","
            all_values += ","
        all_keys += i
        all_values += all_data_dict[i]
        # print i,all_data_dict[i]
    # print all_keys
    # print all_values
    exec_stat_sql('t_dolphin_stat_refund',all_values,one_day_ago_datetime)

    # 先插入全是空的数据，然后update 如果原来就有数据，就会插入失败，直接更新就好了
    print "==============发货 table t_dolphin_stat_goods_delivery"
    all_keys=""
    all_values=""
    for i in all_data_list_send_four_day_ago:
        if (all_keys != ""):
            all_keys += ","
            all_values += ","
        all_keys += i
        all_values += all_data_dict[i]
        # print i,all_data_dict[i]
    # print all_keys
    # print all_values
    exec_stat_sql('t_dolphin_stat_goods_delivery',all_values,four_day_ago_datetime)


if __name__ == "__main__":

    file_name = sys.argv[1]
    insert_date = sys.argv[2]
    id = sys.argv[3]

    # print file_name
    # print insert_date
    # print id
    # print time.strptime(insert_date,'%Y-%m-%d').tm_mday + time.strptime(insert_date,'%Y-%m-%d').tm_mday 
    insert_datetime = datetime.datetime(string.atoi(insert_date.split("-")[0]),string.atoi(insert_date.split("-")[1]),string.atoi(insert_date.split("-")[2]))
    date_tmp = -1
    one_day_ago_datetime = ( insert_datetime + datetime.timedelta(days = date_tmp) ).strftime("%Y-%m-%d") 
    date_tmp = -2
    two_day_ago_datetime = ( insert_datetime + datetime.timedelta(days = date_tmp) ).strftime("%Y-%m-%d") 
    date_tmp = -4
    four_day_ago_datetime = ( insert_datetime + datetime.timedelta(days = date_tmp) ).strftime("%Y-%m-%d") 

    init_data_dict()
    get_data_from_file()
    show_all_data()

    '''
    结果转化成 kv 模式，方便后续使用
    cat result_2015-01-28 | awk 'BEGIN {FS="\t";OFS="\t"} { if (NF==2) {print $1,$2} if(NF == 3) {print $1"+"$2,$3} if(NF==7) {print $1"+"$2,$3","$4","$5","$6","$7}}'

    '''
