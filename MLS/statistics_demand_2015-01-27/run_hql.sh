#!/bin/bash
#encoding:utf-8
# 测试用


todaydate=`date +%Y-%m-%d`

if [ $# = 1 ];then
    todaydate=`date +%Y-%m-%d`
fi

one_day_ago=`date -d "${todaydate} -1day" +%Y-%m-%d`
two_day_ago=`date -d "${todaydate} -2day" +%Y-%m-%d`
ten_day_ago=`date -d "${todaydate} -10day" +%Y-%m-%d`
ninety_day_ago=`date -d "${todaydate} -90day" +%Y-%m-%d`
hundred_day_ago=`date -d "${todaydate} -100day" +%Y-%m-%d`
four_day_ago=`date -d "${todaydate} -4day" +%Y-%m-%d`
echo "${one_day_ago}"

# 第一个表数据  评价
# 主动评价商品数
hive -S -e "select 'goods_initiative_comment' as id,count(*) as goods_initiative_comment from ods_bat_goods_comment where dt = '${one_day_ago}' and level != 0 and fashion != 0 ;"
# 全部评价商品数
hive -S -e "select 'goods_all_comment' as id,count(*) as goods_all_comment from ods_bat_goods_comment where dt = '${one_day_ago}'"
# 全部评价订单数
hive -S -e "select 'order_all_comment' as id,count(*) as order_all_comment from ods_bat_shop_order_comment where dt = '${one_day_ago}'"

# 全部评价中的好评数。level>=4
hive -S -e "select 'goods_favourable_comment' as id,count(*) as goods_favourable_comment from ods_bat_goods_comment where dt = '${one_day_ago}' and level >= 4 "
# 各个类目全部评价量
hive -S -e "select 'leimu_comment' as id,fisrt_cata,count(*) from (select goods_id,level from ods_bat_goods_comment where dt = '${one_day_ago}')t1 left outer join (select goods_id,fisrt_cata from dm.dm_order_4analyst where order_create_dt > '${one_day_ago}')t2 on t1.goods_id = t2.goods_id group by fisrt_cata limit 20;"
# 各个类目好评量
hive -S -e "select 'leimu_haoping_comment' as id,fisrt_cata,count(*) from (select goods_id,level from ods_bat_goods_comment where dt = '${one_day_ago}' and level >= 4)t1 left outer join (select goods_id,fisrt_cata from dm.dm_order_4analyst where order_create_dt > '${one_day_ago}')t2 on t1.goods_id = t2.goods_id group by fisrt_cata limit 20;"

# 全类目DSR
hive -S -e "
select 'DSR' as id,CASE mayjor 
WHEN cast(0 AS BIGINT) then '未设定'
WHEN cast(1 AS BIGINT) then '衣服'
WHEN cast(2 AS BIGINT) then '鞋子'
WHEN cast(3 AS BIGINT) then '包包'
WHEN cast(4 AS BIGINT) then '配饰'
WHEN cast(5 AS BIGINT) then '美妆'
WHEN cast(6 AS BIGINT) then '家居'
WHEN cast(7 AS BIGINT) then '综合'
ELSE '未知' END as major,avg(fast),avg(accord),avg(quality),avg(attitude),avg((fast+accord+quality+attitude)/4) from (select shop_id,quality,attitude,fast,accord from ods_bat_shop_order_comment where dt = '2015-01-26' and fast != 0 )t1 join (select shop_id,mayjor from ods_focus_shop_info )t2 on t1.shop_id = t2.shop_id group by mayjor; "


# 第二个表数据  退款
reason_list="('商品破损','商家错发或漏发','商品质量问题','未按约定时间发货','未按照约定时间发货','商品 损或无法使用','商品破损无法使用 ','商品与描述不符','描述不符','没货','缺货','没货了','无货','商家没货','卖家没货','断货')"

# 有理由退款 商品量
hive -S -e "select 'mount_youliyou' as id,sum(amount) from (select mid from ods_bat_order_refund where dt = '${one_day_ago}' and reason in ${reason_list} ) t1 join (select mid,amount from ods_bat_goods_map where dt = '${one_day_ago}') t2 on t1.mid = t2.mid limit 3;"

# 无理由退款 商品量
hive -S -e "select 'mount_wuliyou' as id,sum(amount) from (select mid from ods_bat_order_refund where dt = '${one_day_ago}' and reason not in ${reason_list} ) t1 join (select mid,amount from ods_bat_goods_map where dt = '${one_day_ago}') t2 on t1.mid = t2.mid limit 3;"

# 退款成功的，无论是哪天申请的，只看完成时间在昨天的

# 有理由退款 成功 商品量
hive -S -e "select 'mount_youliyou_success' as id,sum(amount) from (select mid from ods_bat_order_refund where dt >= '${ten_day_ago}' and refund_status = 41 and from_unixtime(finish_time) > '${one_day_ago} 00:00:00' and reason in ${reason_list} ) t1 join (select mid,amount from ods_bat_goods_map where dt >= '${ten_day_ago}') t2 on t1.mid = t2.mid limit 3;"

# 无理由退款 成功 商品量
hive -S -e "select 'mount_wuliyou_success' as id,sum(amount) from (select mid from ods_bat_order_refund where dt >= '${ten_day_ago}' and refund_status = 41 and from_unixtime(finish_time) > '${one_day_ago} 00:00:00' and reason not in ${reason_list} ) t1 join (select mid,amount from ods_bat_goods_map where dt >= '${ten_day_ago}') t2 on t1.mid = t2.mid limit 3;"

# 有理由 退款 申请金额
hive -S -e "select 'money_youliyou' as id,sum(refund_price_apply) from ods_bat_order_refund where dt = '${one_day_ago}' and reason in ${reason_list} limit 3;"
# 无理由 退款 申请金额
hive -S -e "select 'money_wuliyou' as id,sum(refund_price_apply) from ods_bat_order_refund where dt = '${one_day_ago}' and reason not in ${reason_list} limit 3;"
# 有理由 退款 成功 实际金额 如果有仲裁金额，以仲裁金额为准，否则，以申请金额为准
hive -S -e "select 'money_youliyou_success' as id,sum(if(refund_price_deal=0.0,refund_price_apply,refund_price_deal)) from ods_bat_order_refund where dt >= '${ten_day_ago}' and refund_status = 41 and from_unixtime(finish_time) > '${one_day_ago} 00:00:00' and reason in ${reason_list} limit 3;"
# 无理由 退款 成功 实际金额
hive -S -e "select 'money_wuliyou_success' as id,sum(if(refund_price_deal=0.0,refund_price_apply,refund_price_deal)) from ods_bat_order_refund where dt >= '${ten_day_ago}' and refund_status = 41 and from_unixtime(finish_time) > '${one_day_ago} 00:00:00' and reason not in ${reason_list} limit 3;"


# 90天内 有理由 成功 退款的商品总数
hive -S -e "select 'mount_youliyou_success_90day' as id,sum(amount) from (select mid from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}' and refund_status = 41  and reason in ${reason_list} ) t1 join (select mid,amount from ods_bat_goods_map where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}' ) t2 on t1.mid = t2.mid limit 3;"
# 90天内 无理由 成功
hive -S -e "select 'mount_wuliyou_success_90day' as id,sum(amount) from (select mid from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}'  and refund_status = 41  and reason not in ${reason_list} ) t1 join (select mid,amount from ods_bat_goods_map where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}' ) t2 on t1.mid = t2.mid limit 3;"
# 90天内成交商品数，ods_bat_order表 paytime > 90 
hive -S -e "select 'mount_all_order_90day' as id,sum(amount) from (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59')t1 join (select order_id,amount from ods_bat_goods_map where dt >= '${hundred_day_ago}')t2 on t1.order_id = t2.order_id "


# 90天 有理由 退款 成功 钱数
hive -S -e "select 'money_youliyou_success_90day' as id,sum( if(refund_price_deal=0.0,refund_price_apply,refund_price_deal) ) from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}'  and refund_status = 41 and reason in ${reason_list} limit 3;"
# 90天 无理由 成功 钱数
hive -S -e "select 'money_wuliyou_success_90day' as id,sum( if(refund_price_deal=0.0,refund_price_apply,refund_price_deal) ) from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}'  and refund_status = 41 and reason not in ${reason_list} limit 3;"
# 90天内成交的订单，成交金额，ods_bat_order表 paytime > 90 
hive -S -e "select 'money_all_order_90day' as id,sum(total_price) from ods_bat_order where dt >= '${hundred_day_ago}' and from_unixtime(pay_time) > '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59'"

# 第三个表 发货

# 72小时内未产生发货前退款订单数 退款时间大于发货时间 或者 无退款 但是必须是已经发货的
hive -S -e "select 'send_before_refund' as id,count(*) from (select send_time,apply_time from (select order_id,send_time from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${four_day_ago} 23:59:59' and from_unixtime(send_time) <= '${one_day_ago} 23:59:59' )t1 left outer join (select order_id,apply_time from ods_bat_order_refund where dt >='${four_day_ago}' and dt <= '${one_day_ago}')t2 on t1.order_id = t2.order_id) tt1 where tt1.send_time < tt1.apply_time and  from_unixtime(apply_time) < '${one_day_ago} 23:59:59'  or apply_time is NULL"
# 下单72小时内发货订单数
hive -S -e "select 'all_order_fahuo_xiayu_xiadan_72hour' as id,count(*) from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${four_day_ago} 23:59:59' and from_unixtime(send_time) <= '${one_day_ago} 23:59:59'"

# 物流时间大于成交时间并且在72小时之内的 * 可能有问题
hive -S -e "select 'wuliu_dayu_chengjiao_xiaoyu_72' as id,count(*) from (select pay_time,express_time from (select order_id,pay_time from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${four_day_ago} 23:59:59' )t1 join (select order_id,min(express_time) as express_time from dw.dw_order_express_detail where order_create_dt >='${ten_day_ago}' and order_create_dt <= '${one_day_ago}' group by order_id)t2 on t1.order_id = t2.order_id)tt1 where express_time > from_unixtime(pay_time) and express_time < '${one_day_ago} 23:59:59'"

# 四天前的订单 发货减去成交时间的平均值，单位是毫秒 测试的时候，结果是平均331115.218349397757 毫秒，需要确认一下准确性
hive -S -e "select 'avg_sendtime_paytime' as id, avg( (send_time-pay_time) ) from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${four_day_ago} 23:59:59' and send_time != 0  limit 10"

# 四天前的订单 物流时间减去成交时间的平均值，单位是毫秒 测试的时候结果是 68656.75362340015 毫秒
hive -S -e "select 'avg_expresstime_paytime' as id ,avg( unix_timestamp(express_time) - pay_time) from (select pay_time,express_time from (select order_id,pay_time from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${four_day_ago} 23:59:59' )t1 join (select order_id,min(express_time) as express_time from dw.dw_order_express_detail where order_create_dt >='${ten_day_ago}' and order_create_dt <= '${one_day_ago}' group by order_id)t2 on t1.order_id = t2.order_id)tt1 where express_time > from_unixtime(pay_time) "

# 两天前成交的订单，没有物流信息
hive -S -e "select 'wu_wuliu' as id ,count(*) from (select t1.order_id,express_time from (select order_id from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(send_time) >= '${two_day_ago} 00:00:00' and from_unixtime(send_time) <= '${two_day_ago} 23:59:59' )t1 left outer join (select order_id,min(express_time) as express_time from dw.dw_order_express_detail where order_create_dt >='${ten_day_ago}' and order_create_dt <= '${one_day_ago}' group by order_id)t2 on t1.order_id = t2.order_id) tt1 where tt1.express_time is NULL"

# 两天前成交的订单，有物流信息，但是物流信息小于成交时间
hive -S -e "select 'wuliu_zaoyu_chengjiao' as id ,count(*) from (select pay_time,express_time from (select order_id,pay_time from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(send_time) >= '${two_day_ago} 00:00:00' and from_unixtime(send_time) <= '${two_day_ago} 23:59:59' )t1 left outer join (select order_id,min(express_time) as express_time from dw.dw_order_express_detail where order_create_dt >='${ten_day_ago}' and order_create_dt <= '${one_day_ago}' group by order_id)t2 on t1.order_id = t2.order_id)tt1 where express_time is not NULL and express_time < from_unixtime(pay_time) "



# if [ $? != 0 ];then
#     echo "wrong"
# fi

