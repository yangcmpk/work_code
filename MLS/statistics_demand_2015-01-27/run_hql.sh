#!/bin/bash
#encoding:utf-8
# 测试用


todaydate=`date +%Y-%m-%d`

if [ $# = 1 ];then
    todaydate=`date -d $1 +%Y-%m-%d`
fi

one_day_ago=`date -d "${todaydate} -1day" +%Y-%m-%d`
two_day_ago=`date -d "${todaydate} -2day" +%Y-%m-%d`
ten_day_ago=`date -d "${todaydate} -10day" +%Y-%m-%d`
ninety_day_ago=`date -d "${todaydate} -90day" +%Y-%m-%d`
hundred_day_ago=`date -d "${todaydate} -100day" +%Y-%m-%d`
four_day_ago=`date -d "${todaydate} -4day" +%Y-%m-%d`
# 72小时转化成毫秒
# millisecond_72hour=72*60*60*1000
millisecond_72hour=259200000
echo "${one_day_ago}"


# 第一个表数据  评价
# 主动评价商品数
hive -S -e "select 'goods_initiative_comment' as id,count(*) as goods_initiative_comment from ods_bat_goods_comment where dt = '${one_day_ago}' and level != 0 and quality != 0 and status=0 ;"
# 全部评价商品数
hive -S -e "select 'goods_all_comment' as id,count(*) as goods_all_comment from ods_bat_goods_comment where dt = '${one_day_ago}' and status=0"
# 全部评价订单数
hive -S -e "select 'order_all_comment' as id,count(*) as order_all_comment from ods_bat_shop_order_comment where dt = '${one_day_ago}'and fast!=0 and status=0"

# 全部评价中的好评数。level>=4
hive -S -e "select 'goods_favourable_comment' as id,count(*) as goods_favourable_comment from ods_bat_goods_comment where dt = '${one_day_ago}' and level >= 4 and status=0"
# 各个类目全部评价量
hive -S -e "select 'leimu_comment' as id,catalog1,count(*) from (select goods_id,level from ods_bat_goods_comment where dt = '${one_day_ago}' and status=0)t1 left outer join (select distinct goods_id,catalog1,catalog2,catalog3,goods_on_shelf,goods_img from (select distinct goods_id,goods_price,t2.goods_catalog,t2.catalog1,t2.catalog2,t2.catalog3,goods_on_shelf,goods_img from ods_brd_goods_info t1 left outer join dw.dw_goods_catalog_tree t2 on t1.sort_id = t2.goods_catalog ) a )t2 on t1.goods_id = t2.goods_id  group by catalog1 limit 20;"
# 各个类目好评量
hive -S -e "select 'leimu_haoping_comment' as id,catalog1,count(*) from (select goods_id,level from ods_bat_goods_comment where dt = '${one_day_ago}' and level >= 4 and status=0)t1 left outer join (select distinct goods_id,catalog1,catalog2,catalog3,goods_on_shelf,goods_img from (select distinct goods_id,goods_price,t2.goods_catalog,t2.catalog1,t2.catalog2,t2.catalog3,goods_on_shelf,goods_img from ods_brd_goods_info t1 left outer join dw.dw_goods_catalog_tree t2 on t1.sort_id = t2.goods_catalog ) a )t2 on t1.goods_id = t2.goods_id  group by catalog1 limit 20;"

# 全类目DSR
hive -S -e "
select *, (fast+ accord +quality+attitude)/4 from (select 'DSR' as id, '全站' as major, avg(fast) as fast ,avg(case when accord>0 then accord end) as accord,avg(quality) as quality,avg(attitude) as attitude from ods_bat_shop_order_comment where dt = '${one_day_ago}' and fast != 0 and status=0)t1; "
#分类目DSR
# ELSE '未知' END as major,fast,if(accord is not NULL,accord,0),quality,attitude,if(accord is not NULL,(fast+ accord +quality+attitude)/4,(fast+ quality+attitude)/3)
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
ELSE '未知' END as major,fast,accord,quality,attitude,(fast+ accord +quality+attitude)/4
    from
(
select mayjor,avg(fast) as fast ,avg(case when accord>0 then accord end) as accord,avg(quality) as quality,avg(attitude) as attitude from (select shop_id,quality,attitude,fast,accord from ods_bat_shop_order_comment where dt = '${one_day_ago}' and fast != 0 and status=0)t1 join (select shop_id,mayjor from ods_focus_shop_info )t2 on t1.shop_id = t2.shop_id group by mayjor) t2; "

# 第二个表数据  退款
reason_list="('商品破损','商家错发或漏发','商品质量问题','未按约定时间发货','未按照约定时间发货','商品 损或无法使用','商品破损无法使用 ','商品与描述不符','描述不符','没货','缺货','没货了','无货','商家没货','卖家没货','断货')"
#-------------------------------------------------------------------------------------------
# 有理由退款 商品量
# 风控豁免退款表有类型限制，type=1为有理由退款豁免，type=2为纠纷豁免
hive -S -e "select 'mount_youliyou' as id,sum(amount) from (select rid,mid from ods_bat_order_refund where dt = '${one_day_ago}' and select_reason_id < 200 and (select_reason_id%100) in (1,3,22,24,25,42,43,46,47,48,49) ) t1 join (select mid,amount from ods_bat_goods_map) t2 on t1.mid = t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid = 1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;"

##细分退款商品量
# 有理由退款 发货问题 商品量
hive -S -e "select 'mount_youliyou_fahuo' as id,sum(amount) from (select rid,mid from ods_bat_order_refund where dt = '${one_day_ago}' and select_reason_id < 200 and select_reason_id%100 in (1,3,22,24,42,46)) t1 join (select mid,amount from ods_bat_goods_map) t2 on t1.mid=t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid = 1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;" 

# 有理由退款 描述问题 商品量
hive -S -e "select 'mount_youliyou_miaoshu' as id,sum(amount) from (select rid,mid from ods_bat_order_refund where dt = '${one_day_ago}' and select_reason_id < 200 and select_reason_id%100 = 47) t1 join (select mid,amount from ods_bat_goods_map) t2 on t1.mid=t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid = 1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;" 

# 有理由退款 质量问题 商品量
hive -S -e "select 'mount_youliyou_zhiliang' as id,sum(amount) from (select rid,mid from ods_bat_order_refund where dt = '${one_day_ago}' and select_reason_id < 200 and select_reason_id%100 in (25,43,48,49)) t1 join (select mid,amount from ods_bat_goods_map) t2 on t1.mid=t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid=1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;" 
#-------------------------------------------------------------------------------------------

# 无理由退款 商品量
# 需要加上条件，“或 ods_risk_stat_health_order之中豁免的商品”
hive -S -e "select 'mount_wuliyou' as id, sum(amount) from (select rid, mid， select_reason_id from ods_bat_order_refund where dt = '${one_day_ago}') t1 join (select mid, amount from ods_bat_goods_map) t2 on t1.mid = t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid=1) t3 on t1.rid=t3.refund_id where select_reason_id >=200 or select_reason_id%100 not in (1, 3, 22, 24, 25, 42, 43, 46, 47, 48, 49) or t3.refund_id is not null limit 3;" 


# 退款成功的，无论是哪天申请的，只看完成时间在昨天的

#-------------------------------------------------------------------------------------------
# 有理由退款 成功 商品量
hive -S -e "select 'mount_youliyou_success' as id,sum(amount) from (select rid,mid from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00'  and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59' and select_reason_id < 200 and select_reason_id%100 in (1,3,22,24,25,42,43,46,47,48,49) ) t1 join (select mid,amount from ods_bat_goods_map) t2 on t1.mid = t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid = 1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;"

##细分退款成功商品量
# 有理由退款 发货问题 成功 商品量
hive -S -e "select 'mount_youliyou_fahuo_success' as id,sum(amount) from (select rid,mid from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00'  and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59' and select_reason_id < 200 and select_reason_id%100 in (1,3,22,24,42,46) ) t1 join (select mid,amount from ods_bat_goods_map) t2 on t1.mid = t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid = 1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;"

# 有理由退款 描述问题 成功 商品量
hive -S -e "select 'mount_youliyou_miaoshu_success' as id,sum(amount) from (select rid,mid from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00'  and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59' and select_reason_id < 200 and select_reason_id%100 = 47 ) t1 join (select mid,amount from ods_bat_goods_map) t2 on t1.mid = t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;"

# 有礼有退款 质量问题 成功 商品量
hive -S -e "select 'mount_youliyou_zhiliang_success' as id,sum(amount) from (select rid,mid from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00'  and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59' and select_reason_id < 200 and select_reason_id%100 in (25,43,48,49) ) t1 join (select mid,amount from ods_bat_goods_map) t2 on t1.mid = t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;"


#-------------------------------------------------------------------------------------------

# 无理由退款 成功 商品量
hive -S -e "select 'mount_wuliyou_success' as id, sum(amount) from (select rid, mid, select_reason_id from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00' and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59') t1 join (select mid, amount from ods_bat_goods_map) t2 on t1.mid = t2.mid left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid=1) t3 on t1.rid=t3.refund_id where select_reason_id >=200 or select_reason_id%100 not in (1, 3, 22, 24, 25, 42, 43, 46, 47, 48, 49) or t3.refund_id is not null limit 3;" 


#-------------------------------------------------------------------------------------------
# 有理由 退款 申请金额
hive -S -e "select 'money_youliyou' as id,sum(t1.refund_price_apply) from (select rid,refund_price_apply from ods_bat_order_refund where dt = '${one_day_ago}' and select_reason_id < 200 and select_reason_id%100 in (1,3,22,24,25,42,43,46,47,48,49)) t1 left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t2 on t1.rid=t2.refund_id where t2.refund_id is null limit 3;"

## 细分理由
# 有理由 发货问题 退款 申请金额
hive -S -e "select 'money_youliyou_fahuo' as id,sum(t1.refund_price_apply) from (select rid,refund_price_apply from ods_bat_order_refund where dt = '${one_day_ago}' and select_reason_id < 200 and select_reason_id%100 in (1,3,22,24,42,46)) t1 left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t2 on t1.rid=t2.refund_id where t2.refund_id is null limit 3;"

# 有理由 描述问题 退款 申请金额
hive -S -e "select 'money_youliyou_miaoshu' as id,sum(t1.refund_price_apply) from (select rid,refund_price_apply from ods_bat_order_refund where dt = '${one_day_ago}' and select_reason_id < 200 and select_reason_id%100 = 47 ) t1 left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t2 on t1.rid=t2.refund_id where t2.refund_id is null limit 3;"

# 有理由 质量问题 退款 申请金额
hive -S -e "select 'money_youliyou_zhiliang' as id,sum(t1.refund_price_apply) from (select rid,refund_price_apply from ods_bat_order_refund where dt = '${one_day_ago}' and select_reason_id < 200 and select_reason_id%100 in (25,43,48,49)) t1 left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t2 on t1.rid=t2.refund_id where t2.refund_id is null limit 3;"
#-------------------------------------------------------------------------------------------

# 无理由 退款 申请金额
hive -S -e "select 'money_wuliyou' as id, sum(refund_price_apply) from (select rid, select_reason_id, refund_price_apply from ods_bat_order_refund where dt = '${one_day_ago}') t1 left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid=1) t3 on t1.rid=t3.refund_id where select_reason_id >=200 or select_reason_id%100 not in (1, 3, 22, 24, 25, 42, 43, 46, 47, 48, 49) or t3.refund_id is not null limit 3;" 

#-------------------------------------------------------------------------------------------
# 有理由 退款 成功 实际金额 如果有仲裁金额，以仲裁金额为准，否则，以申请金额为准
hive -S -e "select 'money_youliyou_success' as id,sum(if(t1.refund_price_deal=0.0,t1.refund_price_apply,t1.refund_price_deal)) from (select rid,refund_price_apply,refund_price_deal from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00' and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59' and select_reason_id<200 and select_reason_id%100 in (1,3,22,24,25,42,43,46,47,48,49)) t1 left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t2 on t1.rid=t2.refund_id where t2.refund_id is null limit 3;"

## 细分理由
# 有理由 发货问题 退款 成功 实际金额
hive -S -e "select 'money_youliyou_fahuo_success' as id,sum(if(t1.refund_price_deal=0.0,t1.refund_price_apply,t1.refund_price_deal)) from (select rid,refund_price_apply,refund_price_deal from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00' and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59' and select_reason_id<200 and select_reason_id%100 in (1,3,22,24,42,46)) t1 left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t2 on t1.rid=t2.refund_id where t2.refund_id is null limit 3;"

# 有理由 描述问题 退款 成功 实际金额
hive -S -e "select 'money_youliyou_miaoshu_success' as id,sum(if(t1.refund_price_deal=0.0,t1.refund_price_apply,t1.refund_price_deal)) from (select rid,refund_price_apply,refund_price_deal from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00' and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59' and select_reason_id<200 and select_reason_id%100 = 47) t1 left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t2 on t1.rid=t2.refund_id where t2.refund_id is null limit 3;"

# 有理由 质量问题 退款 成功 实际金额
hive -S -e "select 'money_youliyou_zhiliang_success' as id,sum(if(t1.refund_price_deal=0.0,t1.refund_price_apply,t1.refund_price_deal)) from (select rid,refund_price_apply,refund_price_deal from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00' and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59' and select_reason_id<200 and select_reason_id%100 in (25,43,48,49)) t1 left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t2 on t1.rid=t2.refund_id where t2.refund_id is null limit 3;"


#-------------------------------------------------------------------------------------------

# 无理由 退款 成功 实际金额
hive -S -e "select 'money_wuliyou_success' as id, sum(if(refund_price_deal=0.0,refund_price_apply,refund_price_deal)) from (select rid, select_reason_id, refund_price_apply, refund_price_deal from ods_bat_order_refund where refund_status = 41 and from_unixtime(finish_time) >= '${one_day_ago} 00:00:00' and from_unixtime(finish_time) <= '${one_day_ago} 23:59:59') left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid=1) t3 on t1.rid=t3.refund_id where select_reason_id >=200 or select_reason_id%100 not in (1, 3, 22, 24, 25, 42, 43, 46, 47, 48, 49) or t3.refund_id is not null limit 3;" 

#-------------------------------------------------------------------------------------------
# 90天内 有理由 成功 退款的商品总数
hive -S -e "select 'mount_youliyou_success_90day' as id,sum(amount) from (select rid,order_id,mid from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}' and refund_status = 41  and select_reason_id<200 and select_reason_id in (1,3,22,24,25,42,43,46,47,48,49) ) t1 join (select mid,amount from ods_bat_goods_map where dt >= '${hundred_day_ago}' and dt <= '${one_day_ago}' ) t2 on t1.mid = t2.mid join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t3 on t1.order_id=t3.order_id left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t4 on t1.rid=t4.refund_id where t4.refund_id is null limit 3;"

## 细分理由
# 90天内 有理由 发货问题 成功 退款的商品总数
hive -S -e "select 'mount_youliyou_fahuo_success_90day' as id,sum(amount) from (select rid,order_id,mid from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}' and refund_status = 41  and select_reason_id<200 and select_reason_id in (1,3,22,24,42,46) ) t1 join (select mid,amount from ods_bat_goods_map where dt >= '${hundred_day_ago}' and dt <= '${one_day_ago}' ) t2 on t1.mid = t2.mid join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t3 on t1.order_id=t3.order_id left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t4 on t1.rid=t4.refund_id where t4.refund_id is null limit 3;"

# 90天内 有理由 描述问题 成功 退款的商品总数
hive -S -e "select 'mount_youliyou_miaoshu_success_90day' as id,sum(amount) from (select rid,order_id,mid from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}' and refund_status = 41  and select_reason_id<200 and select_reason_id =47 ) t1 join (select mid,amount from ods_bat_goods_map where dt >= '${hundred_day_ago}' and dt <= '${one_day_ago}' ) t2 on t1.mid = t2.mid join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t3 on t1.order_id=t3.order_id left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t4 on t1.rid=t4.refund_id where t4.refund_id is null limit 3;"

# 90天内 有理由 质量问题 成功 退款的商品总数
hive -S -e "select 'mount_youliyou_zhiliang_success_90day' as id,sum(amount) from (select rid,order_id,mid from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}' and refund_status = 41  and select_reason_id<200 and select_reason_id in (25,43,48,49) ) t1 join (select mid,amount from ods_bat_goods_map where dt >= '${hundred_day_ago}' and dt <= '${one_day_ago}' ) t2 on t1.mid = t2.mid join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t3 on t1.order_id=t3.order_id left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t4 on t1.rid=t4.refund_id where t4.refund_id is null limit 3;"


#-------------------------------------------------------------------------------------------

# 90天内 无理由 成功
hive -S -e "select 'mount_wuliyou_success_90day' as id, sum(amount) from (select rid, select_reason_id, order_id, mid from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}' and refund_status = 41) t1 join (select mid, amount from ods_bat_goods_map where dt >= '${hundred_day_ago}' and dt <= '${one_day_ago}') t2 on t1.mid = t2.mid join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t3 on t1.order_id=t3.order_id left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid=1) t4 on t1.rid=t4.refund_id where select_reason_id >=200 or select_reason_id%100 not in (1, 3, 22, 24, 25, 42, 43, 46, 47, 48, 49) or t4.refund_id is not null limit 3;" 
# 90天内成交商品数，ods_bat_order表 paytime > 90 
hive -S -e "select 'mount_all_order_90day' as id,sum(amount) from (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59')t1 join (select order_id,amount from ods_bat_goods_map where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}')t2 on t1.order_id = t2.order_id "


#-------------------------------------------------------------------------------------------
# 90天 有理由 退款 成功 钱数
hive -S -e "select 'money_youliyou_success_90day' as id,sum( if(t1.refund_price_deal=0.0,t1.refund_price_apply,t1.refund_price_deal) ) from (select rid,order_id,refund_price_deal,refund_price_apply from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}'  and refund_status = 41 and select_reason_id<200 and select_reason_id in (1,3,22,24,25,42,43,46,47,48,49)) t1 join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t2 on t1.order_id=t2.order_id left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;"

## 细分理由
# 90天内 有理由 发货问题 退款 成功 钱数
hive -S -e "select 'money_youliyou_fahuo_success_90day' as id,sum( if(t1.refund_price_deal=0.0,t1.refund_price_apply,t1.refund_price_deal) ) from (select rid,order_id,refund_price_deal,refund_price_apply from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}'  and refund_status = 41 and select_reason_id<200 and select_reason_id in (1,3,22,24,42,46)) t1 join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t2 on t1.order_id=t2.order_id left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;"

# 90天内 有理由 描述问题 退款 成功 钱数
hive -S -e "select 'money_youliyou_miaoshu_success_90day' as id,sum( if(t1.refund_price_deal=0.0,t1.refund_price_apply,t1.refund_price_deal) ) from (select rid,order_id,refund_price_deal,refund_price_apply from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}'  and refund_status = 41 and select_reason_id<200 and select_reason_id = 47) t1 join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t2 on t1.order_id=t2.order_id left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;"

# 90天内 有理由 质量问题 退款 成功 钱数
hive -S -e "select 'money_youliyou_zhiliang_success_90day' as id,sum( if(t1.refund_price_deal=0.0,t1.refund_price_apply,t1.refund_price_deal) ) from (select rid,order_id,refund_price_deal,refund_price_apply from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}'  and refund_status = 41 and select_reason_id<200 and select_reason_id in (25,43,48,49)) t1 join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t2 on t1.order_id=t2.order_id left outer join (select refund_id from ods_risk_stat_health_order where type=1 and is_valid=1) t3 on t1.rid=t3.refund_id where t3.refund_id is null limit 3;"

#-------------------------------------------------------------------------------------------


# 90天 无理由 成功 钱数
hive -S -e "select 'money_wuliyou_success_90day' as id, sum(if(refund_price_deal=0.0,refund_price_apply,refund_price_deal)) from (select rid, select_reason_id, order_id, refund_price_apply, refund_price_deal from ods_bat_order_refund where dt >= '${ninety_day_ago}' and dt <= '${one_day_ago}' and refund_status = 41) t1 join (select order_id from ods_bat_order where dt >= '${hundred_day_ago}' and dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59') t2 on t1.order_id=t2.order_id left outer join (select refund_id from ods_risk_stat_health_order where type = 1 and is_valid=1) t3 on t1.rid=t3.refund_id where select_reason_id >=200 or select_reason_id%100 not in (1, 3, 22, 24, 25, 42, 43, 46, 47, 48, 49) or t3.refund_id is not null limit 3;" 
# 90天内成交的订单，成交金额，ods_bat_order表 paytime > 90 
hive -S -e "select 'money_all_order_90day' as id,sum(total_price) from ods_bat_order where dt >= '${hundred_day_ago}' and  dt <= '${one_day_ago}' and from_unixtime(pay_time) >= '${ninety_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${one_day_ago} 23:59:59'"

# 第三个表 发货

# 72小时内未产生发货前退款订单数 退款时间大于发货时间 或者 无退款 但是必须是已经发货的
hive -S -e "select 'send_before_refund' as id,count(*) from (select send_time,apply_time,pay_time from (select order_id,send_time,pay_time from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${four_day_ago} 23:59:59' and from_unixtime(send_time) <= '${one_day_ago} 23:59:59' )t1 left outer join (select order_id,apply_time from ods_bat_order_refund where dt >='${four_day_ago}' and dt <= '${one_day_ago}')t2 on t1.order_id = t2.order_id) tt1 where tt1.send_time < tt1.apply_time and  (apply_time - pay_time) < ${millisecond_72hour}  or apply_time is NULL"
# 下单72小时内发货订单数
hive -S -e "select 'all_order_fahuo_xiayu_xiadan_72hour' as id,count(*) from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${four_day_ago} 23:59:59' and (send_time - pay_time) < ${millisecond_72hour}"


# ========

# 物流时间大于成交时间并且在72小时之内的 * 可能有问题
hive -S -e "SELECT 'wuliu_dayu_chengjiao_xiaoyu_72' AS id, count(*) FROM (           SELECT T3.pay_time, T2.express_time FROM (    SELECT order_id, express_id, ctime, pay_time, send_time, receive_time, regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(express_company, '^韵达.*', '韵达快运'), '^邮政.*|^国内小包.*', '包裹/平邮'), '^EMS.*', 'EMS'), '^中通.*', '中通快递'), '^圆通.*', '圆通快递'), '^申通.*', '申通快递'), '^汇通.*|^百世汇通.*', '汇通快运'), '^顺丰.*', '顺丰速运'), '^优速.*', '优速物流'), '^龙邦.*', '龙邦物流'), '^快捷.*', '快捷速递') AS express_company_inner FROM default.ods_bat_order WHERE dt >= '${ten_day_ago}' AND dt <= '${one_day_ago}' AND order_type=1000 AND from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' AND from_unixtime(pay_time) <= '${four_day_ago} 23:59:59'     )T3 JOIN (       SELECT T1.express_id AS ei, T1.et,T1.express_time as express_time, ods_bat_express_company.express_company AS ec FROM (SELECT express_id, express_code, express_time, from_unixtime(min(unix_timestamp(express_time))) AS et FROM dw.dw_order_express_detail WHERE order_create_dt >='${ten_day_ago}' AND order_create_dt <= '${one_day_ago}' GROUP BY express_id,express_code,express_time) T1 JOIN default.ods_bat_express_company ON T1.express_code = ods_bat_express_company.express_code       )T2 ON (T2.ei = T3.express_id AND T2.ec = T3.express_company_inner)              )tt1 WHERE express_time > from_unixtime(pay_time) AND (unix_timestamp(express_time) - pay_time) < ${millisecond_72hour}"

# 四天前的订单 发货减去成交时间的平均值，单位是毫秒 测试的时候，结果是平均331115.218349397757 毫秒，需要确认一下准确性 单位要变成天
hive -S -e "select 'avg_sendtime_paytime' as id, avg( (send_time-pay_time) )/86400 from ods_bat_order where dt >= '${ten_day_ago}' and dt <= '${one_day_ago}' and order_type=1000 and from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' and from_unixtime(pay_time) <= '${four_day_ago} 23:59:59' and send_time != 0  limit 10"

# 四天前的订单 物流时间减去成交时间的平均值，单位是毫秒 测试的时候结果是 68656.75362340015 毫秒 单位要变成天
hive -S -e "select 'avg_expresstime_paytime' as id ,avg( unix_timestamp(express_time) - pay_time)/86400 from (           SELECT T3.pay_time, T2.express_time FROM (    SELECT order_id, express_id, ctime, pay_time, send_time, receive_time, regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(express_company, '^韵达.*', '韵达快运'), '^邮政.*|^国内小包.*', '包裹/平邮'), '^EMS.*', 'EMS'), '^中通.*', '中通快递'), '^圆通.*', '圆通快递'), '^申通.*', '申通快递'), '^汇通.*|^百世汇通.*', '汇通快运'), '^顺丰.*', '顺丰速运'), '^优速.*', '优速物流'), '^龙邦.*', '龙邦物流'), '^快捷.*', '快捷速递') AS express_company_inner FROM default.ods_bat_order WHERE dt >= '${ten_day_ago}' AND dt <= '${one_day_ago}' AND order_type=1000 AND from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' AND from_unixtime(pay_time) <= '${four_day_ago} 23:59:59'     )T3 JOIN (       SELECT T1.express_id AS ei, T1.et,T1.express_time as express_time, ods_bat_express_company.express_company AS ec FROM (SELECT express_id, express_code, express_time, from_unixtime(min(unix_timestamp(express_time))) AS et FROM dw.dw_order_express_detail WHERE order_create_dt >='${ten_day_ago}' AND order_create_dt <= '${one_day_ago}' GROUP BY express_id,express_code,express_time) T1 JOIN default.ods_bat_express_company ON T1.express_code = ods_bat_express_company.express_code       )T2 ON (T2.ei = T3.express_id AND T2.ec = T3.express_company_inner)              )tt1 where express_time > from_unixtime(pay_time)"

# 两天前成交的订单，没有物流信息
hive -S -e "select 'wu_wuliu' as id ,count(*) from (           SELECT T3.order_id, T2.express_time FROM (    SELECT order_id, express_id, ctime, pay_time, send_time, receive_time, regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(express_company, '^韵达.*', '韵达快运'), '^邮政.*|^国内小包.*', '包裹/平邮'), '^EMS.*', 'EMS'), '^中通.*', '中通快递'), '^圆通.*', '圆通快递'), '^申通.*', '申通快递'), '^汇通.*|^百世汇通.*', '汇通快运'), '^顺丰.*', '顺丰速运'), '^优速.*', '优速物流'), '^龙邦.*', '龙邦物流'), '^快捷.*', '快捷速递') AS express_company_inner FROM default.ods_bat_order WHERE dt >= '${ten_day_ago}' AND dt <= '${one_day_ago}' AND order_type=1000 AND from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' AND from_unixtime(pay_time) <= '${four_day_ago} 23:59:59'     )T3 JOIN (       SELECT T1.express_id AS ei, T1.et,T1.express_time as express_time, ods_bat_express_company.express_company AS ec FROM (SELECT express_id, express_code, express_time, from_unixtime(min(unix_timestamp(express_time))) AS et FROM dw.dw_order_express_detail WHERE order_create_dt >='${ten_day_ago}' AND order_create_dt <= '${one_day_ago}' GROUP BY express_id,express_code,express_time) T1 JOIN default.ods_bat_express_company ON T1.express_code = ods_bat_express_company.express_code       )T2 ON (T2.ei = T3.express_id AND T2.ec = T3.express_company_inner)              ) tt1 where tt1.express_time is NULL"

# 两天前成交的订单，有物流信息，但是物流信息小于成交时间
hive -S -e "select 'wuliu_zaoyu_chengjiao' as id ,count(*) from (           SELECT T3.order_id, T2.express_time,pay_time FROM (    SELECT order_id, express_id, ctime, pay_time, send_time, receive_time, regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(express_company, '^韵达.*', '韵达快运'), '^邮政.*|^国内小包.*', '包裹/平邮'), '^EMS.*', 'EMS'), '^中通.*', '中通快递'), '^圆通.*', '圆通快递'), '^申通.*', '申通快递'), '^汇通.*|^百世汇通.*', '汇通快运'), '^顺丰.*', '顺丰速运'), '^优速.*', '优速物流'), '^龙邦.*', '龙邦物流'), '^快捷.*', '快捷速递') AS express_company_inner FROM default.ods_bat_order WHERE dt >= '${ten_day_ago}' AND dt <= '${one_day_ago}' AND order_type=1000 AND from_unixtime(pay_time) >= '${four_day_ago} 00:00:00' AND from_unixtime(pay_time) <= '${four_day_ago} 23:59:59'     )T3 JOIN (       SELECT T1.express_id AS ei, T1.et,T1.express_time as express_time, ods_bat_express_company.express_company AS ec FROM (SELECT express_id, express_code, express_time, from_unixtime(min(unix_timestamp(express_time))) AS et FROM dw.dw_order_express_detail WHERE order_create_dt >='${ten_day_ago}' AND order_create_dt <= '${one_day_ago}' GROUP BY express_id,express_code,express_time) T1 JOIN default.ods_bat_express_company ON T1.express_code = ods_bat_express_company.express_code       )T2 ON (T2.ei = T3.express_id AND T2.ec = T3.express_company_inner)              )tt1 where express_time is not NULL and express_time < from_unixtime(pay_time)"


# if [ $? != 0 ];then
#     echo "wrong"
# fi

