-- create database dev_realtime_v2_xinyu_luo;

use dev_realtime_v2_xinyu_luo;

-- 商品信息表
drop table if exists ods_product_info;
create external table if not exists ods_product_info (
    product_id STRING COMMENT '商品ID，用于唯一标识商品',
    product_name STRING COMMENT '商品名称',
    shop_name STRING COMMENT '商铺名称',
    category_leaf STRING COMMENT '商品所属叶子类目',
    product_price DECIMAL(10, 2) COMMENT '商品价格'
)PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/warehouse/dev_realtime_v2_xinyu_luo/ods/ods_product_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo_v2/product_info/2025-03-31' overwrite into table dev_realtime_v2_xinyu_luo.ods_product_info partition(dt='2025-03-31');

-- 访客行为表
drop table if exists ods_visitor_behavior;
create external table if not exists ods_visitor_behavior (
    visitor_id STRING COMMENT '访客ID，用于唯一标识访客',
    product_id STRING COMMENT '商品ID，关联商品信息表',
    visit_time TIMESTAMP COMMENT '访问时间',
    stay_duration DECIMAL(5, 2) COMMENT '停留时长，单位为秒',
    is_collected INT COMMENT '是否点击收藏，1 表示收藏，0 表示未收藏',
    is_added_to_cart INT COMMENT '是否加入购物车，1 表示加入，0 表示未加入',
    is_detail_click INT COMMENT '是否点击详情页，1 表示点击，0 表示未点击'
)PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/warehouse/dev_realtime_v2_xinyu_luo/ods/ods_visitor_behavior/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo_v2/visitor_behavior/2025-03-31' overwrite into table dev_realtime_v2_xinyu_luo.ods_visitor_behavior partition(dt='2025-03-31');

-- 订单表
drop table if exists ods_order_info;
create external table if not exists ods_order_info (
    order_id STRING COMMENT '订单ID，用于唯一标识订单',
    buyer_id STRING COMMENT '买家ID，用于唯一标识买家',
    product_id STRING COMMENT '商品ID，关联商品信息表',
    order_time TIMESTAMP COMMENT '下单时间',
    order_item_count INT COMMENT '下单件数',
    order_amount DECIMAL(10, 2) COMMENT '下单金额'
)PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/warehouse/dev_realtime_v2_xinyu_luo/ods/ods_order_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo_v2/order_info/2025-03-31' overwrite into table dev_realtime_v2_xinyu_luo.ods_order_info partition(dt='2025-03-31');

-- 支付表
drop table if exists ods_payment_info;
create external table if not exists ods_payment_info (
    payment_id STRING COMMENT '支付ID，用于唯一标识支付记录',
    buyer_id STRING COMMENT '买家ID，关联订单表中的买家ID',
    product_id STRING COMMENT '商品ID，关联商品信息表',
    payment_time TIMESTAMP COMMENT '支付时间',
    payment_amount DECIMAL(10, 2) COMMENT '支付金额',
    payment_channel STRING COMMENT '支付渠道，如支付宝、微信等',
    is_new_buyer INT COMMENT '是否为新买家，1 表示新买家，0 表示老买家'
)PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/warehouse/dev_realtime_v2_xinyu_luo/ods/ods_payment_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo_v2/payment_info/2025-03-31' overwrite into table dev_realtime_v2_xinyu_luo.ods_payment_info partition(dt='2025-03-31');

-- 退款表
drop table if exists ods_refund_info;
create external table if not exists ods_refund_info (
    refund_id STRING COMMENT '退款ID，用于唯一标识退款记录',
    buyer_id STRING COMMENT '买家ID，关联订单表和支付表中的买家ID',
    product_id STRING COMMENT '商品ID，关联商品信息表',
    refund_time TIMESTAMP COMMENT '退款时间',
    refund_amount DECIMAL(10, 2) COMMENT '退款金额',
    refund_type STRING COMMENT '退款类型，如售中退款、售后退款等'
)PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/warehouse/dev_realtime_v2_xinyu_luo/ods/ods_refund_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo_v2/refund_info/2025-03-31' overwrite into table dev_realtime_v2_xinyu_luo.ods_refund_info partition(dt='2025-03-31');

-- 活动表
drop table if exists ods_produce_activity;
create external table if not exists ods_produce_activity (
    activity_id STRING COMMENT '活动ID，用于唯一标识活动',
    product_id STRING COMMENT '商品ID，关联商品信息表',
    activity_name STRING COMMENT '活动名称，如聚划算等',
    activity_time TIMESTAMP COMMENT '活动时间',
    activity_payment_amount DECIMAL(10, 2) COMMENT '活动产生的支付金额'
)PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/warehouse/dev_realtime_v2_xinyu_luo/ods/ods_produce_activity/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo_v2/produce_activity/2025-03-31' overwrite into table dev_realtime_v2_xinyu_luo.ods_produce_activity partition(dt='2025-03-31');


drop table if exists dws_product_theme_dashboard;
create external table if not exists dws_product_theme_dashboard (
    product_id string comment '商品id',
    product_name string comment '商品名称',
    shop_name string comment '商铺名称',
    product_visitor_num int comment '商品访客数',
    visited_product_count int comment '有访问商品数',
    product_view_count int comment '商品浏览量',
    average_stay_duration decimal(5, 2) comment '商品平均停留时长，单位为秒',
    detail_page_bounce_rate decimal(5, 2) comment '商品详情页跳出率',
    product_collection_num int comment '商品收藏人数',
    product_cart_item_num int comment '商品加购件数',
    product_cart_num int comment '商品加购人数',
    visit_collection_conversion_rate decimal(5, 2) comment '访问收藏转化率',
    visit_cart_conversion_rate decimal(5, 2) comment '访问加购转化率',
    order_placed_buyer_count int comment '下单买家数',
    order_placed_item_count int comment '下单件数',
    order_placed_amount decimal(10, 2) comment '下单金额',
    order_placed_conversion_rate decimal(5, 2) comment '下单转化率',
    payment_buyer_count int comment '支付买家数',
    payment_item_count int comment '支付件数',
    payment_amount decimal(10, 2) comment '支付金额',
    paid_product_count int comment '有支付商品数',
    payment_conversion_ratio decimal(5, 2) comment '支付转化率',
    new_payment_buyer_count int comment '支付新买家数',
    old_payment_buyer_count int comment '支付老买家数',
    old_buyer_payment_amount decimal(10, 2) comment '老买家支付金额',
    average_payment_per_buyer decimal(10, 2) comment '客单价',
    successful_refund_amount decimal(10, 2) comment '成功退款退货金额',
    juhuasuan_payment_amount decimal(10, 2) comment '聚划算支付金额',
    annual_cumulative_payment_amount decimal(10, 2) comment '年累计支付金额',
    average_visitor_value decimal(10, 2) comment '访客平均价值',
    competitiveness_score decimal(5, 2) comment '竞争力评分',
    micro_detail_visitor_count int comment '商品微详情访客数'
) PARTITIONED BY (dt string) -- 按照时间创建分区
location '/warehouse/dev_realtime_v2_xinyu_luo/dws/dws_product_theme_dashboard/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="snappy");



insert overwrite table dws_product_theme_dashboard
select
    pi.product_id,
    pi.product_name,
    pi.shop_name,
    -- 商品访客数
    count(distinct vb.visitor_id) as product_visitor_num,
    -- 有访问商品数
    count(distinct case when vb.stay_duration > 0 then vb.product_id end) as visited_product_count,
    -- 商品浏览量
    count(vb.visit_time) as product_view_count,
    -- 商品平均停留时长
    round(AVG(vb.stay_duration), 2) as average_stay_duration,
    -- 商品详情页跳出率
    round(sum(case when vb.is_detail_click = 0 then 1 else 0 end) / count(vb.visitor_id) * 100, 4) as detail_page_bounce_rate,
    -- 商品收藏人数
    count(distinct case when vb.is_collected = 1 then vb.visitor_id end) as product_collection_num,
    -- 商品加购件数
    sum(ot.order_item_count) as product_cart_item_num,
    -- 商品加购人数
    count(distinct ot.buyer_id) as product_cart_num,
    -- 访问收藏转化率
    round(count(distinct case when vb.is_collected = 1 then vb.visitor_id end) / count(distinct vb.visitor_id) * 100, 4) as visit_collection_conversion_rate,
    -- 访问加购转化率
    round(count(distinct ot.buyer_id) / count(distinct vb.visitor_id) * 100, 4) as visit_cart_conversion_rate,
    -- 下单买家数
    count(distinct ot.buyer_id) as order_placed_buyer_count,
    -- 下单件数
    sum(ot.order_item_count) as order_placed_item_count,
    -- 下单金额
    sum(ot.order_amount) as order_placed_amount,
    -- 下单转化率
    round(count(distinct ot.buyer_id) / count(distinct vb.visitor_id) * 100, 4) as order_placed_conversion_rate,
    -- 支付买家数
    count(distinct pt.buyer_id) as payment_buyer_count,
    -- 支付件数
    sum(pt.payment_amount / pi.product_price) as payment_item_count,
    -- 支付金额
    sum(pt.payment_amount) as payment_amount,
    -- 有支付商品数
    count(distinct case when pt.payment_amount > 0 then pt.product_id end) as paid_product_count,
    -- 支付转化率
    round(count(distinct pt.buyer_id) / count(distinct vb.visitor_id) * 100, 4) as payment_conversion_ratio,
    -- 支付新买家数
    count(distinct case when pt.is_new_buyer = 1 then pt.buyer_id end) as new_payment_buyer_count,
    -- 支付老买家数
    count(distinct case when pt.is_new_buyer = 0 then pt.buyer_id end) as old_payment_buyer_count,
    -- 老买家支付金额
    sum(case when pt.is_new_buyer = 0 then pt.payment_amount else 0 end) as old_buyer_payment_amount,
    -- 客单价
    round(sum(pt.payment_amount) / count(distinct pt.buyer_id), 2) as average_payment_per_buyer,
    -- 成功退款退货金额
    sum(rt.refund_amount) as successful_refund_amount,
    -- 聚划算支付金额
    sum(case when at.activity_name = 'Juhuasuan' then at.activity_payment_amount else 0 end) as juhuasuan_payment_amount,
    -- 年累计支付金额
    sum(pt.payment_amount) as annual_cumulative_payment_amount,
    -- 访客平均价值
    round(sum(pt.payment_amount) / count(distinct vb.visitor_id), 2) as average_visitor_value,
    -- 竞争力评分（这里简单假设为支付金额排名，可根据实际情况调整）
    rank() over (order by sum(pt.payment_amount) desc) as competitiveness_score,
    -- 商品微详情访客数
        count(distinct case when vb.is_detail_click = 1 then vb.visitor_id end) as micro_detail_visitor_count,
    -- 假设以订单时间作为统计时间
    pi.dt
FROM
    ods_product_info pi
-- 关联访客行为表
        left join
    ods_visitor_behavior vb on pi.product_id = vb.product_id
-- 关联订单表
        left join
    ods_order_info ot on pi.product_id = ot.product_id
-- 关联支付表
        left join
    ods_payment_info pt on pi.product_id = pt.product_id
-- 关联退款表
        left join
    ods_refund_info rt on pi.product_id = rt.product_id
-- 关联活动表
        left join
    ods_produce_activity at on pi.product_id = at.product_id
where pi.dt='2025-03-31' and vb.dt='2025-03-31' and ot.dt='2025-03-31'
  and pt.dt='2025-03-31' and rt.dt='2025-03-31' and at.dt='2025-03-31'
group by
    pi.product_id, pi.product_name, pi.shop_name,pi.dt;


select * from dws_product_theme_dashboard;