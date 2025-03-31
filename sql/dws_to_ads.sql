drop table if exists ads_user_topic;
create external table ads_user_topic(
    `dt` string COMMENT '统计日期',
    `day_users` string COMMENT '活跃会员数',
    `day_new_users` string COMMENT '新增会员数',
    `day_new_payment_users` string COMMENT '新增消费会员数',
    `payment_users` string COMMENT '总付费会员数',
    `users` string COMMENT '总会员数',
    `day_users2users` decimal(16,2) COMMENT '会员活跃率',
    `payment_users2users` decimal(16,2) COMMENT '会员付费率',
    `day_new_users2users` decimal(16,2) COMMENT '会员新鲜度'
) COMMENT '会员信息表'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_user_topic';

insert into table ads_user_topic
select
    '2025-03-23',
    sum(if(login_date_last='2025-03-23',1,0)),
    sum(if(login_date_first='2025-03-23',1,0)),
    sum(if(payment_date_first='2025-03-23',1,0)),
    sum(if(payment_count>0,1,0)),
    count(*),
    sum(if(login_date_last='2025-03-23',1,0))/count(*),
    sum(if(payment_count>0,1,0))/count(*),
    sum(if(login_date_first='2025-03-23',1,0))/sum(if(login_date_last='2025-03-23',1,0))
from dwt_user_topic;


drop table if exists ads_user_action_convert_day;
create external  table ads_user_action_convert_day(
    `dt` string COMMENT '统计日期',
    `home_count`  bigint COMMENT '浏览首页人数',
    `good_detail_count` bigint COMMENT '浏览商品详情页人数',
    `home2good_detail_convert_ratio` decimal(16,2) COMMENT '首页到商品详情转化率',
    `cart_count` bigint COMMENT '加入购物车的人数',
    `good_detail2cart_convert_ratio` decimal(16,2) COMMENT '商品详情页到加入购物车转化率',
    `order_count` bigint     COMMENT '下单人数',
    `cart2order_convert_ratio`  decimal(16,2) COMMENT '加入购物车到下单转化率',
    `payment_amount` bigint     COMMENT '支付人数',
    `order2payment_convert_ratio` decimal(16,2) COMMENT '下单到支付的转化率'
) COMMENT '漏斗分析'
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_user_action_convert_day/';

with
    tmp_uv as
        (
            select
                '2025-03-23' dt,
                sum(if(array_contains(pages,'home'),1,0)) home_count,
                sum(if(array_contains(pages,'good_detail'),1,0)) good_detail_count
            from
                (
                    select
                        mid_id,
                        collect_set(page_id) pages
                    from dwd_page_log
                    where

                    group by mid_id
                )tmp
        ),
    tmp_cop as
        (
            select
                '2025-03-23' dt,
                sum(if(cart_count>0,1,0)) cart_count,
                sum(if(order_count>0,1,0)) order_count,
                sum(if(payment_count>0,1,0)) payment_count
            from dws_user_action_daycount
            where dt='2025-03-23'
        )
insert into table ads_user_action_convert_day
select
    tmp_uv.dt,
    tmp_uv.home_count,
    tmp_uv.good_detail_count,
    tmp_uv.good_detail_count/tmp_uv.home_count*100,
    tmp_cop.cart_count,
    tmp_cop.cart_count/tmp_uv.good_detail_count*100,
    tmp_cop.order_count,
    tmp_cop.order_count/tmp_cop.cart_count*100,
    tmp_cop.payment_count,
    tmp_cop.payment_count/tmp_cop.order_count*100
from tmp_uv
         join tmp_cop
              on tmp_uv.dt=tmp_cop.dt;


drop table if exists ads_product_info;
create external table ads_product_info(
    `dt` string COMMENT '统计日期',
    `sku_num` string COMMENT 'sku个数',
    `spu_num` string COMMENT 'spu个数'
) COMMENT '商品个数信息'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_product_info';

insert into table ads_product_info
select
    '2025-03-23' dt,
    sku_num,
    spu_num
from
    (
        select
            '2025-03-23' dt,
            count(*) sku_num
        from
            dwt_sku_topic
    ) tmp_sku_num
        join
    (
        select
            '2025-03-23' dt,
            count(*) spu_num
        from
            (
                select
                    spu_id
                from
                    dwt_sku_topic
                group by
                    spu_id
            ) tmp_spu_id
    ) tmp_spu_num
    on tmp_sku_num.dt=tmp_spu_num.dt;


drop table if exists ads_product_sale_topN;
create external table ads_product_sale_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `payment_amount` bigint COMMENT '销量'
) COMMENT '商品销量排名'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_product_sale_topN';

insert into table ads_product_sale_topN
select
    '2025-03-23' dt,
    sku_id,
    payment_amount
from
    dws_sku_action_daycount
where
        dt='2025-03-23'
order by payment_amount desc
    limit 10;


drop table if exists ads_product_favor_topN;
create external table ads_product_favor_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏排名'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_product_favor_topN';


insert into table ads_product_favor_topN
select
    '2025-03-23' dt,
    sku_id,
    favor_count
from
    dws_sku_action_daycount
where
        dt='2025-03-23'
order by favor_count desc
    limit 10;


drop table if exists ads_product_cart_topN;
create external table ads_product_cart_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `cart_count` bigint COMMENT '加入购物车次数'
) COMMENT '商品加入购物车排名'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_product_cart_topN';

insert into table ads_product_cart_topN
select
    '2025-03-23' dt,
    sku_id,
    cart_count
from
    dws_sku_action_daycount
where
        dt='2025-03-23'
order by cart_count desc
    limit 10;


drop table if exists ads_product_refund_topN;
create external table ads_product_refund_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `refund_ratio` decimal(16,2) COMMENT '退款率'
) COMMENT '商品退款率排名'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_product_refund_topN';

insert into table ads_product_refund_topN
select
    '2025-03-23',
    sku_id,
    refund_last_30d_count/payment_last_30d_count*100 refund_ratio
from dwt_sku_topic
order by refund_ratio desc


drop table if exists ads_appraise_bad_topN;
create external table ads_appraise_bad_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `appraise_bad_ratio` decimal(16,2) COMMENT '差评率'
) COMMENT '商品差评率'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_appraise_bad_topN';

insert into table ads_appraise_bad_topN
select
    '2025-03-23' dt,
    sku_id,
    appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) appraise_bad_ratio
from
    dws_sku_action_daycount
where
        dt='2025-03-23'
order by appraise_bad_ratio desc
    limit 10;


drop table if exists ads_order_daycount;
create external table ads_order_daycount(
    dt string comment '统计日期',
    order_count bigint comment '单日下单笔数',
    order_amount bigint comment '单日下单金额',
    order_users bigint comment '单日下单用户数'
) comment '下单数目统计'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_order_daycount';

insert into table ads_order_daycount
select
    '2025-03-23',
    sum(order_count),
    sum(order_amount),
    sum(if(order_count>0,1,0))
from dws_user_action_daycount
where dt='2025-03-23';


drop table if exists ads_payment_daycount;
create external table ads_payment_daycount(
    dt string comment '统计日期',
    order_count bigint comment '单日支付笔数',
    order_amount bigint comment '单日支付金额',
    payment_user_count bigint comment '单日支付人数',
    payment_sku_count bigint comment '单日支付商品数',
    payment_avg_time decimal(16,2) comment '下单到支付的平均时长，取分钟数'
) comment '支付信息统计'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_payment_daycount';

insert into table ads_payment_daycount
select
    tmp_payment.dt,
    tmp_payment.payment_count,
    tmp_payment.payment_amount,
    tmp_payment.payment_user_count,
    tmp_skucount.payment_sku_count,
    tmp_time.payment_avg_time
from
    (
        select
            '2025-03-23' dt,
            sum(payment_count) payment_count,
            sum(payment_amount) payment_amount,
            sum(if(payment_count>0,1,0)) payment_user_count
        from dws_user_action_daycount
        where dt='2025-03-23'
    )tmp_payment
        join
    (
        select
            '2025-03-23' dt,
            sum(if(payment_count>0,1,0)) payment_sku_count
        from dws_sku_action_daycount
        where dt='2025-03-23'
    )tmp_skucount on tmp_payment.dt=tmp_skucount.dt
        join
    (
        select
            '2025-03-23' dt,
            sum(unix_timestamp(payment_time)-unix_timestamp(create_time))/count(*)/60 payment_avg_time
        from dwd_fact_order_info
        where dt='2025-03-23'
          and payment_time is not null
    )tmp_time on tmp_payment.dt=tmp_time.dt;


drop table ads_sale_tm_category1_stat_mn;
create external table ads_sale_tm_category1_stat_mn
(
    tm_id string comment '品牌id',
    category1_id string comment '1级品类id ',
    category1_name string comment '1级品类名称 ',
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(16,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(16,2)  comment  '多次复购率',
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期'
) COMMENT '品牌复购率统计'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_sale_tm_category1_stat_mn/';

with
    tmp_order as
        (
            select
                user_id,
                order_stats_struct.sku_id sku_id,
                order_stats_struct.order_count order_count
            from dws_user_action_daycount lateral view explode(order_detail_stats) tmp as order_stats_struct
where date_format(dt,'yyyy-MM')=date_format('2025-03-23','yyyy-MM')
    ),
    tmp_sku as
    (
select
    id,
    tm_id,
    category1_id,
    category1_name
from dwd_dim_sku_info
where dt='2025-03-23'
    )
insert into table ads_sale_tm_category1_stat_mn
select
    tm_id,
    category1_id,
    category1_name,
    sum(if(order_count>=1,1,0)) buycount,
    sum(if(order_count>=2,1,0)) buyTwiceLast,
    sum(if(order_count>=2,1,0))/sum( if(order_count>=1,1,0)) buyTwiceLastRatio,
    sum(if(order_count>=3,1,0))  buy3timeLast  ,
    sum(if(order_count>=3,1,0))/sum( if(order_count>=1,1,0)) buy3timeLastRatio ,
    date_format('2025-03-23' ,'yyyy-MM') stat_mn,
    '2025-03-23' stat_date
from
    (
        select
            tmp_order.user_id,
            tmp_sku.category1_id,
            tmp_sku.category1_name,
            tmp_sku.tm_id,
            sum(order_count) order_count
        from tmp_order
                 join tmp_sku
                      on tmp_order.sku_id=tmp_sku.id
        group by tmp_order.user_id,tmp_sku.category1_id,tmp_sku.category1_name,tmp_sku.tm_id
    )tmp
group by tm_id, category1_id, category1_name;


drop table if exists ads_area_topic;
create external table ads_area_topic(
    `dt` string COMMENT '统计日期',
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_day_count` bigint COMMENT '当天活跃设备数',
    `order_day_count` bigint COMMENT '当天下单次数',
    `order_day_amount` decimal(16,2) COMMENT '当天下单金额',
    `payment_day_count` bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16,2) COMMENT '当天支付金额'
) COMMENT '地区主题信息'
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ads/ads_area_topic/';

insert into table ads_area_topic
select
    '2025-03-23',
    id,
    province_name,
    area_code,
    iso_code,
    region_id,
    region_name,
    login_day_count,
    order_day_count,
    order_day_amount,
    payment_day_count,
    payment_day_amount
from dwt_area_topic;


