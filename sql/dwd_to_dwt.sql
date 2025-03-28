drop table if exists dwt_uv_topic;
create external table dwt_uv_topic
(
    `mid_id` string comment '设备id',
    `brand` string comment '手机品牌',
    `model` string comment '手机型号',
    `login_date_first` string  comment '首次活跃时间',
    `login_date_last` string  comment '末次活跃时间',
    `login_day_count` bigint comment '当日活跃次数',
    `login_count` bigint comment '累积活跃天数'
) COMMENT '设备主题宽表'
stored as parquet
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/dwt/dwt_uv_topic'
tblproperties ("parquet.compression"="lzo");


insert overwrite table dwt_uv_topic
select
    nvl(new.mid_id,old.mid_id),
    nvl(new.model,old.model),
    nvl(new.brand,old.brand),
    if(old.mid_id is null,'2025-03-23',old.login_date_first),
    if(new.mid_id is not null,'2025-03-23',old.login_date_last),
    if(new.mid_id is not null, new.login_count,0),
    nvl(old.login_count,0)+if(new.login_count>0,1,0)
from
    (
        select
            *
        from dwt_uv_topic
    )old
        full outer join
    (
        select
            *
        from dws_uv_detail_daycount
        where dt='2025-03-23'
    )new
    on old.mid_id=new.mid_id;

drop table if exists dwt_user_topic;
create external table dwt_user_topic
(
    user_id string  comment '用户id',
    login_date_first string  comment '首次登录时间',
    login_date_last string  comment '末次登录时间',
    login_count bigint comment '累积登录天数',
    login_last_30d_count bigint comment '最近30日登录天数',
    order_date_first string  comment '首次下单时间',
    order_date_last string  comment '末次下单时间',
    order_count bigint comment '累积下单次数',
    order_amount decimal(16,2) comment '累积下单金额',
    order_last_30d_count bigint comment '最近30日下单次数',
    order_last_30d_amount bigint comment '最近30日下单金额',
    payment_date_first string  comment '首次支付时间',
    payment_date_last string  comment '末次支付时间',
    payment_count decimal(16,2) comment '累积支付次数',
    payment_amount decimal(16,2) comment '累积支付金额',
    payment_last_30d_count decimal(16,2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16,2) comment '最近30日支付金额'
)COMMENT '会员主题宽表'
stored as parquet
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/dwt/dwt_user_topic/'
tblproperties ("parquet.compression"="lzo");


insert overwrite table dwt_user_topic
select
    nvl(new.user_id,old.user_id),
    if(old.login_date_first is null and new.login_count>0,'2025-03-23',old.login_date_first),
    if(new.login_count>0,'2025-03-23',old.login_date_last),
    nvl(old.login_count,0)+if(new.login_count>0,1,0),
    nvl(new.login_last_30d_count,0),
    if(old.order_date_first is null and new.order_count>0,'2025-03-23',old.order_date_first),
    if(new.order_count>0,'2025-03-23',old.order_date_last),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.order_amount,0)+nvl(new.order_amount,0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0),
    if(old.payment_date_first is null and new.payment_count>0,'2025-03-23',old.payment_date_first),
    if(new.payment_count>0,'2025-03-23',old.payment_date_last),
    nvl(old.payment_count,0)+nvl(new.payment_count,0),
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0)
from
    dwt_user_topic old
        full outer join
    (
        select
            user_id,
            sum(if(dt='2025-03-23',login_count,0)) login_count,
            sum(if(dt='2025-03-23',order_count,0)) order_count,
            sum(if(dt='2025-03-23',order_amount,0)) order_amount,
            sum(if(dt='2025-03-23',payment_count,0)) payment_count,
            sum(if(dt='2025-03-23',payment_amount,0)) payment_amount,
            sum(if(login_count>0,1,0)) login_last_30d_count,
            sum(order_count) order_last_30d_count,
            sum(order_amount) order_last_30d_amount,
            sum(payment_count) payment_last_30d_count,
            sum(payment_amount) payment_last_30d_amount
        from dws_user_action_daycount
        where dt>=date_add( '2025-03-23',-30)
        group by user_id
    )new
    on old.user_id=new.user_id;


drop table if exists dwt_sku_topic;
create external table dwt_sku_topic
(
    sku_id string comment 'sku_id',
    spu_id string comment 'spu_id',
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(16,2)  comment '最近30日被下单金额',
    order_count bigint comment '累积被下单次数',
    order_num bigint comment '累积被下单件数',
    order_amount decimal(16,2) comment '累积被下单金额',
    payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(16,2) comment '最近30日被支付金额',
    payment_count   bigint  comment '累积被支付次数',
    payment_num bigint comment '累积被支付件数',
    payment_amount  decimal(16,2) comment '累积被支付金额',
    refund_last_30d_count bigint comment '最近三十日退款次数',
    refund_last_30d_num bigint comment '最近三十日退款件数',
    refund_last_30d_amount decimal(16,2) comment '最近三十日退款金额',
    refund_count bigint comment '累积退款次数',
    refund_num bigint comment '累积退款件数',
    refund_amount decimal(16,2) comment '累积退款金额',
    cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    cart_count bigint comment '累积被加入购物车次数',
    favor_last_30d_count bigint comment '最近30日被收藏次数',
    favor_count bigint comment '累积被收藏次数',
    appraise_last_30d_good_count bigint comment '最近30日好评数',
    appraise_last_30d_mid_count bigint comment '最近30日中评数',
    appraise_last_30d_bad_count bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
    appraise_good_count bigint comment '累积好评数',
    appraise_mid_count bigint comment '累积中评数',
    appraise_bad_count bigint comment '累积差评数',
    appraise_default_count bigint comment '累积默认评价数'
 )COMMENT '商品主题宽表'
stored as parquet
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/dwt/dwt_sku_topic/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dwt_sku_topic
select
    nvl(new.sku_id,old.sku_id),
    sku_info.spu_id,
    nvl(new.order_count30,0),
    nvl(new.order_num30,0),
    nvl(new.order_amount30,0),
    nvl(old.order_count,0) + nvl(new.order_count,0),
    nvl(old.order_num,0) + nvl(new.order_num,0),
    nvl(old.order_amount,0) + nvl(new.order_amount,0),
    nvl(new.payment_count30,0),
    nvl(new.payment_num30,0),
    nvl(new.payment_amount30,0),
    nvl(old.payment_count,0) + nvl(new.payment_count,0),
    nvl(old.payment_num,0) + nvl(new.payment_count,0),
    nvl(old.payment_amount,0) + nvl(new.payment_count,0),
    nvl(new.refund_count30,0),
    nvl(new.refund_num30,0),
    nvl(new.refund_amount30,0),
    nvl(old.refund_count,0) + nvl(new.refund_count,0),
    nvl(old.refund_num,0) + nvl(new.refund_num,0),
    nvl(old.refund_amount,0) + nvl(new.refund_amount,0),
    nvl(new.cart_count30,0),
    nvl(old.cart_count,0) + nvl(new.cart_count,0),
    nvl(new.favor_count30,0),
    nvl(old.favor_count,0) + nvl(new.favor_count,0),
    nvl(new.appraise_good_count30,0),
    nvl(new.appraise_mid_count30,0),
    nvl(new.appraise_bad_count30,0),
    nvl(new.appraise_default_count30,0)  ,
    nvl(old.appraise_good_count,0) + nvl(new.appraise_good_count,0),
    nvl(old.appraise_mid_count,0) + nvl(new.appraise_mid_count,0),
    nvl(old.appraise_bad_count,0) + nvl(new.appraise_bad_count,0),
    nvl(old.appraise_default_count,0) + nvl(new.appraise_default_count,0)
from
    dwt_sku_topic old
        full outer join
    (
        select
            sku_id,
            sum(if(dt='2025-03-23', order_count,0 )) order_count,
            sum(if(dt='2025-03-23',order_num ,0 ))  order_num,
            sum(if(dt='2025-03-23',order_amount,0 )) order_amount ,
            sum(if(dt='2025-03-23',payment_count,0 )) payment_count,
            sum(if(dt='2025-03-23',payment_num,0 )) payment_num,
            sum(if(dt='2025-03-23',payment_amount,0 )) payment_amount,
            sum(if(dt='2025-03-23',refund_count,0 )) refund_count,
            sum(if(dt='2025-03-23',refund_num,0 )) refund_num,
            sum(if(dt='2025-03-23',refund_amount,0 )) refund_amount,
            sum(if(dt='2025-03-23',cart_count,0 )) cart_count,
            sum(if(dt='2025-03-23',favor_count,0 )) favor_count,
            sum(if(dt='2025-03-23',appraise_good_count,0 )) appraise_good_count,
            sum(if(dt='2025-03-23',appraise_mid_count,0 ) ) appraise_mid_count ,
            sum(if(dt='2025-03-23',appraise_bad_count,0 )) appraise_bad_count,
            sum(if(dt='2025-03-23',appraise_default_count,0 )) appraise_default_count,
            sum(order_count) order_count30 ,
            sum(order_num) order_num30,
            sum(order_amount) order_amount30,
            sum(payment_count) payment_count30,
            sum(payment_num) payment_num30,
            sum(payment_amount) payment_amount30,
            sum(refund_count) refund_count30,
            sum(refund_num) refund_num30,
            sum(refund_amount) refund_amount30,
            sum(cart_count) cart_count30,
            sum(favor_count) favor_count30,
            sum(appraise_good_count) appraise_good_count30,
            sum(appraise_mid_count) appraise_mid_count30,
            sum(appraise_bad_count) appraise_bad_count30,
            sum(appraise_default_count) appraise_default_count30
        from dws_sku_action_daycount
        where dt >= date_add ('2025-03-23', -30)
        group by sku_id
    )new
    on new.sku_id = old.sku_id
        left join
    (select * from dwd_dim_sku_info where dt='2025-03-23') sku_info
    on nvl(new.sku_id,old.sku_id)= sku_info.id;


drop table if exists dwt_activity_topic;
create external table dwt_activity_topic(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    `display_day_count` bigint COMMENT '当日曝光次数',
    `order_day_count` bigint COMMENT '当日下单次数',
    `order_day_amount` decimal(20,2) COMMENT '当日下单金额',
    `payment_day_count` bigint COMMENT '当日支付次数',
    `payment_day_amount` decimal(20,2) COMMENT '当日支付金额',
    `display_count` bigint COMMENT '累积曝光次数',
    `order_count` bigint COMMENT '累积下单次数',
    `order_amount` decimal(20,2) COMMENT '累积下单金额',
    `payment_count` bigint COMMENT '累积支付次数',
    `payment_amount` decimal(20,2) COMMENT '累积支付金额'
) COMMENT '活动主题宽表'
stored as parquet
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/dwt/dwt_activity_topic/'
tblproperties ("parquet.compression"="lzo");


insert overwrite table dwt_activity_topic
select
    nvl(new.id,old.id),
    nvl(new.activity_name,old.activity_name),
    nvl(new.activity_type,old.activity_type),
    nvl(new.start_time,old.start_time),
    nvl(new.end_time,old.end_time),
    nvl(new.create_time,old.create_time),
    nvl(new.display_count,0),
    nvl(new.order_count,0),
    nvl(new.order_amount,0.0),
    nvl(new.payment_count,0),
    nvl(new.payment_amount,0.0),
    nvl(new.display_count,0)+nvl(old.display_count,0),
    nvl(new.order_count,0)+nvl(old.order_count,0),
    nvl(new.order_amount,0.0)+nvl(old.order_amount,0.0),
    nvl(new.payment_count,0)+nvl(old.payment_count,0),
    nvl(new.payment_amount,0.0)+nvl(old.payment_amount,0.0)
from
    (
        select
            *
        from dwt_activity_topic
    )old
        full outer join
    (
        select
            *
        from dws_activity_info_daycount
        where dt='2025-03-23'
    )new
    on old.id=new.id;


drop table if exists dwt_area_topic;
create external table dwt_area_topic(
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_day_count` string COMMENT '当天活跃设备数',
    `login_last_30d_count` string COMMENT '最近30天活跃设备数',
    `order_day_count` bigint COMMENT '当天下单次数',
    `order_day_amount` decimal(16,2) COMMENT '当天下单金额',
    `order_last_30d_count` bigint COMMENT '最近30天下单次数',
    `order_last_30d_amount` decimal(16,2) COMMENT '最近30天下单金额',
    `payment_day_count` bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16,2) COMMENT '当天支付金额',
    `payment_last_30d_count` bigint COMMENT '最近30天支付次数',
    `payment_last_30d_amount` decimal(16,2) COMMENT '最近30天支付金额'
) COMMENT '地区主题宽表'
stored as parquet
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/dwt/dwt_area_topic/'
tblproperties ("parquet.compression"="lzo");


insert overwrite table dwt_area_topic
select
    nvl(old.id,new.id),
    nvl(old.province_name,new.province_name),
    nvl(old.area_code,new.area_code),
    nvl(old.iso_code,new.iso_code),
    nvl(old.region_id,new.region_id),
    nvl(old.region_name,new.region_name),
    nvl(new.login_day_count,0),
    nvl(new.login_last_30d_count,0),
    nvl(new.order_day_count,0),
    nvl(new.order_day_amount,0.0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0.0),
    nvl(new.payment_day_count,0),
    nvl(new.payment_day_amount,0.0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0.0)
from
    (
        select
            *
        from dwt_area_topic
    )old
        full outer join
    (
        select
            id,
            province_name,
            area_code,
            iso_code,
            region_id,
            region_name,
            sum(if(dt='2025-03-23',login_count,0)) login_day_count,
            sum(if(dt='2025-03-23',order_count,0)) order_day_count,
            sum(if(dt='2025-03-23',order_amount,0.0)) order_day_amount,
            sum(if(dt='2025-03-23',payment_count,0)) payment_day_count,
            sum(if(dt='2025-03-23',payment_amount,0.0)) payment_day_amount,
            sum(login_count) login_last_30d_count,
            sum(order_count) order_last_30d_count,
            sum(order_amount) order_last_30d_amount,
            sum(payment_count) payment_last_30d_count,
            sum(payment_amount) payment_last_30d_amount
        from dws_area_stats_daycount
        where dt>=date_add('2025-03-23',-30)
        group by id,province_name,area_code,iso_code,region_id,region_name
    )new
    on old.id=new.id;


