drop table if  exists dwd_dim_sku_info;
create external table dwd_dim_sku_info (
id string COMMENT '商品id',
spu_id string COMMENT 'spuid',
price decimal(16,2) COMMENT '商品价格',
sku_name string COMMENT '商品名称',
sku_desc string COMMENT '商品描述',
weight decimal(16,2) COMMENT '重量',
tm_id string COMMENT '品牌id',
tm_name string COMMENT '品牌名称',
category3_id string COMMENT '三级分类id',
category2_id string COMMENT '二级分类id',
category1_id string COMMENT '一级分类id',
category3_name string COMMENT '三级分类名称',
category2_name string COMMENT '二级分类名称',
category1_name string COMMENT '一级分类名称',
spu_name string COMMENT 'spu名称',
create_time string COMMENT '创建时间'
) COMMENT '商品维度表'
PARTITIONED BY (dt string)
stored as parquet
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/dwd/dwd_dim_sku_info/'
tblproperties ("parquet.compression"="lzo");

-- set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

set hive.exec.dynamic.partition.mode='nonstrict';
insert overwrite table dwd_dim_sku_info partition(dt)
select
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    ob.tm_name,
    sku.category3_id,
    c2.id as category2_id,
    c1.id as category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    spu.spu_name,
    sku.create_time,
    sku.dt
from
    (
        select * from ods_sku_info where dt='2025-03-23'
    )sku
        join
    (
        select * from ods_base_trademark where dt='2025-03-23'
    )ob on sku.tm_id=ob.tm_id
        join
    (
        select * from ods_spu_info where dt='2025-03-23'
    )spu on spu.id = sku.spu_id
        join
    (
        select * from ods_base_category3 where dt='2025-03-23'
    )c3 on sku.category3_id=c3.id
        join
    (
        select * from ods_base_category2 where dt='2025-03-23'
    )c2 on c3.category2_id=c2.id
        join
    (
        select * from ods_base_category1 where dt='2025-03-23'
    )c1 on c2.category1_id=c1.id;


drop table if  exists dwd_dim_coupon_info;
create external table dwd_dim_coupon_info(
    id string COMMENT '购物券编号',
    coupon_name string COMMENT '购物券名称',
    coupon_type string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    condition_amount decimal(16,2) COMMENT '满额数',
    condition_num bigint COMMENT '满件数',
    activity_id string COMMENT '活动编号',
    benefit_amount decimal(16,2) COMMENT '减金额',
    benefit_discount decimal(16,2) COMMENT '折扣',
    create_time string COMMENT '创建时间',
    range_type string COMMENT '范围类型 1、商品 2、品类 3、品牌',
    spu_id string COMMENT '商品id',
    tm_id string COMMENT '品牌id',
    category3_id string COMMENT '品类id',
    limit_num bigint COMMENT '最多领用次数',
    operate_time  string COMMENT '修改时间',
    expire_time  string COMMENT '过期时间'
) COMMENT '优惠券维度表'
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_coupon_info/'
tblproperties ("parquet.compression"="lzo");

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_coupon_info partition(dt='2025-03-23')
select
    id,
    coupon_name,
    coupon_type,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    create_time,
    range_type,
    spu_id,
    tm_id,
    category3_id,
    limit_num,
    operate_time,
    expire_time
from ods_coupon_info
where dt='2025-03-23';

drop table if  exists dwd_dim_activity_info;
create external table dwd_dim_activity_info(
    id string COMMENT '编号',
    activity_name string  COMMENT '活动名称',
    activity_type string  COMMENT '活动类型',
    start_time string  COMMENT '开始时间',
    end_time string  COMMENT '结束时间',
    create_time string  COMMENT '创建时间'
) COMMENT '活动信息表'
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_activity_info/'
tblproperties ("parquet.compression"="lzo");

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_activity_info partition(dt='2025-03-23')
select
    id,
    activity_name,
    activity_type,
    start_time,
    end_time,
    create_time
from ods_activity_info
where dt='2025-03-23';

drop table if  exists dwd_dim_base_province;
create external table dwd_dim_base_province (
    id string COMMENT 'id',
    province_name string COMMENT '省市名称',
    area_code string COMMENT '地区编码',
    iso_code string COMMENT 'ISO编码',
    region_id string COMMENT '地区id',
    region_name string COMMENT '地区名称'
) COMMENT '地区维度表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_base_province/'
tblproperties ("parquet.compression"="lzo");

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_base_province
select
    bp.id,
    bp.name,
    bp.area_code,
    bp.iso_code,
    bp.region_id,
    br.region_name
from
    (
        select * from ods_base_province
    ) bp
        join
    (
        select * from ods_base_region
    ) br
    on bp.region_id = br.id;