drop table if exists ods_order_info;
create table if not exists ods_order_info (
  id bigint comment '编号',
  total_amount decimal(16,2) comment '总金额',
  order_status string comment '订单状态',
  user_id bigint comment '用户id',
  payment_way string comment '订单备注',
  out_trade_no string comment '订单交易编号（第三方支付用)',
  create_time string comment '创建时间',
  operate_time string comment '操作时间'
) comment '订单表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_order_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/order_info/2025-03-23' overwrite into table ods_order_info partition (dt='2025-03-23');

drop table if exists ods_order_detail;
create table if not exists ods_order_detail (
  id bigint comment '编号',
  order_id bigint comment '订单编号',
  user_id bigint comment '用户id',
  sku_id bigint comment 'sku_id',
  sku_name string comment 'sku名称（冗余)',
  img_url string comment '图片名称（冗余)',
  order_price decimal(10,2) comment '购买价格(下单时sku价格）',
  sku_num string comment '购买个数',
  create_time string comment '创建时间'
) comment '订单明细表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_order_detail/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/order_detail/2025-03-23' overwrite into table ods_order_detail partition (dt='2025-03-23');

drop table if exists ods_activity_info;
create table if not exists ods_activity_info (
  id bigint comment '活动id',
  activity_name string comment '活动名称',
  activity_type string comment '活动类型',
  activity_desc string comment '活动描述',
  start_time string COMMENT '开始时间',
  end_time string COMMENT '结束时间',
  create_time string COMMENT '创建时间'
) comment '活动表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_activity_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/activity_info/2025-03-23' overwrite into table ods_activity_info partition (dt='2025-03-23');

drop table if exists ods_activity_order;
create table if not exists ods_activity_order (
  `id` bigint comment '编号',
  `activity_id` bigint comment '活动id ',
  `order_id` bigint comment '订单编号',
  `create_time` string comment '发生日期'
) comment '活动与订单关联表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_activity_order/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/activity_order/2025-03-23' overwrite into table ods_activity_order partition (dt='2025-03-23');

drop table if exists ods_activity_rule;
create table if not exists ods_activity_rule (
  `id` bigint comment '编号',
  `activity_id` bigint comment '类型',
  `condition_amount` decimal(16,2) comment '满减金额',
  `condition_num` bigint comment '满减件数',
  `benefit_amount` decimal(16,2) comment '优惠金额',
  `benefit_discount` bigint comment '优惠折扣',
  `benefit_level` bigint comment '优惠级别'
) comment '优惠规则'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_activity_order/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/activity_rule/2025-03-23' overwrite into table ods_activity_rule partition (dt='2025-03-23');

drop table if exists ods_base_category1;
create table if not exists ods_base_category1 (
  `id` bigint comment '编号',
  `name` string comment '分类名称'
) comment '一级分类表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_base_category1/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/base_category1/2025-03-23' overwrite into table ods_base_category1 partition (dt='2025-03-23');

drop table if exists ods_base_category2;
create table if not exists ods_base_category2 (
  `id` bigint comment '编号',
  `name` string comment '二级分类名称',
  `category1_id` bigint comment '一级分类编号'
) comment '二级分类表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_base_category2/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/base_category2/2025-03-23' overwrite into table ods_base_category2 partition (dt='2025-03-23');

drop table if exists ods_base_category3;
create table if not exists ods_base_category3 (
  `id` bigint comment '编号',
  `name` string comment '三级分类名称',
  `category2_id` bigint comment '二级分类编号'
) comment '三级分类表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_base_category3/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/base_category3/2025-03-23' overwrite into table ods_base_category3 partition (dt='2025-03-23');

drop table if exists ods_base_dic;
create table if not exists ods_base_dic (
  `dic_code` string comment '编号',
  `dic_name` string comment '编码名称',
  `parent_code` string comment '父编号',
  `create_time` string comment '创建日期',
  `operate_time` string comment '修改日期'
)
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_base_dic/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/base_province/2025-03-23' overwrite into table ods_base_dic partition (dt='2025-03-23');

drop table if exists ods_base_province;
create table if not exists ods_base_province (
  `id` bigint comment 'id',
  `name` string comment '省名称',
  `region_id` string comment '大区id',
  `area_code` string comment '行政区位码',
  `iso_code` string comment '国际编码'
)
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_base_province/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/base_province/2025-03-23' overwrite into table ods_base_province partition (dt='2025-03-23');

drop table if exists ods_base_region;
create table if not exists ods_base_region (
  `id` string comment '大区id',
  `region_name` string comment '大区名称'
)
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_base_region/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/base_region/2025-03-23' overwrite into table ods_base_region partition (dt='2025-03-23');

drop table if exists ods_base_trademark;
create table if not exists ods_base_trademark (
  `tm_id` string comment '品牌id',
  `tm_name` string comment '品牌名称'
)
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_base_trademark/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/base_trademark/2025-03-23' overwrite into table ods_base_trademark partition (dt='2025-03-23');

drop table if exists ods_cart_info;
create table if not exists ods_cart_info (
  `id` bigint comment '编号',
  `user_id` bigint comment '用户id',
  `sku_id` bigint comment 'skuid',
  `cart_price` decimal(10,2) comment '放入购物车时价格',
  `sku_num` bigint comment '数量',
  `img_url` string comment '图片文件',
  `sku_name` string comment 'sku名称 (冗余)',
  `create_time` string comment '创建时间',
  `operate_time` string comment '修改时间',
  `is_ordered` bigint comment '是否已经下单',
  `order_time` string comment '下单时间',
  `source_type` string comment '来源类型',
  `source_id` bigint comment '来源编号'
) comment '购物车表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_cart_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/cart_info/2025-03-23' overwrite into table ods_cart_info partition (dt='2025-03-23');

drop table if exists ods_comment_info;
create table if not exists ods_base_trademark (
  `id` bigint comment '编号',
  `user_id` bigint comment '用户名称',
  `sku_id` bigint comment 'skuid',
  `spu_id` bigint comment '商品id',
  `order_id` bigint comment '订单编号',
  `appraise` string comment '评价 1 好评 2 中评 3 差评',
  `comment_txt` string comment '评价内容',
  `create_time` string comment '创建时间',
  `operate_time` string comment '修改时间'
) comment '商品评论表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_base_trademark/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/comment_info/2025-03-23' overwrite into table ods_base_trademark partition (dt='2025-03-23');

drop table if exists ods_coupon_info;
create table if not exists ods_coupon_info (
  `id` bigint comment '购物券编号',
  `coupon_name` string comment '购物券名称',
  `coupon_type` string comment '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
  `condition_amount` decimal(10,2) comment '满额数',
  `condition_num` bigint comment '满件数',
  `activity_id` bigint comment '活动编号',
  `benefit_amount` decimal(16,2) comment '减金额',
  `benefit_discount` bigint comment '折扣',
  `create_time` string comment '创建时间',
  `range_type` string comment '范围类型 1、商品 2、品类 3、品牌',
  `spu_id` bigint comment '商品id',
  `tm_id` bigint comment '品牌id',
  `category3_id` bigint comment '品类id',
  `limit_num` int comment '最多领用次数',
  `operate_time` string comment '修改时间',
  `expire_time` string comment '过期时间'
) comment '优惠券表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_coupon_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/coupon_info/2025-03-23' overwrite into table ods_coupon_info partition (dt='2025-03-23');

drop table if exists ods_coupon_use;
create table if not exists ods_coupon_use (
  `id` bigint comment '编号',
  `coupon_id` bigint comment '购物券ID',
  `user_id` bigint comment '用户ID',
  `order_id` bigint comment '订单ID',
  `coupon_status` string comment '购物券状态',
  `get_time` string comment '领券时间',
  `using_time` string comment '使用时间',
  `used_time` string comment '支付时间',
  `expire_time` string comment '过期时间'
) comment '优惠券领用表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_coupon_use/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/coupon_use/2025-03-23' overwrite into table ods_coupon_use partition (dt='2025-03-23');

drop table if exists ods_favor_info;
create table if not exists ods_favor_info (
  `id` bigint comment '编号',
  `user_id` bigint comment '用户名称',
  `sku_id` bigint comment 'skuid',
  `spu_id` bigint comment '商品id',
  `is_cancel` string comment '是否已取消 0 正常 1 已取消',
  `create_time` string comment '创建时间',
  `cancel_time` string comment '修改时间'
) comment '商品收藏表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_favor_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/favor_info/2025-03-23' overwrite into table ods_favor_info partition (dt='2025-03-23');

drop table if exists ods_order_refund_info;
create table if not exists ods_order_refund_info (
  `id` bigint comment '编号',
  `user_id` bigint comment '用户id',
  `order_id` bigint comment '订单编号',
  `sku_id` bigint comment 'skuid',
  `refund_type` string comment '退款类型',
  `refund_num` bigint comment '退货件数',
  `refund_amount` decimal(16,2) comment '退款金额',
  `refund_reason_type` string comment '原因类型',
  `refund_reason_txt` string comment '原因内容',
  `create_time` string comment '创建时间'
) comment '退单表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_order_refund_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/order_refund_info/2025-03-23' overwrite into table ods_order_refund_info partition (dt='2025-03-23');

drop table if exists ods_order_status_log;
create table if not exists ods_order_status_log (
  `id` bigint ,
  `order_id` bigint,
  `order_status` string,
  `operate_time` string
)
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_order_status_log/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/order_status_log/2025-03-23' overwrite into table ods_order_status_log partition (dt='2025-03-23');

drop table if exists ods_payment_info;
create table if not exists ods_payment_info (
  `id` bigint comment '编号',
  `out_trade_no` string comment '对外业务编号',
  `order_id` bigint comment '订单编号',
  `user_id` bigint comment '用户编号',
  `alipay_trade_no` string comment '支付宝交易流水编号',
  `total_amount` decimal(16,2) comment '支付金额',
  `subject` string comment '交易内容',
  `payment_type` string comment '支付方式',
  `payment_time` string comment '支付时间'
) comment '支付流水表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_payment_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/payment_info/2025-03-23' overwrite into table ods_payment_info partition (dt='2025-03-23');

drop table if exists ods_sku_info;
create table if not exists ods_sku_info (
  `id` bigint comment 'skuid(itemID)',
  `spu_id` bigint comment 'spuid',
  `price` decimal(10,0) comment '价格',
  `sku_name` string comment 'sku名称',
  `sku_desc` string comment '商品规格描述',
  `weight` decimal(10,2) comment '重量',
  `tm_id` bigint comment '品牌(冗余)',
  `category3_id` bigint comment '三级分类id（冗余)',
  `create_time` string comment '创建时间'
) comment '库存单元表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_sku_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/sku_info/2025-03-23' overwrite into table ods_sku_info partition (dt='2025-03-23');

drop table if exists ods_spu_info;
create table if not exists ods_spu_info (
  `id` bigint comment '商品id',
  `spu_name` string comment '商品名称',
  `description` string comment '商品描述(后台简述）',
  `category3_id` bigint comment '三级分类id',
  `tm_id` bigint comment '品牌id'
) comment '商品表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_spu_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/spu_info/2025-03-23' overwrite into table ods_spu_info partition (dt='2025-03-23');

drop table if exists ods_user_info;
create table if not exists ods_user_info (
  `id` bigint comment '编号',
  `name` string comment '用户姓名',
  `birthday` date comment '用户生日',
  `gender` string comment '性别 M男,F女',
  `email` string comment '邮箱',
  `user_level` string comment '用户级别',
  `create_time` string comment '创建时间'
) comment '支付流水表'
PARTITIONED BY (dt string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/user/hive/warehouse/dev_realtime_v1_xinyu_luo.db/ods/ods_user_info/' -- 指定数据在hdfs上的存储位置
tblproperties ("parquet.comperssion"="gzip");

load data inpath '/2207A/xinyu_luo/user_info/2025-03-23' overwrite into table ods_user_info partition (dt='2025-03-23');
