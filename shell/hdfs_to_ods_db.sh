#!/bin/bash

APP=gmall2
hive=/opt/module/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

sql1=" 
load data inpath '/origin_data/db2/order_info/$do_date' OVERWRITE into table ${APP}.ods_order_info partition(dt='$do_date');

load data inpath '/origin_data/db2/order_detail/$do_date' OVERWRITE into table ${APP}.ods_order_detail partition(dt='$do_date');

load data inpath '/origin_data/db2/sku_info/$do_date' OVERWRITE into table ${APP}.ods_sku_info partition(dt='$do_date');

load data inpath '/origin_data/db2/user_info/$do_date' OVERWRITE into table ${APP}.ods_user_info partition(dt='$do_date');

load data inpath '/origin_data/db2/payment_info/$do_date' OVERWRITE into table ${APP}.ods_payment_info partition(dt='$do_date');

load data inpath '/origin_data/db2/base_category1/$do_date' OVERWRITE into table ${APP}.ods_base_category1 partition(dt='$do_date');

load data inpath '/origin_data/db2/base_category2/$do_date' OVERWRITE into table ${APP}.ods_base_category2 partition(dt='$do_date');

load data inpath '/origin_data/db2/base_category3/$do_date' OVERWRITE into table ${APP}.ods_base_category3 partition(dt='$do_date'); 

load data inpath '/origin_data/db2/base_trademark/$do_date' OVERWRITE into table ${APP}.ods_base_trademark partition(dt='$do_date'); 

load data inpath '/origin_data/db2/activity_info/$do_date' OVERWRITE into table ${APP}.ods_activity_info partition(dt='$do_date'); 

load data inpath '/origin_data/db2/activity_order/$do_date' OVERWRITE into table ${APP}.ods_activity_order partition(dt='$do_date'); 

load data inpath '/origin_data/db2/cart_info/$do_date' OVERWRITE into table ${APP}.ods_cart_info partition(dt='$do_date'); 

load data inpath '/origin_data/db2/comment_info/$do_date' OVERWRITE into table ${APP}.ods_comment_info partition(dt='$do_date'); 

load data inpath '/origin_data/db2/coupon_info/$do_date' OVERWRITE into table ${APP}.ods_coupon_info partition(dt='$do_date'); 

load data inpath '/origin_data/db2/coupon_use/$do_date' OVERWRITE into table ${APP}.ods_coupon_use partition(dt='$do_date'); 

load data inpath '/origin_data/db2/favor_info/$do_date' OVERWRITE into table ${APP}.ods_favor_info partition(dt='$do_date'); 

load data inpath '/origin_data/db2/order_refund_info/$do_date' OVERWRITE into table ${APP}.ods_order_refund_info partition(dt='$do_date'); 

load data inpath '/origin_data/db2/order_status_log/$do_date' OVERWRITE into table ${APP}.ods_order_status_log partition(dt='$do_date'); 

load data inpath '/origin_data/db2/spu_info/$do_date' OVERWRITE into table ${APP}.ods_spu_info partition(dt='$do_date'); 

load data inpath '/origin_data/db2/activity_rule/$do_date' OVERWRITE into table ${APP}.ods_activity_rule partition(dt='$do_date'); 

load data inpath '/origin_data/db2/base_dic/$do_date' OVERWRITE into table ${APP}.ods_base_dic partition(dt='$do_date'); 
"

sql2=" 
load data inpath '/origin_data/db2/base_province/$do_date' OVERWRITE into table ${APP}.ods_base_province;

load data inpath '/origin_data/db2/base_region/$do_date' OVERWRITE into table ${APP}.ods_base_region;
"
case $1 in
"first"){
    $hive -e "$sql1$sql2"
};;
"all"){
    $hive -e "$sql1"
};;
esac
