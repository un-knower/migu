#!/bin/bash

# 有如下三种调用方式：
# sh TB_SA_Cache.sh
# sh TB_SA_Cache.sh ifp_source 330000
# sh TB_SA_Cache.sh ifp_source 330000 20180417

Store_Version="ifp_source"
if [ "$1" != "" ]; then
  Store_Version="$1"
fi

PT_prov="330000"
if [ "$2" != "" ]; then
  PT_prov="$2"
fi

PT_CACHE_DATE=$(date +%Y%m%d -d"1 day ago")
if [ "$3" != "" -a ${#3} = 8 ]; then
  PT_CACHE_DATE="$3"
fi

hive -e "
LOAD DATA INPATH '/${Store_Version}/DataCenter/SA/TB_SA_Cache_small/${PT_CACHE_DATE}/${PT_prov}/*/data/store/*.orc' INTO TABLE ${Store_Version}.TB_SA_Cache_small PARTITION (ds='${PT_CACHE_DATE}', prov='${PT_prov}');

LOAD DATA INPATH '/${Store_Version}/DataCenter/SA/TB_SA_Cache_big/${PT_CACHE_DATE}/${PT_prov}/*/data/store/*.orc' INTO TABLE ${Store_Version}.TB_SA_Cache_big PARTITION (ds='${PT_CACHE_DATE}', prov='${PT_prov}');
"

