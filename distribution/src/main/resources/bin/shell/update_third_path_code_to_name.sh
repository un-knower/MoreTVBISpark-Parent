#数据库信息
HOSTNAME="10.10.2.15"
PORT="3306"
USERNAME="bi"
PASSWORD="mlw321@moretv"

Params=($@)
Length=${#Params[@]}
Day=${Params[1]}
TableName=${Params[Length-1]}
DayCN=`date -d "-1 day $Day" +%Y-%m-%d`

#update_third_path_name_everyday
#用于更新三级路径PV的名称
update_third_path_name_sql="UPDATE eagletv.$TableName SET name=(SELECT t.name FROM bi.mtv_list t WHERE t.parent_code = channel AND t.code = path LIMIT 1) WHERE day = '$DayCN';"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} -e "${update_third_path_name_sql}"

update_third_path_name_sql2="UPDATE eagletv.$TableName SET name=(SELECT t.name FROM bi.mtv_list t WHERE t.code = path LIMIT 1) WHERE day = '$DayCN' AND channel IN ('kids_home','sports');"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} -e "${update_third_path_name_sql2}"
