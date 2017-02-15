#数据库信息
HOSTNAME="10.10.2.15"
PORT="3306"
USERNAME="bi"
PASSWORD="mlw321@moretv"
offset=$1

#generate_mtv_user_overview_everyday
#产生汇总表的数据
generate_mtv_user_overview_everyday_sql="CALL bi.generate_mtv_user_overview_everyday($offset)"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} -e "${generate_mtv_user_overview_everyday_sql}"
