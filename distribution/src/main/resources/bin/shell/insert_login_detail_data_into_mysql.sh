#数据库信息
HOSTNAME="10.10.2.15"
PORT="3306"
USERNAME="bi"
PASSWORD="mlw321@moretv"
offset=$1

#generate_login_detail_data
#产生汇总表的数据
generate_login_detail_data_sql="CALL bi.generate_login_detail_data_everyday($offset)"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} -e "${generate_login_detail_data_sql}"
