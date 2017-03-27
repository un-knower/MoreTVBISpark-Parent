//通过ansible下发依赖的lib
1.通过fliezilla上传lib到ftp上
2.登录到管理机上 23：bigdata-extsvr-sys-manager
3.选用spark用户
4.切换目录
cd /data/tools/ansible/modules/azkaban/playbook
5.获取上传的lib.zip
ansible all -i azkaban_server.host -mshell -a "cd /data/apps/azkaban/medusa/;rm -rf lib"

ansible all -i azkaban_server.host -mshell -a "cd /data/apps/azkaban/medusa/;python /data/tscripts/scripts/ftp.py -s get -f lib.zip"

ansible all -i azkaban_server.host -mshell -a "cd /data/apps/azkaban/medusa/;unzip lib.zip"