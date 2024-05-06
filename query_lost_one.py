import yaml
from pyhive import presto
import json
# 读取 YAML 文件
with open('env.yaml', 'r') as file:
    config = yaml.safe_load(file)

#不同环境的数据库
env = config['hotfix4.3']
#项目id
projectId=3
#env =config['hotfix4.2']
# 连接到 MySQL 数据库
conn = presto.connect(
    host=env['host'],
    port='8080',
)

# 创建一个游标对象
cursor = conn.cursor()
from datetime import datetime

timestamp_str = '2024-04-24 10:00:00'
timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

# 执行 SQL 查询
cursor.execute('SELECT * FROM kafka.ta."dwe-ta-distribute-topic-2" WHERE _message LIKE %s AND _message LIKE %s AND _timestamp > %s',
               ('%$part_event":"register%', '%"#account_id":"hermes-race-auto%', timestamp))
# 获取查询结果
results = cursor.fetchall()
list=[]

for row in results:
    # print(row)
    list.append(row[3])

user_list=[]
for item in list:
    data = json.loads(item)
    property_value = data['data']['#user_id']
    print(property_value) 
    user_list.append(property_value) 
print(user_list)    

# 执行 SQL 查询
# 执行查询
query = 'SELECT * FROM kafka.ta."hermes-triggered-task-notify" WHERE _message LIKE %s'
params = ('%"taskId":"1397"%',)
cursor.execute(query, params)
# 获取查询结果
results = cursor.fetchall()
list=[]

for row in results:
    # print(row)
    list.append(row[3])

user_list_except=[]
for item in list:
    data = json.loads(item)
    property_value = data['userId']
    print(property_value) 
    user_list_except.append(property_value) 
print(user_list_except) 
print(len(user_list_except))  
# 关闭游标和连接
cursor.close()
conn.close()

# 使用集合的差集操作找出不在第一个列表中的元素
not_in_list_1000 = set(user_list)-set(user_list_except)

# 打印结果
print("不在第一个列表中的元素:")
print(not_in_list_1000)
