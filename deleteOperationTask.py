import json
import pymysql
import yaml


# 读取 YAML 文件
with open('env.yaml', 'r') as file:
    config = yaml.safe_load(file)

#不同环境的数据库
env = config['hotfix4.3']
#项目id
projectId=3
#任务状态: 0草稿 1运行中 2暂停中 3已结束
status=3
#accessToken
accessToken='0c6fa1a0-f5ef-4ceb-baf0-7159575fcf3c'
#env =config['hotfix4.2']
# 连接到 MySQL 数据库
conn = pymysql.connect(
    host=env['host'],
    user=env['username'],
    password=env['password'],
    database=env['database']
)

# 创建一个游标对象
cursor = conn.cursor()

# 执行 SQL 查询

cursor.execute(f'SELECT t.task_id FROM hermes.hermes_operation_task t WHERE status ={status} and project_id={projectId} and deleted =0')

# 获取查询结果
results = cursor.fetchall()
list=[]
for row in results:
    print(row)
    list.append(row[0])
print(list)    
# 关闭游标和连接
cursor.close()
conn.close()
import requests

for i in list:
    
    delete_url = env['url']+f'/v1/hermes/operationTask/delete?projectId={projectId}&taskIds={i}'  # 替换为您要发送请求的URL
    operating_url= env['url']+f'/v1/hermes/operationTask/end?projectId={projectId}'
    print(delete_url)
    delete_data = {
        "projectId": projectId
    }
    operating_data={
        "taskId": str(i),
        "projectId": projectId
    }
    operating_data = json.dumps(operating_data)
    delete_data = json.dumps(delete_data)
    headers = {
        'User-Agent': 'Mozilla/5.0',  # 自定义User-Agent头
        'Content-Type': 'application/json'  ,
        'Td-App' : 'hermes',
        'Authorization': f'bearer {accessToken}'# 自定义Content-Type头
    }
    response = requests.post(operating_url, data=operating_data, headers=headers)
    response = requests.post(delete_url, data=delete_data, headers=headers)

    # 检查响应状态码
    if response.status_code == 200:
        # 请求成功
        print('请求成功')
        print('响应内容:', response.text)
    else:
        # 请求失败
        print('请求失败')
        print('错误码:', response.status_code)
      
    

