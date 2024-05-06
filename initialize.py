import json
import pymysql
import requests
from datetime import datetime,timedelta

import yaml


# 读取 YAML 文件
with open('env.yaml', 'r') as file:
    config = yaml.safe_load(file)

#不同环境的数据库
env = config['hotfix4.3']
#不同环境的URL
env_url = env['url']
#项目id
projectId=3

login_url=env_url+'/v1/oauth/loginForToken'

login_data={
    'loginName':'root',
    'enPassword':'ICq6sK1S0PugLHPwhNjJdRyk2X9L72MZGa9+mQ1A8Y3z3FUutwh4ddzD+bCsRkzR0bC5C5jAUePspO3ZPohFsf0MWpBGfwspjr7S0RBDZqXmyg0d4/iqRi/NRQJu2Q/f0A+O1+QIaAlMseJLWDtyWFFX7itavNXHNPCu3kTehv8='
}

login_response = requests.post(login_url, data=login_data)

if login_response.status_code == 200:
    # 请求成功
    print('请求成功')
    print('响应内容:', login_response.text)
else:
    # 请求失败
    print('请求失败')
    print('错误码:', login_response.status_code)

#accessToken
accessToken=login_response.json()["data"]['oAuthLoginInfo']['accessToken']

print(accessToken)
    
channel_url=env_url+f'/v1/hermes/channel/add?projectId={projectId}'
    
channel_data={
  "channelName": "自动创建的通道",
  "channelType": 1,
  "pushIdType": "user:#account_id",
  "touchEventSource": "custom",
  "channelSubBizType": "webhook",
  "config": "{\"url\":\"http://localhost:8999/v1/hermes/webhook/test/path99\",\"authConfig\":{},\"paramsList\":[{\"key\":\"title\",\"keyName\":\"标题\",\"type\":\"STRING\",\"required\":0}],\"userParamsList\":[]}",
  "enableTouchEvent": 0,
  "eventDeliveryName": "",
  "eventClickName": "",
  "projectId": projectId
}

channel_data=json.dumps(channel_data)

headers = {
    'User-Agent': 'Mozilla/5.0',  # 自定义User-Agent头
    'Content-Type': 'application/json'  ,
    'Td-App' : 'hermes',
    'Authorization': f'bearer {accessToken}'# 自定义Content-Type头
}

response = requests.post(channel_url, data=channel_data, headers=headers)

# 检查响应状态码
if response.status_code == 200:
    # 请求成功
    print('请求成功')
    print('响应内容:', response.text)
else:
    # 请求失败
    print('请求失败')
    print('错误码:', response.status_code)
    
channel_id=response.json()["data"]['channelId']
    
channel_status_url=env_url+f'/v1/hermes/channel/status/update?projectId={projectId}'
    
channel_status_data={
  "channelId": channel_id,
  "status": 1,
  "projectId": projectId
}

channel_status_data=json.dumps(channel_status_data)

response = requests.post(channel_status_url, data=channel_status_data, headers=headers)

# 检查响应状态码
if response.status_code == 200:
    # 请求成功
    print('请求成功')
    print('响应内容:', response.text)
else:
    # 请求失败
    print('请求失败')
    print('错误码:', response.status_code)      
    
task_url=env_url+f'/v1/hermes/operationTask/approval/saveAndSubmit?projectId={projectId}'
    


# 获取当前时间
current_time = datetime.now()

# 打印当前时间
print(current_time)   
# 计算一天后的时间
one_day = timedelta(days=1)
one_day_later = current_time + one_day

# 打印一天后的时间
print(one_day_later)

# 原始的 JSON 字符串
json_str = '[{"periodStart":"2024-05-06 16:01","periodEnd":"2024-05-17 16:02","periodTimeSymbol":"TS02","dayStartTime":null,"startDay":null,"zoneoffset":8,"events":[{"key":"4jiKU_zJ","eventName":"register","eventDesc":"用户注册","eventType":"event","taPropQuota":{"analysis":"A200","analysisDesc":"次数","analysisParams":"","quota":"","quotaDesc":""},"op":"gt","uceCalcuSymbol":"C030","count":1,"filts":[],"relation":1,"num":"1","bubbleInfo":{"bubbleType":"custom_event_props","id":264},"isDisPlay":1,"realAvailable":true}]}]'

# 解析 JSON 字符串
data = json.loads(json_str)

# 获取当前时间
current_time = datetime.now()

# 计算一天后的时间
one_day = timedelta(days=1)
one_day_later = current_time + one_day

# 替换时间字段
data[0]["periodStart"] = current_time.strftime("%Y-%m-%d %H:%M")
data[0]["periodEnd"] = one_day_later.strftime("%Y-%m-%d %H:%M")

# 转换回 JSON 字符串
updated_json_str = json.dumps(data)

# 打印更新后的 JSON 字符串
print(updated_json_str)
 
task_data={
  "realtime": 1,
  "clusterRefresh": None,
  "clusterRefreshTime": None,
  "triggerMixQpVersion": "4.3",
  "triggerType": 3,
  "triggerTimeStrategy": "fixed_time_zone",
  "frequencyLimits": "{\"enableFrequencyLimits\":false}",
  "doNotDisturb": None,
  "pushDelay": None,
  "triggerRule": updated_json_str,
  "timeoutControl": None,
  "sceneId": None,
  "startDate": f"{current_time}",
  "endDate": f"{one_day_later}",
  "taskName": "自动创建触发式完成A",
  "groupId": 0,
  "channelType": 1,
  "channelId": channel_id,
  "enableChannelTouchLimits": False,
  "targetClusterType": 1,
  "groupContentList": [
    {
      "expGroupType": 1,
      "expGroupId": "default",
      "expGroupName": "",
      "order": "",
      "contentList": [
        {
          "pushLanguageCode": "default",
          "content": [
            {
              "key": "title",
              "type": "STRING",
              "required": 0,
              "name": "标题"
            }
          ]
        }
      ]
    }
  ],
  "expConfig": {
    "enableExp": False
  },
  "tzOffset": 8,
  "clusterKey": None,
  "qp": "{\"totalCFilter\":{\"relation\":\"1\",\"filts\":[{\"relation\":\"1\",\"filts\":[{\"conditionType\":\"user\",\"userCondition\":{\"calcuSymbol\":\"C04\",\"columnDesc\":\"账户ID\",\"columnIndex\":-1,\"columnName\":\"#account_id\",\"columnType\":\"varchar\",\"ftv\":[],\"selectType\":\"string\",\"tableType\":\"1\",\"subTableType\":\"\",\"timeRelative\":\"\",\"timeUnit\":\"\",\"clusterDatePolicy\":null,\"clusterDatePolicyConfigurable\":false,\"specifiedClusterDate\":\"2024-05-06\"}}]}]}}",
  "completionIndicatorDef": "{\"completionIndicators\":[{\"completionIndicatorType\":0,\"name\":\"\",\"touch_cycle_num\":1,\"touch_cycle_num_unit\":\"day\",\"event\":{\"eventName\":\"level_up\",\"eventDesc\":\"升级事件\",\"eventType\":\"event\",\"relation\":1,\"key\":\"IQ-7AApv\",\"taPropQuota\":{\"analysis\":\"A200\",\"analysisDesc\":\"次数\",\"analysisParams\":\"\",\"quota\":\"\",\"quotaDesc\":\"\"},\"op\":\"gt\",\"uceCalcuSymbol\":\"C03\",\"count\":0,\"filts\":[],\"num\":\"0\",\"bubbleInfo\":{\"displayName\":\"battle\",\"name\":\"battle\",\"bubbleType\":\"custom_event_props\",\"id\":264,\"desc\":\"\"},\"realAvailable\":true,\"isDisPlay\":1}}]}",
  "metricMap": None,
  "projectId": projectId
}

task_data=json.dumps(task_data)

response = requests.post(task_url, data=task_data, headers=headers)

# 检查响应状态码
if response.status_code == 200:
    # 请求成功
    print('请求成功')
    print('响应内容:', response.text)
else:
    # 请求失败
    print('请求失败')
    print('错误码:', response.status_code)  
          
task_id=response.json()['data'] 
print(task_id)    
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

cursor.execute(f'select creator from hermes.hermes_operation_task where hermes_operation_task.task_id={task_id}')

# 获取查询结果
results = cursor.fetchall()
print(results) 
# 关闭游标和连接
cursor.close()
conn.close()

approver_url=env_url+f'/v1/hermes/operationTask/approver/batchAdd?projectId={projectId}'
    
approver_data={
  "approvers": [
    f"{results[0][0]}"
  ],
  "projectId": projectId
}

approver_data=json.dumps(approver_data)

headers = {
    'User-Agent': 'Mozilla/5.0',  # 自定义User-Agent头
    'Content-Type': 'application/json'  ,
    'Td-App' : 'hermes',
    'Authorization': f'bearer {accessToken}'# 自定义Content-Type头
}

response = requests.post(approver_url, data=approver_data, headers=headers)

# 检查响应状态码
if response.status_code == 200:
    # 请求成功
    print('请求成功')
    print('响应内容:', response.text)
else:
    # 请求失败
    print('请求失败')
    print('错误码:', response.status_code)

