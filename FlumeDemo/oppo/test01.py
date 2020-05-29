#pro.sources.s1.type = exec
#pro.sources.s1.command = tail -F D:\log
# encoding: utf-8

import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pykafka import KafkaClient

# producer = KafkaProducer(bootstrap_servers='172.16.216.235:9094')
# producer.send('testTopic', "msg")
# for i in range(6):
#   msg = "msg %d" % i
#     print(i)
#     producer.send('testTopic', i)

# msg_dict = {
#     "operatorId":"test",
#     "terminalId":"123",
#     "terminalNo":"1"
# }
# msg = json.dumps(msg_dict).encode()
# producer.send("test1", msg)
# producer.close()
# print("结束")


# client = KafkaClient(hosts="172.16.216.235:9094")
# print(client.topics)
# print(client.brokers)

# 创建字典
# dict = {}
# dict['id'] = 'cc695906217'
# dict['name'] = '种冲'
# 打印字典
# print(dict)
# 保存为json文件
# with open('dict.json','w',encoding='utf-8') as f:
#     f.write((str)(dict))
# 加载json文件
# with open('dict.json','r',encoding='utf-8') as f:
#     s = f.read()
#     print(s)
#     js = json.loads(json.dumps(eval(s)))
#     print(js['name'])

#list 去重
myList = [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8]
s = list(set(myList))
print(s)


