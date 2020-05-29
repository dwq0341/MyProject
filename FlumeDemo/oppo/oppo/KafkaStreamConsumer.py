# encoding:utf-8
import operator

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import matplotlib.pyplot as plt
import pandas as pd

def kafkaConsumeUtil():
    consumer = KafkaConsumer(
        # "testTopic02",
        # auto_offset_reset='latest',
        group_id="udp_tcp_group",
        bootstrap_servers=['172.16.216.235:9094']
    )
    topicName = 'DEST_IP_COUNT04'
    start = 812650  # 800000 -> 812650
    partition = TopicPartition(topic=topicName, partition=0)
    consumer.assign([partition])
    consumer.seek(partition=partition, offset=start)
    return consumer

#消费者
def readMessage(consumer):
    destIpDict = dict()
    minDict = dict()
    end = 904665
    for message in consumer:
        if message.offset > end:
            break
        else:
            # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
            value = message.value.decode('utf-8')
            key = message.key.decode('utf-8')
            newValueDict = eval(value)
            newValue = newValueDict['DESTIP_CNT']
            #不超过10位大小,放另一个字典
            if newValue <= 1000:
                l = list()
                minDict[key] = l.append(newValue)
            else:
                destIpDict[key] = list()
                if key in destIpDict:
                    destIpDict[key].append(newValue)
            # 写文件
            dataFrameToWps(destIpDict)
            # 排序
            sortDestIp = sorted(destIpDict.items(), key=lambda x:x[1], reverse=False)
            newA = dict(sorted(destIpDict.items(), key=operator.itemgetter(1), reverse=True)[:15])
            destIp = dict(newA)
            # 画图
            matplotlib(destIp)
plt.pause(0) #不关闭图像

# 画图
def matplotlib(destIp):
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
    plt.rcParams['axes.unicode_minus'] = False  # 解决保存图像时符号-显示为方块的2问题
    # fig, ax = plt.subplots()
    x = list(destIp.keys())
    y = list(destIp.values())
    # plt.figure(figsize=(10, 5))
    plt.clf()
    plt.plot(x, y, 'o-')  # 画图,以点线连接的方式显示
    plt.title('dest_ip 统计')
    plt.xlabel('Ip')
    plt.ylabel('数量')
    plt.xticks(x, x, rotation=30)
    for k, v in destIp.items():
        plt.annotate(v[0], xy=(k, v[0]))
    # plt.xlim([0, 10000000])
    plt.draw()
    plt.pause(0.1)

# dataFrame写表格
def dataFrameToWps(destIpDict):
    frame = pd.DataFrame(destIpDict)
    frame.to_excel('D:\destIp.xlsx', encoding='utf-8', sheet_name='data')

if __name__ == '__main__':
    consume = kafkaConsumeUtil()
    readMessage(consume)