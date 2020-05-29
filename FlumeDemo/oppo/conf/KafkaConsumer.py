# encoding:utf-8

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import json
#消费者
#auto_offset_reset='latest'
#enable_auto_commit=False
consumer = KafkaConsumer(
    # "testTopic",
    group_id="test_group",
    bootstrap_servers=['172.16.216.235:9094'],
    # value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
#消费多个主题
#consumer.subscribe(topics=('', ''))

#获取组题列表
print("topics:", consumer.topics())

#获取topic所有的partition
print("topic所有的分区:", consumer.partitions_for_topic(topic="testTopic"))

#获取当前消费者订阅的组题
# print("subscription:", consumer.subscription())

#获取当前消费者topic、分区信息
# print("获取当前消费者topic、分区信息:", consumer.assignment())

##获取当前消费者可消费的偏移量
# consumer.beginning_offsets(consumer.assignment())

#获取当前组题分区的偏移量
# print("获取当前组题分区的偏移量:", consumer.position(TopicPartition(topic='testTopic', partition=0)))

# partition = TopicPartition(topic='testTopic', partition=0)

tps = [TopicPartition(topic='testTopic', partition=p) for p in consumer.partitions_for_topic(topic="testTopic")]
print("tps=====>>>", tps)

print("每个分区的最新offset:", consumer.end_offsets(tps))

for i in range(len(tps)):
    print("每个分区消费到的位置:", consumer.position(tps[i]))

#为consumer分配分区
# partition=TopicPartition(topic='testTopic', partition=0)
# consumer.assign([partition])
# consumer.seek(partition, offset=396461)
#使用seek()设置偏移量
consumer.seek(partition=TopicPartition(topic='testTopic', partition=0), offset=98)

#获取分区最后的offset
# tp=TopicPartition(topic='testTopic', partition=0)
# print(consumer.end_offsets(tp))
# try:
#     for message in consumer:
#         print("offset:", message.offset)
#         print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
# except KafkaError as e:
#     print(e)
# finally:
#     consumer.close()