# encoding:utf-8

import re
from pykafka import KafkaClient

class Topic():
    def __init__(self, topic_name, num_partitions=1, replication_factor=1, replica_assignment=[], config_entries=[]):
        """
        :param topic_name:
        :param num_partitions:
        :param replication_factor:
        :param replica_assignment:
        :param cofing_entries:
        """
        self.topic_name = topic_name.encode()
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.replica_assignment = replica_assignment
        self.config_entries = config_entries

client = KafkaClient(hosts="172.16.216.235:9094")
print(client.topics)
for i in client.brokers.values():
    try:
        i.create_topics(topic_reqs=(Topic("users"),))
    except Exception as e:
        print(e)
        # if re.search("41", str(e)):
        #     print("该broker 不是 leader，交由下一个broker创建")
        # elif re.search("7", str(e)):
        #     print("创建完成")
        #     break
        # elif re.search("36", str(e)):
        #     print("topic 已存在")
        #     break
        # else:
        #     raise e
