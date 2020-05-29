# encoding:utf-8

import json
import re

# <1>2020-04-27T18:51:44+08:00 tdp-oppo-1 Threatbook[366830]: {"time":1587984704,"timeStr":"2020-04-27T18:51:44+08:00","direction":"out","machine":"172.18.28.209","machine_port":52109,"external_ip":"","external_port":0,"attacker":"","victim":"","data":"nimap.oppo.com","incident_id":"","behave_uuid":"unix-socket-1206047102889409","net":{"src_ip":"172.18.28.209","src_port":52109,"dest_ip":"172.16.40.114","dest_port":53,"real_src_ip":"172.18.28.209","proto":"UDP","type":"dns","dns":{"type":"query","id":30074,"rrname":"nimap.oppo.com","rrtype":"A"},"http":{}},"geo_data":{"Country":"未知","Province":"未知","City":"未知","Org":"","Isp":"","Latitude":"","Longitude":"","TimeZone":"","UTC":"","ChinaCode":"","PhoneCode":"","ISO2":"","Continent":""},"threat":null,"assets":{"id":81,"status":1,"name":"","ip":"172.16.0.0/12","section":"终端","location":"","level":0,"ext":"","mac":"","source":"manual","update_time":"2020-02-19 21:34:39","sub_type":""},"dest_assets":{"id":86,"status":1,"name":"172.16.40.114","ip":"172.16.40.114","section":"服务器","location":"","level":0,"ext":"","mac":"","source":"auto","update_time":"2020-02-20 10:08:18","sub_type":"dns"}}

# 字符串
str = '{\"time\":1587986867,\"timeStr\":\"2020-04-27T19:27:47+08:00\",\"direction\":\"in\",\"machine\":\"172.16.40.212\",\"machine_port\":9443,\"external_ip\":\"39.187.64.142\",\"external_port\":53354,\"attacker\":\"39.187.64.142\",\"victim\":\"172.16.40.212\",\"data\":\"rms.oppoer.cn/lib/ligerui/skins/gray/css/����/web-inf/web.xml\",\"incident_id\":\"084501df951c3a85dbf45e8257729cea-1587981663\",\"behave_uuid\":\"unix-socket-2043978185944066\",\"net\":{\"src_ip\":\"172.19.2.246\",\"src_port\":53354,\"dest_ip\":\"172.16.40.212\",\"dest_port\":9443,\"real_src_ip\":\"39.187.64.142\",\"proto\":\"TCP\",\"type\":\"http\",\"http\":{\"method\":\"GET\",\"protocol\":\"HTTP/1.1\",\"status\":404,\"url\":\"/lib/ligerUI/skins/Gray/css/\ufffd\ufffd\ufffd\ufffd/WEB-INF/web.xml\"}},\"geo_data":{\"Country\":\"中国\",\"Province\":\"浙江\",\"City\":\"宁波\",\"Org\":\"\",\"Isp\":\"移动\",\"Latitude\":\"29\",\"Longitude\":\"121\",\"TimeZone\":\"Asia/Shanghai\",\"UTC\":\"UTC+8\",\"ChinaCode\":\"330200\",\"PhoneCode\":\"86\",\"ISO2\":\"CN\",\"Continent\":\"AP\"},\"threat\":{\"module\":\"tdps\",\"suuid\":\"DHb44f5d5862\",\"level\":\"attack\",\"name\":\"爆破敏感文件\",\"msg\":\"检测到爆破URL路径获取敏感文件路径\",\"severity\":1,\"type\":\"recon\",\"confidence\":60,\"is_apt\":0,\"status\":0,\"result\":\"failed\",\"tag\":[\"猜测敏感文件\"],\"success_by\":[\"url_file_exists\",\"resp_content_type_application/octet-stream\"],\"failed_by\":[\"http_status_3XX\",\"http_status_4XX\"],\"phase\":\"recon\",\"cap\":0,\"dest\":\"home_net\"},\"assets\":{\"id\":81,\"status\":1,\"name\":\"\",\"ip\":\"172.16.0.0/12\",\"section\":\"终端\",\"location\":\"\",\"level\":0,\"ext\":\"\",\"mac\":\"\",\"source\":\"manual\",\"update_time\":\"2020-02-19 21:34:39\",\"sub_type\":\"\"},\"dest_assets\":{\"id\":4919,\"status\":1,\"name\":\"172.19.2.246\",\"ip\":\"172.19.2.246\",\"section\":\"服务器\",\"location\":\"\",\"level\":0,\"ext\":\"\",\"mac\":\"\",\"source\":\"auto\",\"update_time\":\"2020-02-22 13:02:55\",\"sub_type\":\"loadbalance\"}}'

# s = str.split(':', 1)[1]
# print("s==========>>>>>>", s)

pattern = re.compile(r'([a-z]+).*?\[(\d+)\]:\s(\{.*\})', re.I)
pattern2 = re.compile(r'.*(:).*')
# pattern2 = re.compile('(\[.*?\])')
result = pattern2.findall(str)
print(result)

# for i in result[0]:
#     print("匹配:", i)
# temp = ('Threatbook', '380728', '{"time":1587986867,"timeStr":"2020-04-27T19:27:47+08:00","direction":"in","machine":"172.16.40.212","machine_port":9443,"external_ip":"39.187.64.142","external_port":53354,"attacker":"39.187.64.142","victim":"172.16.40.212","data":"rms.oppoer.cn/lib/ligerui/skins/gray/css/����/web-inf/web.xml","incident_id":"084501df951c3a85dbf45e8257729cea-1587981663","behave_uuid":"unix-socket-2043978185944066","net":{"src_ip":"172.19.2.246","src_port":53354,"dest_ip":"172.16.40.212","dest_port":9443,"real_src_ip":"39.187.64.142","proto":"TCP","type":"http","http":{"method":"GET","protocol":"HTTP/1.1","status":404,"url":"/lib/ligerUI/skins/Gray/css/����/WEB-INF/web.xml"}},"geo_data":{"Country":"中国","Province":"浙江","City":"宁波","Org":"","Isp":"移动","Latitude":"29","Longitude":"121","TimeZone":"Asia/Shanghai","UTC":"UTC+8","ChinaCode":"330200","PhoneCode":"86","ISO2":"CN","Continent":"AP"},"threat":{"module":"tdps","suuid":"DHb44f5d5862","level":"attack","name":"爆破敏感文件","msg":"检测到爆破URL路径获取敏感文件路径","severity":1,"type":"recon","confidence":60,"is_apt":0,"status":0,"result":"failed","tag":["猜测敏感文件"],"success_by":["url_file_exists","resp_content_type_application/octet-stream"],"failed_by":["http_status_3XX","http_status_4XX"],"phase":"recon","cap":0,"dest":"home_net"},"assets":{"id":81,"status":1,"name":"","ip":"172.16.0.0/12","section":"终端","location":"","level":0,"ext":"","mac":"","source":"manual","update_time":"2020-02-19 21:34:39","sub_type":""},"dest_assets":{"id":4919,"status":1,"name":"172.19.2.246","ip":"172.19.2.246","section":"服务器","location":"","level":0,"ext":"","mac":"","source":"auto","update_time":"2020-02-22 13:02:55","sub_type":"loadbalance"}')

# dict = result[0][2]
# print(dict)
# print(type(dict))
#
# # print(dict['time'])
# json = json.loads(dict)
# print(type(json))
#
# print(json['data'])