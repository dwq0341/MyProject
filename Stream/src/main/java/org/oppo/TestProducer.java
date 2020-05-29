package org.oppo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

public class TestProducer {
    public static void main(String[] args) throws Exception {
//        String topicName = "mytopic1";
//        String topicName = "testTopic03";
//        String topicName = "testTopic04";
        String topicName = "testTopic05";

        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.216.235:9094");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer<String, String>(props);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = dateFormat.format(System.currentTimeMillis());
//        File file = new File("D:\\tmp\\wordCount2.txt");
//        BufferedReader reader = new BufferedReader(new FileReader(file));
//        String tempStr = null;
        String str = "{\"time\":1587986867,\"timeStr\":\"2020-04-27T19:27:47+08:00\",\"direction\":\"in\",\"machine\":\"172.16.40.212\",\"machine_port\":9443,\"external_ip\":\"39.187.64.142\",\"external_port\":53354,\"attacker\":\"39.187.64.143\",\"victim\":\"172.16.40.212\",\"data\":\"rms.oppoer.cn/lib/ligerui/skins/gray/css/����/web-inf/web.xml\",\"incident_id\":\"084501df951c3a85dbf45e8257729cea-1587981663\",\"behave_uuid\":\"unix-socket-2043978185944066\",\"net\":{\"src_ip\":\"172.19.2.246\",\"src_port\":53354,\"dest_ip\":\"172.16.40.212\",\"dest_port\":9443,\"real_src_ip\":\"39.187.64.142\",\"proto\":\"TCP\",\"type\":\"http\",\"http\":{\"method\":\"GET\",\"protocol\":\"HTTP/1.1\",\"status\":404,\"url\":\"/lib/ligerUI/skins/Gray/css/\ufffd\ufffd\ufffd\ufffd/WEB-INF/web.xml\"}},\"geo_data\":{\"Country\":\"中国\",\"Province\":\"浙江\",\"City\":\"宁波\",\"Org\":\"\",\"Isp\":\"移动\",\"Latitude\":\"29\",\"Longitude\":\"121\",\"TimeZone\":\"Asia/Shanghai\",\"UTC\":\"UTC+8\",\"ChinaCode\":\"330200\",\"PhoneCode\":\"86\",\"ISO2\":\"CN\",\"Continent\":\"AP\"},\"threat\":{\"module\":\"tdps\",\"suuid\":\"DHb44f5d5862\",\"level\":\"attack\",\"name\":\"爆破敏感文件\",\"msg\":\"检测到爆破URL路径获取敏感文件路径\",\"severity\":1,\"type\":\"recon\",\"confidence\":60,\"is_apt\":0,\"status\":0,\"result\":\"failed\",\"tag\":[\"猜测敏感文件\"],\"success_by\":[\"url_file_exists\",\"resp_content_type_application/octet-stream\"],\"failed_by\":[\"http_status_3XX\",\"http_status_4XX\"],\"phase\":\"recon\",\"cap\":0,\"dest\":\"home_net\"},\"assets\":{\"id\":81,\"status\":1,\"name\":\"\",\"ip\":\"172.16.0.0/12\",\"section\":\"终端\",\"location\":\"\",\"level\":0,\"ext\":\"\",\"mac\":\"\",\"source\":\"manual\",\"update_time\":\"2020-02-19 21:34:39\",\"sub_type\":\"\"},\"dest_assets\":{\"id\":4919,\"status\":1,\"name\":\"172.19.2.246\",\"ip\":\"172.19.2.246\",\"section\":\"服务器\",\"location\":\"\",\"level\":0,\"ext\":\"\",\"mac\":\"\",\"source\":\"auto\",\"update_time\":\"2020-02-22 13:02:55\",\"sub_type\":\"loadbalance\"}}";
//        String str= "{\"behave_uuid\":\"unix-socket-74695993847764\",\"data\":\"global.asimov.events.data.trafficmanager.net\",\"machine_port\":64747,\"attacker\":\"\",\"timeStr\":\"2020-05-12T19:10:37+08:00\",\"incident_id\":\"\",\"machine\":\"10.241.247.190\",\"external_port\":0,\"victim\":\"\",\"geo_data\":{\"Org\":\"\",\"UTC\":\"\",\"Isp\":\"\",\"Latitude\":\"\",\"City\":\"未知\",\"Longitude\":\"\",\"Province\":\"未知\",\"PhoneCode\":\"\",\"ISO2\":\"\",\"TimeZone\":\"\",\"Continent\":\"\",\"Country\":\"未知\",\"ChinaCode\":\"\"},\"time\":1589281837,\"net\":{\"src_ip\":\"172.16.40.114\",\"src_port\":53,\"real_src_ip\":\"172.16.40.114\",\"dest_ip\":\"10.241.247.190\",\"proto\":\"UDP\",\"dns\":{\"rrname\":\"global.asimov.events.data.trafficmanager.net\",\"rdata\":\"skypedataprdcolcus05.cloudapp.net\",\"rcode\":\"NOERROR\",\"id\":24346,\"type\":\"answer\",\"ttl\":32,\"rrtype\":\"CNAME\"},\"http\":{},\"type\":\"dns\",\"dest_port\":64747},\"external_ip\":\"\",\"direction\":\"out\"}";
//        String str =  "{\"temp\":66, \"humidity\": 11, \"attacker\": \"39.187.64.143\",\"threat\":\"\"}";
//        String str = "50";
//        String[] str = {"19", "91", "92"};
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord(topicName, str));
//            i+=1;
            Thread.currentThread().sleep(5000);
            System.out.println("生成第==>>"+i+"==条数据"+"==日期==>>"+dateFormat.format(System.currentTimeMillis()));
        }
//        producer.send(new ProducerRecord(topic, str));

//        while ((tempStr = reader.readLine()) != null) {
//            producer.send(new ProducerRecord(topicName, tempStr));
//        }
//        reader.close();
            producer.close();
        }
    }