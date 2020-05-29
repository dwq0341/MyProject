package org.example;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    public static void main(String[] args) {

//        String str = "Threatbook[380728]: {\"time\":1587986867,\"timeStr\":\"2020-04-27T19:27:47+08:00\",\"direction\":\"in\",\"machine\":\"172.16.40.212\",\"machine_port\":9443,\"external_ip\":\"39.187.64.142\",\"external_port\":53354,\"attacker\":\"39.187.64.142\",\"victim\":\"172.16.40.212\",\"data\":\"rms.oppoer.cn/lib/ligerui/skins/gray/css/����/web-inf/web.xml\",\"incident_id\":\"084501df951c3a85dbf45e8257729cea-1587981663\",\"behave_uuid\":\"unix-socket-2043978185944066\",\"net\":{\"src_ip\":\"172.19.2.246\",\"src_port\":53354,\"dest_ip\":\"172.16.40.212\",\"dest_port\":9443,\"real_src_ip\":\"39.187.64.142\",\"proto\":\"TCP\",\"type\":\"http\",\"http\":{\"method\":\"GET\",\"protocol\":\"HTTP/1.1\",\"status\":404,\"url\":\"/lib/ligerUI/skins/Gray/css/\ufffd\ufffd\ufffd\ufffd/WEB-INF/web.xml\"}},\"geo_data\":{\"Country\":\"中国\",\"Province\":\"浙江\",\"City\":\"宁波\",\"Org\":\"\",\"Isp\":\"移动\",\"Latitude\":\"29\",\"Longitude\":\"121\",\"TimeZone\":\"Asia/Shanghai\",\"UTC\":\"UTC+8\",\"ChinaCode\":\"330200\",\"PhoneCode\":\"86\",\"ISO2\":\"CN\",\"Continent\":\"AP\"},\"threat\":{\"module\":\"tdps\",\"suuid\":\"DHb44f5d5862\",\"level\":\"attack\",\"name\":\"爆破敏感文件\",\"msg\":\"检测到爆破URL路径获取敏感文件路径\",\"severity\":1,\"type\":\"recon\",\"confidence\":60,\"is_apt\":0,\"status\":0,\"result\":\"failed\",\"tag\":[\"猜测敏感文件\"],\"success_by\":[\"url_file_exists\",\"resp_content_type_application/octet-stream\"],\"failed_by\":[\"http_status_3XX\",\"http_status_4XX\"],\"phase\":\"recon\",\"cap\":0,\"dest\":\"home_net\"},\"assets\":{\"id\":81,\"status\":1,\"name\":\"\",\"ip\":\"172.16.0.0/12\",\"section\":\"终端\",\"location\":\"\",\"level\":0,\"ext\":\"\",\"mac\":\"\",\"source\":\"manual\",\"update_time\":\"2020-02-19 21:34:39\",\"sub_type\":\"\"},\"dest_assets\":{\"id\":4919,\"status\":1,\"name\":\"172.19.2.246\",\"ip\":\"172.19.2.246\",\"section\":\"服务器\",\"location\":\"\",\"level\":0,\"ext\":\"\",\"mac\":\"\",\"source\":\"auto\",\"update_time\":\"2020-02-22 13:02:55\",\"sub_type\":\"loadbalance\"}}";
//        String str = "Threatbook[380728]: {\"machine_port\":9443,\"net\":{\"src_ip\":\"172.19.2.246\"},\"direction\":\"in\"}";
//        str = str.substring(str.indexOf(":") + 1);
//        JSONObject json = JSONObject.parseObject(str);
//        System.out.println("json111=============>>>>"+json);
//        json.remove("net");
//        System.out.println("json222=============>>>>"+json);


//        Jedis jedis = new Jedis("localhost", 6379);
//        jedis.set("net.status", "status$$=$$304");
//        jedis.set("threat.confidence", "confidence$$>=$$60");
//        System.out.println(jedis.get("threat.confidence"));
//
//        jedis.lpush("site-list", "scala");
//        jedis.lpush("site-list", "java");
//        jedis.lpush("site-list", "python");

//        List<String> list = jedis.lrange("site-list",0, 3);
//        for (int i = 0; i < list.size(); i++) {
//            System.out.println("list.get(i)===========>>>>>"+list.get(i));
//        }

        String str = "confidence$$>=$$60";
        String str2 = "status$$=$$304";

//        String pattern = "(.*?)\\**(.*?)";
//        Pattern p = Pattern.compile(pattern);
//        Matcher mapcher = p.matcher(str);
//        if (mapcher.find()) {
//            String key = mapcher.group(0);
//            String val =mapcher.group(1);
//            System.out.println("key========>>>>>>: "+key);
//            System.out.println("value==========>>>>>: "+val);
//        }
//        String[] s = str2.split("\\$\\$");
//        int i = 11;
//        int j = 11;
//        if(i == j){
//            System.out.println("相等");
//        }
//        System.out.println("===========>>>>>> "+s[0]+"  "+s[1]+"  "+s[2]);
    }
}
