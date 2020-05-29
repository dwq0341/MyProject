package org.oppo;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.Logger;
import org.jdbc.GetConnUtil;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaStreamToMysql {
    private static Logger log = Logger.getLogger(KafkaStreamToMysql.class);

    public static void main(String[] args) {
        String topicName = "testTopic02";
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-alart-flow");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.216.235:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> jsonStream = builder.stream("testTopic04");
        KStream<String, String> jsonStream06 = builder.stream("testTopic06");
        jsonStream.flatMapValues(value -> {
            List<Map> list = new ArrayList<>();
            try {
                Map maps = alarmLine(value);
                list.add(maps);
            } catch (Exception exception) {
                System.out.println("=====告警离线解析异常=====>>>>>"+exception.getMessage());
                log.error("=====告警离线解析异常=====>>>>>"+exception.getMessage());
            }
            return list == null ? new ArrayList<>() : list;
        }).filter((key, value) -> {
            Boolean flag = filter(value);
            return flag;
        }).map((k, v) -> new KeyValue<>(k, v.toString()))
                .to(topicName, Produced.with(Serdes.String(), Serdes.String()));

        //流量stream
        jsonStream06.flatMapValues(
                value -> {
                    List<Map> list = new ArrayList<>();
                    try {
                        Map maps = flowLine(value);
                        list.add(maps);
                    } catch (Exception exception) {
                        System.out.println("=====流量离线解析异常=====>>>>>"+exception.getMessage());
                        log.error("=====流量离线解析异常=====>>>>>"+exception.getMessage());
                    }
                    return list == null ? new ArrayList<>() : list;
                }).map(((key, value) -> new KeyValue<>(key, value.toString())))
                .print(Printed.toSysOut());

        KafkaStreams stream = new KafkaStreams(builder.build(), props);
        stream.start();
    }

    /**
     * 条件过滤
     * @param value
     * @return
     */
    public static Boolean filter(Map value) {
        final String http = "http";
        final String dns = "dns";
        //redis
        Jedis jedis = new Jedis("localhost", 6379);
//        jedis.auth("123456");
        String[] statusArr = jedis.get("net.status").split("\\$\\$");
        String[] confidenceArr = jedis.get("threat.confidence").split("\\$\\$");

        //告警流量与一般流量过滤规则不同,应根据消息类型获取规则
        List<String[]> list = new ArrayList<>();
        String type = String.valueOf(value.get("type"));
        if (http.equals(type)) {
            String confidence = String.valueOf(value.get("confidence"));
            String status = String.valueOf(JSONObject.parseObject(String.valueOf(value.get("http"))).get("status"));

            String[] newConfidence = new String[confidenceArr.length+1];
            System.arraycopy(confidenceArr, 0, newConfidence, 0, confidenceArr.length);
            newConfidence[3] = confidence;

            String[] newStatus = new String[statusArr.length+1];
            System.arraycopy(statusArr, 0, newStatus, 0, statusArr.length);
            newStatus[3] = status;

            list.add(newStatus);
            list.add(newConfidence);
        }
        if (dns.equals(type)) {

        }
        //规则匹配
        Map maps = ruleVerify(list, type);
        if (maps == null || maps.size() == 0) { return false;}

        Boolean flag = false;
        if (http.equals(type)) {
            Boolean ruleConfide = Boolean.valueOf(maps.get("confidence").toString());
            Boolean ruleStatus = Boolean.valueOf(maps.get("status").toString());
            //为true, 消息写入Topic
            if (ruleConfide == true && ruleStatus == true) {
                flag = true;
            }
        }
        if (dns.equals(type)) {

        }
        return flag;
    }

    /**
     * 规则校验
     * @param lists
     * @param type
     * @return
     */
    public static Map ruleVerify(List<String[]> lists, String type) {
        if (lists == null || lists.size() == 0) { return null;}

        final String http = "http";
        final String dns = "dns";
        Boolean flag = false;
        Map maps = new HashMap<>();
        for (String[] arrs: lists) {
            String key = arrs[0];
            String symbol = arrs[1];
            int left = Integer.parseInt(arrs[2]);  //redis固定值
            int right = Integer.parseInt(arrs[3]);
            if (http.equals(type)) {
                switch (symbol) {
                    case ">":
                        flag = right >= left == true ? true : false;
                        break;
                    case "<":
                        break;
                    case "=":
                        flag = right == left == true ? true : false;
                        break;
                    case ">=":
                        break;
                    case "<=":
                        break;
                    default:
                }
            }
            if (dns.equals(type)) {
                switch (symbol) {
                    case ">":
                        break;
                    case "<":
                        break;
                    case "=":
                        break;
                    case ">=":
                        break;
                    case "<=":
                        break;
                    default:
                }
            }
            maps.put(key, flag);
        }
        return maps;
    }

    /**
     * 解析告警数据入库
     * @param value
     */
    public static JSONObject alarmLine(String value) throws SQLException {
        System.out.println("=========Alarm start =========");
        final String http = "http";
        //oneTable
        JSONObject json = JSONObject.parseObject(value);
        String direction = json.getString("direction");
        String attacker = json.getString("attacker");
        String victim = json.getString("victim");
        String data = json.getString("data");
        String incident_id = json.getString("incident_id");
        //two--->netTable
        JSONObject netJson = JSONObject.parseObject(json.getString("net"));
        String src_ip = netJson.getString("src_ip");
        String src_port = netJson.getString("src_port");
        String dest_ip = netJson.getString("dest_ip");
        String dest_port = netJson.getString("dest_port");
        String real_src_ip = netJson.getString("real_src_ip");
        String proto = netJson.getString("proto");
        String types = netJson.getString("type");
        JSONObject jsons = null;
        //http OR dns OR ...
        switch (types) {
            case "http":
                JSONObject httpJson = JSONObject.parseObject(netJson.getString("http"));
                String method = httpJson == null ? "" : httpJson.getString("method");
                String protocol = httpJson == null ? "" : httpJson.getString("protocol");
                String status = httpJson == null ? "" : httpJson.getString("status");
                String uri = httpJson == null ? "" : httpJson.getString("url");
                jsons = new JSONObject();
                jsons.put("method", method);
                jsons.put("protocol", protocol);
                jsons.put("status", status);
                jsons.put("uri", uri);
                break;
            case "dns":

                break;
            default:
                break;
        }


        //geo_data
        JSONObject geoJson = JSONObject.parseObject(json.getString("geo_data"));
        String country = geoJson == null ? "" : geoJson.getString("Country");
        String province = geoJson == null ? "" : geoJson.getString("Province");
        String city = geoJson == null ? "" : geoJson.getString("City");
        //threat
        JSONObject threatJson = JSONObject.parseObject(json.getString("threat"));
        String name = threatJson == null ? "" : threatJson.getString("name");
        String severity = threatJson == null ? "" : threatJson.getString("severity");
        String confidence = threatJson == null ? "" : threatJson.getString("confidence");
        String result = threatJson == null ? "" : threatJson.getString("result");
        //threat---params
        JSONObject paramsJson = threatJson == null ? new JSONObject() : JSONObject.parseObject(threatJson.getString("params"));
        String userName =  paramsJson == null ? "" : paramsJson.getString("username").replace("null", "");
        String time = json.getString("time");

        JSONObject jsonObject = new JSONObject();
        if (jsons != null) {
            jsonObject.put(types, jsons);
        } else {
            jsonObject.put(types, new JSONObject());
        }
        jsonObject.put("direction", direction);
        jsonObject.put("attacker", attacker);
        jsonObject.put("victim", victim);
        jsonObject.put("data", data);
        jsonObject.put("incident_id", incident_id);
        jsonObject.put("src_ip", src_ip);
        jsonObject.put("src_port", src_port);
        jsonObject.put("dest_ip", dest_ip);
        jsonObject.put("dest_port", dest_port);
        jsonObject.put("real_src_ip", real_src_ip);
        jsonObject.put("proto", proto);
        jsonObject.put("type", types);
        jsonObject.put("Country", country);
        jsonObject.put("Province", province);
        jsonObject.put("City", city);
        jsonObject.put("name", name);
        jsonObject.put("severity", severity);
        jsonObject.put("confidence", confidence);
        jsonObject.put("result", result);
        jsonObject.put("username", userName);
        jsonObject.put("time", time);

        //入库
        ruku(jsonObject);
        System.out.println("=========Alarm end =========");
        return jsonObject;
    }


    /**
     * 解析流量数据入库
     * @param value
     * @return
     * @throws SQLException
     */
    public static JSONObject flowLine(String value) throws SQLException {
        System.out.println("=========flow start =========");
        //oneTable
        JSONObject json = JSONObject.parseObject(value);
        String direction = json.getString("direction");
        String attacker = json.getString("attacker");
        String victim = json.getString("victim");
        String data = json.getString("data");
        String incident_id = json.getString("incident_id");
        //two--->netTable
        JSONObject netJson = JSONObject.parseObject(json.getString("net"));
        String src_ip = netJson.getString("src_ip");
        String src_port = netJson.getString("src_port");
        String dest_ip = netJson.getString("dest_ip");
        String dest_port = netJson.getString("dest_port");
        String real_src_ip = netJson.getString("real_src_ip");
        String proto = netJson.getString("proto");
        String types = netJson.getString("type");
        //dns OR http OR ...
        JSONObject jsons = null;
        switch (types) {
            case "dns":
                JSONObject dnsJson = JSONObject.parseObject(netJson.getString("dns"));
                String rrname = dnsJson == null ? "" : dnsJson.getString("rrname");
                String rdata = dnsJson == null ? "" : dnsJson.getString("rdata");
                String rcode = dnsJson == null ? "" : dnsJson.getString("rcode");
                String id = dnsJson == null ? "" : dnsJson.getString("id");
                String type = dnsJson == null ? "" : dnsJson.getString("type");
                String ttl = dnsJson == null ? "" : dnsJson.getString("ttl");
                String rrtype = dnsJson == null ? "" : dnsJson.getString("rrtype");
                jsons = new JSONObject();
                jsons.put("rrname", rrname);
                jsons.put("rdata", rdata);
                jsons.put("rcode", rcode);
                jsons.put("id", id);
                jsons.put("type", type);
                jsons.put("ttl", ttl);
                jsons.put("rrtype", rrtype);
                break;
            case "http":
                break;
            default:
                break;
        }

        //geo_data
        JSONObject geoJson = JSONObject.parseObject(json.getString("geo_data"));
        String country = geoJson == null ? "" : geoJson.getString("Country");
        String province = geoJson == null ? "" : geoJson.getString("Province");
        String city = geoJson == null ? "" : geoJson.getString("City");
        String time = json.getString("time");

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(types, jsons);
        jsonObject.put("direction", direction);
        jsonObject.put("attacker", attacker);
        jsonObject.put("victim", victim);
        jsonObject.put("data", data);
        jsonObject.put("incident_id", incident_id);
        jsonObject.put("src_ip", src_ip);
        jsonObject.put("src_port", src_port);
        jsonObject.put("dest_ip", dest_ip);
        jsonObject.put("dest_port", dest_port);
        jsonObject.put("real_src_ip", real_src_ip);
        jsonObject.put("proto", proto);
        jsonObject.put("type", types);
        jsonObject.put("Country", country);
        jsonObject.put("Province", province);
        jsonObject.put("City", city);
        jsonObject.put("time", time);

        //入库
        ruku(jsonObject);

        System.out.println("=========flow end =========");
        return jsonObject;
    }

    /**
     * 入库
     * @param jsonObject
     */
    public static void ruku(JSONObject jsonObject) throws SQLException {
        System.out.println("=== ruku start ===");
        //获取当天日期
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String date = dateFormat.format(System.currentTimeMillis());

        //连接mysql
        GetConnUtil connUtil = GetConnUtil.createInstrea();
        Connection connection = connUtil.getConn();
        //表名
        String basicsTable = "basics_alarm_info_"+date;
        String netTable = "net_info_"+date;
        String dnsTable = "dns_info_"+date;
        String httpTable = "http_info_"+date;

        String type = jsonObject.getString("type");
        //sql
        String basicsSql = "";
        String dnsSql = "";
        String httpSql = "";
        String netSql = "insert into "+netTable+" values(?,?,?,?,?,?,?,?)";
        if ("http".equals(type)) {
            basicsSql = "insert into "+basicsTable+" (id, direction, attacker, victim, data, incident_id, " +
                    "country, province, city, time, name, severity, confidence, result, userName) " +
                    "values(null,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            httpSql = "insert into "+httpTable+" values(?,?,?,?,?)";
        }
        if ("dns".equals(type)) {
            basicsSql = "insert into "+basicsTable+" (id, direction, attacker, victim, data, incident_id, country, province, city, time) " +
                    "values(null,?,?,?,?,?,?,?,?,?)";

            dnsSql = "insert into "+dnsTable+" values(?,?,?,?,?,?,?,?)";
        }

        PreparedStatement basicsStatement = null;
        PreparedStatement netStatement = null;
        PreparedStatement typeStatement = null;
        //建表
        createTable(connection, basicsTable, date);
        basicsStatement = connection.prepareStatement(basicsSql, Statement.RETURN_GENERATED_KEYS);
        basicsStatement.setString(1, jsonObject.getString("direction"));
        basicsStatement.setString(2, jsonObject.getString("attacker"));
        basicsStatement.setString(3, jsonObject.getString("victim"));
        basicsStatement.setString(4, jsonObject.getString("data"));
        basicsStatement.setString(5, jsonObject.getString("incident_id"));
        basicsStatement.setString(6, jsonObject.getString("Country"));
        basicsStatement.setString(7, jsonObject.getString("Province"));
        basicsStatement.setString(8, jsonObject.getString("City"));
        basicsStatement.setString(9, jsonObject.getString("time"));
        if ("http".equals(type)) {
            basicsStatement.setString(10, jsonObject.getString("name"));
            basicsStatement.setString(11, jsonObject.getString("severity"));
            basicsStatement.setString(12, jsonObject.getString("confidence"));
            basicsStatement.setString(13, jsonObject.getString("result"));
            basicsStatement.setString(14, jsonObject.getString("userName"));
        }
        int _id = 0;
        try {
            //basicsTable
            basicsStatement.executeUpdate();
            ResultSet rs = basicsStatement.getGeneratedKeys();
            while (rs.next()) {
                _id = rs.getInt(1);
            }
            System.out.println("===获取第一张表_id===>>>"+_id);
            //netTable
            createTable(connection, netTable, date);
            netStatement = connection.prepareStatement(netSql);
            netStatement.setInt(1, _id);
            netStatement.setString(2, jsonObject.getString("src_ip"));
            netStatement.setString(3, jsonObject.getString("src_port"));
            netStatement.setString(4, jsonObject.getString("real_src_ip"));
            netStatement.setString(5, jsonObject.getString("dest_ip"));
            netStatement.setString(6, jsonObject.getString("dest_port"));
            netStatement.setString(7, jsonObject.getString("proto"));
            netStatement.setString(8, jsonObject.getString("type"));
            try {
                netStatement.executeUpdate();
                //http_info_2020xxxx OR dns_info_2020xxxx
                if ("http".equals(type)) {
                    createTable(connection, httpTable, date);
                    typeStatement = connection.prepareStatement(httpSql);

                    JSONObject httpJson = jsonObject.getJSONObject("http");
                    String method = httpJson == null ? "" : httpJson.getString("method");
                    String protocol = httpJson == null ? "" : httpJson.getString("protocol");
                    String status = httpJson == null ? "" : httpJson.getString("status");
                    String uri = httpJson == null ? "" : httpJson.getString("uri");
                    typeStatement.setInt(1, _id);
                    typeStatement.setString(2, method);
                    typeStatement.setString(3, protocol);
                    typeStatement.setString(4, status);
                    typeStatement.setString(5, uri);
                }
                if ("dns".equals(type)) {
                    createTable(connection, dnsTable, date);
                    typeStatement = connection.prepareStatement(dnsSql);

                    JSONObject dnsJson = jsonObject.getJSONObject("dns");
                    String rrname = dnsJson == null ? "" : dnsJson.getString("rrname");
                    String rdata = dnsJson == null ? "" : dnsJson.getString("rdata");
                    String rcode = dnsJson == null ? "" : dnsJson.getString("rcode");
                    String dnsType = dnsJson == null ? "" : dnsJson.getString("type");
                    String rrtype = dnsJson == null ? "" : dnsJson.getString("rrtype");
                    int id = dnsJson == null ? 0 : dnsJson.getIntValue("id");
                    int ttl = dnsJson == null ? 0 : dnsJson.getIntValue("ttl");
                    typeStatement.setInt(1, _id);
                    typeStatement.setString(2, rrname);
                    typeStatement.setString(3, rdata);
                    typeStatement.setString(4, rcode);
                    typeStatement.setInt(5, id);
                    typeStatement.setString(6, dnsType);
                    typeStatement.setInt(7, ttl);
                    typeStatement.setString(8, rrtype);
                }
                try {
                    typeStatement.executeUpdate();
                } catch (Exception e) {
                    connection.rollback(); //回滚
                    log.error("==="+type+" typeStatement入库异常===>>>"+e.getMessage());
                    System.out.println("==="+type+" typeStatement入库异常===>>>"+e.getMessage());
                }
            } catch (Exception e) {
                connection.rollback(); //回滚
                log.error("==="+type+" netStatement入库异常===>>>"+e.getMessage());
                System.out.println("==="+type+" netStatement入库异常===>>>"+e.getMessage());
            }
            //最后提交
            connection.commit();
        } catch (Exception ex) {
            try {
                connection.rollback(); //回滚
            } catch (Exception e) {
                log.error("==="+type+" basicsStatement回滚异常===>>>"+e.getMessage());
                System.out.println("==="+type+" basicsStatement回滚异常===>>>"+e.getMessage());
            }
            log.error("==="+type+" basicsStatement入库异常===>>>"+ex.getMessage());
            System.out.println("==="+type+" basicsStatement入库异常===>>>"+ex.getMessage());
        } finally {
            connUtil.closeAll(basicsStatement, connection);
        }
        System.out.println("==="+type+" jsonObject===>>>"+jsonObject.toString());
        System.out.println("========= ruku end =========");
    }

    /**
     * 按天生成新表
     * @param connection
     * @param tableName
     * @param date
     * @throws SQLException
     */
    public static void createTable(Connection connection, String tableName, String date) throws SQLException {
        String basicsTable = "basics_alarm_info_"+date;
        String netTable = "net_info_"+date;
        String httpTable = "http_info_"+date;
        String dnsTable = "dns_info_"+date;
        String sql = "";
        if (basicsTable.equals(tableName)) {
            sql = "create table IF NOT EXISTS "+tableName+"(" +
                    "id int NOT NULL AUTO_INCREMENT, direction varchar(50), attacker varchar(50), victim varchar(50), " +
                    "data varchar(255), incident_id varchar(50), country varchar(50), province varchar(50), city varchar(50), time varchar(50), " +
                    "name varchar(50), severity varchar(50), confidence varchar(50), result varchar(50), username varchar(50), primary key(id))";
        }
        if (netTable.equals(tableName)) {
            sql = "create table IF NOT EXISTS "+tableName+"(" +
                    "id int NOT NULL, src_ip varchar(50), src_port varchar(50), real_src_ip varchar(50), " +
                    "dest_ip varchar(50), dest_port varchar(50), proto varchar(50), type varchar(50), primary key(id))";
        }
        if (httpTable.equals(tableName)) {
            sql = "create table IF NOT EXISTS "+tableName+"(" +
                    "id int NOT NULL, method varchar(50), protocol varchar(50), status varchar(50), url varchar(255), primary key(id))";
        }
        if (dnsTable.equals(tableName)) {
            sql = "create table IF NOT EXISTS "+tableName+"(" +
                    "id int NOT NULL, rrname varchar(255), rdata varchar(50), rcode varchar(50), " +
                    "ids int, type varchar(50), ttl int, rrtype varchar(50), primary key(id))";
        }
        try {
            Statement statement = connection.createStatement();
            System.out.println("===打印建表sql===>>>"+sql);
            statement.execute(sql);
            statement.close();
        } catch (Exception e) {
            log.error("===创建表tableName===>>>"+tableName+",异常信息: "+e.getMessage());
            System.out.println("===创建表tableName===>>>"+tableName+",异常信息: "+e.getMessage());
        } finally {

        }
    }
}
