package org.flume;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class InfoInterceptor implements Interceptor {
    private static Logger log = Logger.getLogger(String.valueOf(InfoInterceptor.class));
    @Override
    public void initialize() {}
    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charsets.UTF_8);
        try{
            String newBody = body.substring(body.indexOf(":") + 1);
            JSONObject json = JSONObject.parseObject(newBody);
            json.remove("assets");
            json.remove("dest_assets");
            String strBody = json.toString();
            event.setBody(strBody.getBytes());
        } catch (Exception ex) {
            log.error("解析事件异常=====>>>>>"+ex.getMessage()+"===异常body===>>>"+body);
        }
        return event;
    }
    @Override
    public List<Event> intercept(List<Event> list) {
        if (list == null) return null;
        List<Event> events = new ArrayList<Event>();
        for (Event event : list) {
            Event intercept = intercept(event);
            events.add(intercept);
        }
        return events;
    }
    @Override
    public void close() {}
    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new InfoInterceptor();
        }
        @Override
        public void configure(Context context) {}
    }
}
