package org.example;

import com.alibaba.fastjson.JSONObject;
import kafka.utils.json.JsonObject;

public class Statistics {
    private Long avg;
    private Long sum;
    private Long count;
    private JSONObject json;

    public Statistics() {

    }

    public Statistics(long avg, long sum, long count, JSONObject json) {
        this.avg = avg;
        this.sum = sum;
        this.count = count;
        this.json = json;

    }

    public Long getAvg() {
        return avg;
    }

    public void setAvg(Long avg) {
        this.avg = avg;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public JSONObject getJson() {
        return json;
    }

    public void setJson(JSONObject json) {
        this.json = json;
    }
}
