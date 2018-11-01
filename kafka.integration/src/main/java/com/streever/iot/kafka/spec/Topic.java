package com.streever.iot.kafka.spec;

import java.util.Map;

public class Topic {
    private String name;
    private Map<String, String> headers;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }
}
