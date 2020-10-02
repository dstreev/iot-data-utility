package com.streever.iot.data.utility.generator.output.kafka;

import java.util.Map;
import java.util.stream.Collectors;

public class Topic implements Cloneable {
    private Map<String, String> headers;

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        Topic clone = (Topic) super.clone();
        if (this.headers != null) {
            clone.setHeaders(this.headers.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
        return clone;
    }
}
