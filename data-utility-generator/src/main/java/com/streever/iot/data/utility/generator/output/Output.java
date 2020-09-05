package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Record;

import java.util.Map;

public interface Output {
    void write(Map<String, Object> record);
    boolean open();
    boolean close();
}
