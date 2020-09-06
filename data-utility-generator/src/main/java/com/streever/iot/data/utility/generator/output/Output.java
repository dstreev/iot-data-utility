package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Record;
import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.FieldProperties;

import java.util.Map;

public interface Output {
    void link(Record record);
    void write(Map<FieldProperties, Object> record);
    boolean open(String prefix);
    boolean close();
}
