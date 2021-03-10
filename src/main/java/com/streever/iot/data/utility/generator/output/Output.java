package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Schema;
import com.streever.iot.data.utility.generator.fields.FieldProperties;

import java.io.IOException;
import java.util.Map;

public interface Output {
    void link(Schema record);
    long write(Map<FieldProperties, Object> record) throws IOException;
    boolean open(String prefix) throws IOException;
    boolean close() throws IOException;
}
