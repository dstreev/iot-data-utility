package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streever.iot.data.utility.generator.Schema;
import com.streever.iot.data.utility.generator.fields.FieldProperties;

import java.io.IOException;
import java.util.Map;

public interface Output {
    void link(Schema record);
    long write(ObjectNode node) throws IOException;
    boolean open(String prefix) throws IOException;
    boolean close() throws IOException;
}
