package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface Format {
    String format(ObjectNode node);
}
