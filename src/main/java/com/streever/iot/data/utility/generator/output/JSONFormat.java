package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streever.iot.data.utility.generator.fields.FieldProperties;
import com.streever.iot.data.utility.generator.fields.ReferenceField;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.ClassUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class JSONFormat extends FormatBase {
    private static ObjectMapper om = new ObjectMapper();

    public String getExtension() {
        return "json";
    }

    public String format(ObjectNode node) {
        String rtn = null;
        try {
            rtn = om.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return rtn;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        JSONFormat clone = (JSONFormat) super.clone();
        return clone;
    }
}
