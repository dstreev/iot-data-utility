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

public class JSONOutput extends FileOutput {
    private static ObjectMapper om = new ObjectMapper();

    @Override
    protected String getExtension() {
        return "json";
    }

    protected static String getLine(Map<FieldProperties, Object> record)  {
        String rtn = null;
        ObjectNode jRoot = JsonNodeFactory.instance.objectNode();
        Map<String, Object> outMap = new LinkedHashMap<String, Object>();
        Set<Map.Entry<FieldProperties, Object>> entries = record.entrySet();
        for (Map.Entry<FieldProperties, Object> entry : entries) {
            if (entry.getValue() instanceof Short) {
                jRoot.put(entry.getKey().getName(), (Short) entry.getValue());
            } else if (entry.getValue() instanceof Integer) {
                jRoot.put(entry.getKey().getName(), (Integer) entry.getValue());
            } else if (entry.getValue() instanceof Long) {
                jRoot.put(entry.getKey().getName(), (Long) entry.getValue());
            } else if (entry.getValue() instanceof Float) {
                jRoot.put(entry.getKey().getName(), (Float) entry.getValue());
            } else if (entry.getValue() instanceof Double) {
                jRoot.put(entry.getKey().getName(), (Double) entry.getValue());
            } else if (entry.getValue() instanceof BigDecimal) {
                jRoot.put(entry.getKey().getName(), (BigDecimal) entry.getValue());
            } else if (entry.getValue() instanceof BigInteger) {
                jRoot.put(entry.getKey().getName(), (BigInteger) entry.getValue());
            } else if (entry.getValue() instanceof Boolean) {
                jRoot.put(entry.getKey().getName(), (Boolean) entry.getValue());
            } else if (entry.getValue() instanceof String) {
                jRoot.put(entry.getKey().getName(), entry.getValue().toString());
            } else if (entry.getValue() instanceof ArrayList) {
                ArrayNode an = jRoot.putArray(entry.getKey().getName());
                for (Object item : (List) entry.getValue()) {
                    if (item instanceof String)
                        an.add(item.toString());
                    else if (item instanceof Long)
                        an.add(((Long) item).longValue());
                }
            } else if (entry.getValue() instanceof ReferenceField) {
                jRoot.put(entry.getKey().getName(), entry.getValue().toString());
            } else if (!ClassUtils.isPrimitiveOrWrapper(entry.getValue().getClass())) {
                try {
                    for (Field f : entry.getValue().getClass().getDeclaredFields()) {
                        String fName = f.getName();
                        Object fValue = PropertyUtils.getProperty(entry.getValue(), fName);
                        jRoot.put(entry.getKey().getName() + "." + fName, fValue.toString());
                    }
                } catch (Exception e) {
                    // TODO: Handle Error
                    e.printStackTrace();
                }
            }
        }
        try {
            rtn = om.writeValueAsString(jRoot);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return rtn;
    }

    @Override
    public long write(Map<FieldProperties, Object> record) throws IOException {
        long rtn = 0;
        if (isOpen()) {
            String line = getLine(record);
            rtn = line.length() + 1;
            writeLine(line);
        } else {
            // TODO: Throw not open exception.
        }
        return rtn;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        JSONOutput clone = (JSONOutput) super.clone();
        return clone;
    }
}
