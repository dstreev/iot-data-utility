package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.ReferenceField;
import com.streever.iot.data.utility.generator.fields.support.StartStopState;
import com.streever.iot.data.utility.generator.output.Output;
import com.streever.iot.data.utility.generator.output.OutputFormat;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.ClassUtils;

import java.io.BufferedWriter;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class Next {
    private String name;
    private Map<FieldBase, Object> fields = new LinkedHashMap<FieldBase, Object>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void addField(FieldBase field, Object value) {
        fields.put(field, value);
    }

    public void writeRecord(Output output) {
        BufferedWriter writer = output.getWriter(this.name);
        // TODO: write next record
        if (output.getFormat() == OutputFormat.JSON) {

            ObjectNode jRoot = JsonNodeFactory.instance.objectNode();

            // Write Fields (order is implied because of LinkedHashMap
            for (FieldBase fb: fields.keySet()) {


//            while (iFieldKeys.hasNext()) {
//                String iFieldKey = iFieldKeys.next();
//                FieldBase fb = orderedFields.get(iFieldKey);
                for (int i = 0; i < fb.getRepeat(); i++) {
                    // TODO: PICKUP HERE
                    String[] keyParts = fb.getName().split("\\.");
                    if (keyParts[keyParts.length - 1].equals("start")) {
                        fb.setStartStopState(StartStopState.START);
                    } else if (keyParts[keyParts.length - 1].equals("stop")) {
                        fb.setStartStopState(StartStopState.STOP);
                    } else {
                        fb.setStartStopState(StartStopState.NA);
                    }
                    Object value = fb.getNext();
                    String keyFieldName = iFieldKey;
                    if (fb.getRepeat() > 1) {
                        keyFieldName = keyFieldName + "_" + i;
                    }
                    if (keyFields != null && keyFields.contains(fb.getName())) {
                        rtn.addKey(fb.getName(), value);
                    }
                    if (value instanceof Short) {
                        jRoot.put(keyFieldName, (Short) value);
                    } else if (value instanceof Integer) {
                        jRoot.put(keyFieldName, (Integer) value);
                    } else if (value instanceof Long) {
                        jRoot.put(keyFieldName, (Long) value);
                    } else if (value instanceof Float) {
                        jRoot.put(keyFieldName, (Float) value);
                    } else if (value instanceof Double) {
                        jRoot.put(keyFieldName, (Double) value);
                    } else if (value instanceof BigDecimal) {
                        jRoot.put(keyFieldName, (BigDecimal) value);
                    } else if (value instanceof BigInteger) {
                        jRoot.put(keyFieldName, (BigInteger) value);
                    } else if (value instanceof Boolean) {
                        jRoot.put(keyFieldName, (Boolean) value);
                    } else if (value instanceof String) {
                        jRoot.put(keyFieldName, value.toString());
                    } else if (value instanceof ArrayList) {
                        ArrayNode an = jRoot.putArray(iFieldKey);
                        for (Object item : (List) value) {
                            if (item instanceof String)
                                an.add(item.toString());
                            else if (item instanceof Long)
                                an.add(((Long) item).longValue());
                        }
                    } else if (value instanceof ReferenceField) {
                        jRoot.put(keyFieldName, value.toString());
                    } else if (!ClassUtils.isPrimitiveOrWrapper(value.getClass())) {
                        try {
                            for (Field f : value.getClass().getDeclaredFields()) {
                                String fName = f.getName();
                                Object fValue = PropertyUtils.getProperty(value, fName);
                                jRoot.put(keyFieldName + "." + fName, fValue.toString());
                            }
                        } catch (Exception e) {
                            // TODO: Handle Error
                            e.printStackTrace();
                        }
                    }
                }
//            }
//            ObjectMapper om = new ObjectMapper();
//            try {
//                sb.append(om.writeValueAsString(jRoot));
//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
            }
        } else {

            for (FieldBase fb: fields.keySet()) {

                for (int i = 0; i < fb.getRepeat(); i++) {

                    Object value = fb.getNext();
                    if (keyFields != null && keyFields.contains(fb.getName())) {
                        rtn.addKey(fb.getName(), value);
                    } else {
                        rtn.addValue(fb.getName(), value)
                    }
//                    sb.append(value);
//                    if (iFieldKeys.hasNext()) {
//                        sb.append(output.getDelimiter());
//                    }
                }
            }
        }

    }
}
