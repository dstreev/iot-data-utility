package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streever.iot.data.utility.generator.fields.*;
import com.streever.iot.data.utility.generator.fields.support.StartStopState;
import com.streever.iot.data.utility.generator.output.Output;
import com.streever.iot.data.utility.generator.output.OutputFormat;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.ClassUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

@JsonIgnoreProperties({"orderedFields"})
public class RecordGenerator {
    private String title;
    private String description;

    // Used to control if record gen is terminated
    // because of the control fields state.

    private List<String> keyFields;
    private String controlField;
    private ControlField controlFieldInt;

    private Output output;
    private List<String> order;
    private Map<String, FieldBase> orderedFields;

    private JsonFactory jFactory = new JsonFactory();

    public List<String> getKeyFields() {
        return keyFields;
    }

    public void setKeyField(List<String> keyFields) {
        this.keyFields = keyFields;
    }

    private List<FieldBase> fields;

    public String getControlField() {
        return controlField;
    }

    public void setControlField(String controlField) {
        this.controlField = controlField;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    public List<FieldBase> getFields() {
        return fields;
    }

    private Object key = null;
    private Object value = null;

    protected ObjectMapper om = new ObjectMapper();

    public void setFields(List<FieldBase> fields) {

        this.fields = fields;
        if (this.order != null)
            orderFields();

        // If a Control Field has been identified, assign it.
        if (controlField != null) {
            for (FieldBase field : fields) {
                if (field.getName().equals(controlField)) {
                    if (field instanceof ControlField) {
                        ((ControlField) field).setControlField(Boolean.TRUE);
                        this.controlFieldInt = ((ControlField) field);
                    }
                }
            }
        }

    }

    public List<String> getOrder() {
        return order;
    }

    public void setOrder(List<String> order) {
        this.order = order;
        if (this.fields != null)
            orderFields();
    }

    protected void orderFields() {
        if (this.fields != null && this.order != null) {
            orderedFields = new LinkedHashMap<String, FieldBase>();
//            int position = 1;
            for (String fieldName : order) {
                String[] searchFieldName = fieldName.split("\\.");
                boolean found = Boolean.FALSE;
                for (FieldBase field : fields) {
                    if (field.getName().equals(searchFieldName[0])) {
                        orderedFields.put(fieldName, field);
                        found = Boolean.TRUE;
                        break;
                    }
                }
                if (!found) {
                    System.err.println("Field Not Found: " + searchFieldName[0]);
                }
            }
        }
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public String hiveTableLayout() {
        Iterator<String> iFieldKeys = orderedFields.keySet().iterator();


        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE <my_gen_table> (\n");
        while (iFieldKeys.hasNext()) {
            String iFieldKey = iFieldKeys.next();
            FieldBase fb = orderedFields.get(iFieldKey);
            for (int i = 1; i <= fb.getRepeat(); i++) {
                String keyFieldName = iFieldKey;
                if (fb.getRepeat() > 1) {
                    keyFieldName = keyFieldName + "_" + i;
                }
                sb.append("\t" + keyFieldName + " STRING");
                if (i < fb.getRepeat()) {
                    sb.append(",\n");
                }
            }
            if (iFieldKeys.hasNext()) {
                sb.append(",\n");
            } else {
                sb.append(")\n");
            }
        }
        return sb.toString();
    }

    public void next() throws TerminateException {
        StringBuilder sb = new StringBuilder();
        Iterator<String> iFieldKeys = orderedFields.keySet().iterator();
        // reset the key,value
        key = null;
        value = null;
        Map<String, Object> keys = null;
        if (keyFields != null) {
            keys = new TreeMap<String, Object>();
        }

        if (output.getFormat() == OutputFormat.JSON) {

            ObjectNode jRoot = JsonNodeFactory.instance.objectNode();

            while (iFieldKeys.hasNext()) {
                String iFieldKey = iFieldKeys.next();
                FieldBase fb = orderedFields.get(iFieldKey);
                for (int i = 0; i < fb.getRepeat(); i++) {
                    String[] keyParts = iFieldKey.split("\\.");
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
                        keys.put(fb.getName(), value);
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
            }
//            ObjectMapper om = new ObjectMapper();
            try {
                sb.append(om.writeValueAsString(jRoot));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

        } else {

            while (iFieldKeys.hasNext()) {
                String iFieldKey = iFieldKeys.next();
                FieldBase fb = orderedFields.get(iFieldKey);
                for (int i = 0; i < fb.getRepeat(); i++) {

                    Object value = fb.getNext();
                    if (keyFields != null && keyFields.contains(fb.getName())) {
                        keys.put(fb.getName(), value);
                    }
                    sb.append(value);
                    if (iFieldKeys.hasNext()) {
                        sb.append(output.getDelimiter());
                    }
                }
            }
        }
        if (controlFieldInt != null && controlFieldInt.terminate()) {
            throw new TerminateException("Field " + controlField + " has reached it limit and terminated the record generating process");
        }

        // If Key Fields are defined, build them out.
        if (keyFields != null) {
            StringBuffer keyBuffer = new StringBuffer();
            for (String field : keyFields) {
                keyBuffer.append(keys.get(field));
            }
            key = keyBuffer.toString();
        }
        value = sb.toString();
    }


}
