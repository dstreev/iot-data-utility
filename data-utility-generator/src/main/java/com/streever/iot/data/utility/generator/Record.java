package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streever.iot.data.utility.generator.fields.ControlField;
import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.ReferenceField;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.fields.support.StartStopState;
import com.streever.iot.data.utility.generator.output.Output;
import com.streever.iot.data.utility.generator.output.OutputFormat;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.ClassUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

@JsonIgnoreProperties({"output"})
public class Record {

    private List<String> keyFields;
    private List<String> order;
    private Map<String, FieldBase> orderedFields;
    private Output output;

    public List<String> getKeyFields() {
        return keyFields;
    }

    public void setKeyField(List<String> keyFields) {
        this.keyFields = keyFields;
    }

    private List<FieldBase> fields;
    private List<Child> children;

    private Object key = null;
    private Object value = null;

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    public List<FieldBase> getFields() {
        return fields;
    }

    public void setFields(List<FieldBase> fields) {

        this.fields = fields;
        if (this.order != null)
            orderFields();

        // TODO: If a Control Field has been identified, assign it.
//        if (controlField != null) {
//            for (FieldBase field : fields) {
//                if (field.getName().equals(controlField)) {
//                    if (field instanceof ControlField) {
//                        ((ControlField) field).setControlField(Boolean.TRUE);
//                        this.controlFieldInt = ((ControlField) field);
//                    }
//                }
//            }
//        }

    }

    public List<Child> getChildren() {
        return children;
    }

    public void setChildren(List<Child> children) {
        this.children = children;
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

//    public Object getKey() {
//        return key;
//    }

//    public Object getValue() {
//        return value;
//    }

    public Next next() throws TerminateException {
        Next rtn = new Next();
//        StringBuilder sb = new StringBuilder();
        Iterator<String> iFieldKeys = orderedFields.keySet().iterator();

        while (iFieldKeys.hasNext()) {
            String iFieldKey = iFieldKeys.next();
            // TODO: Need to double check this ordering
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

//                Object value = fb.getNext();
//                fb.setLast(value);
                rtn.addField(fb, value);
//                    sb.append(value);
//                    if (iFieldKeys.hasNext()) {
//                        sb.append(output.getDelimiter());
//                    }
            }
        }
        // TODO: Rework Control Fields
//        if (controlFieldInt != null && controlFieldInt.terminate()) {
//            throw new TerminateException("Field " + controlField + " has reached it limit and terminated the record generating process");
//        }

        // If Key Fields are defined, build them out.
//        if (keyFields != null) {
//            StringBuffer keyBuffer = new StringBuffer();
//            for (String field : keyFields) {
//                keyBuffer.append(keys.get(field));
//            }
//            key = keyBuffer.toString();
//        }
//        value = sb.toString();
        return rtn;
    }

}
