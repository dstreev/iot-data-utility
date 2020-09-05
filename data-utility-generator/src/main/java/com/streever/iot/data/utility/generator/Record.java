package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.fields.ControlField;
import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.CSVOutput;
import org.apache.commons.io.IOUtils;

import java.net.URL;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties({"orderedFields", "parent", "id", "keyMap", "valueMap"})
public class Record implements Comparable<Record> {
    private String id;
    private String title;
    private String description;
    private Record parent;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Record getParent() {
        return parent;
    }

    public void setParent(Record parent) {
        this.parent = parent;
    }

    // Used to control if record gen is terminated
    // because of the control fields state.

    private List<String> keyFields;
    private String controlField;
    private ControlField controlFieldInt;

    private CSVOutput output;
    private List<String> order;
    private Map<String, FieldBase> orderedFields;

    private Map<String, Relationship> relationships;

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

    public CSVOutput getOutput() {
        return output;
    }

    public void setOutput(CSVOutput output) {
        this.output = output;
    }

    public List<FieldBase> getFields() {
        return fields;
    }

    public Map<String, Relationship> getRelationships() {
        return relationships;
    }

    public void setRelationships(Map<String, Relationship> relationships) {
        this.relationships = relationships;
    }

    private Map<FieldBase, Object> keyMap = new LinkedHashMap<FieldBase, Object>();
    private Map<FieldBase, Object> valueMap = new LinkedHashMap<FieldBase, Object>();

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

    public Map<FieldBase, Object> getKeyMap() {
        return keyMap;
    }

    public Map<FieldBase, Object> getValueMap() {
        return valueMap;
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

    public void next(Map<FieldBase, Object> parentKeys) throws TerminateException {
        // Clear Maps holding previous record.
        keyMap.clear();
        valueMap.clear();
        if (parentKeys != null && !parentKeys.isEmpty()) {
            keyMap.putAll(parentKeys);
            valueMap.putAll(parentKeys);
        }

        StringBuilder sb = new StringBuilder();
        Iterator<String> iFieldKeys = orderedFields.keySet().iterator();

        Map<String, Object> keys = null;

        while (iFieldKeys.hasNext()) {
            String iFieldKey = iFieldKeys.next();
            FieldBase fb = orderedFields.get(iFieldKey);
            if (fb.getRepeat() == 1) {
                Object value = fb.getNext();
                if (keyFields != null && keyFields.contains(fb.getName())) {
                    keyMap.put(fb, value);
                }
                valueMap.put(fb, value);
            } else {
                for (int i = 0; i < fb.getRepeat(); i++) {

                    Object value = fb.getNext();
                    String keyFieldName = iFieldKey;
                    if (fb.getRepeat() > 1) {
                        keyFieldName = keyFieldName + "_" + i;
                    }
                    // TODO: Need to fix repeat...
                    if (keyFields != null && keyFields.contains(fb.getName())) {
                        keyMap.put(fb, value);
//                        keyMap.put(keyFieldName, value);
                    }
                    valueMap.put(fb, value);
//                    valueMap.put(keyFieldName, value);
                }
            }
        }

        if (controlFieldInt != null && controlFieldInt.terminate()) {
            throw new TerminateException("Field " + controlField + " has reached it limit and terminated the record generating process");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Record record = (Record) o;

        return id != null ? id.equals(record.id) : record.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public int compareTo(Record o) {
        return this.getId().compareTo(o.getId());
    }

    public static Record deserialize(String configResource) {
        Record recDef = null;
//        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
//        StackTraceElement et = stacktrace[2];//maybe this number needs to be corrected
//        String methodName = et.getMethodName();
//        System.out.println("=========================");
//        System.out.println("Build Method: " + methodName);
//        System.out.println("-------------------------");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {
            URL configURL = mapper.getClass().getResource(configResource);
            if (configURL != null) {
                // Convert to String.
                String yamlConfigDefinition = IOUtils.toString(configURL,"UTF-8");
                recDef = mapper.readerFor(Record.class).readValue(yamlConfigDefinition);
            } else {
                throw new RuntimeException("Couldn't locate 'Serialized Record File': " + configResource);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return recDef;
    }

}
