package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.fields.ControlField;
import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.FieldProperties;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.CSVOutput;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URL;
import java.util.*;

@JsonIgnoreProperties({"orderedFields", "parent", "id", "keyMap", "valueMap"})
public class Schema implements Comparable<Schema> {
    private String id;
    private String title;
    private String description;
    private Schema parent;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Schema getParent() {
        return parent;
    }

    public void setParent(Schema parent) {
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

    private Map<FieldProperties, Object> keyMap = new LinkedHashMap<FieldProperties, Object>();
    private Map<FieldProperties, Object> valueMap = new LinkedHashMap<FieldProperties, Object>();

    protected ObjectMapper om = new ObjectMapper();

    /*
    Use this to loop through the fields and relationships to validate the configurations.
     */
    public boolean validate() {
        return validate(this);
    }

    protected boolean validate(Schema record) {
        boolean rtn = Boolean.TRUE;
        for (FieldBase field: record.getFields()) {
            if (!field.validate()) {
                rtn = Boolean.FALSE;
            }
        }
        if (record.getRelationships() != null) {
            Set<String> relationships = record.getRelationships().keySet();
            for (String relationshipKey: relationships) {
                Relationship relationship = record.getRelationships().get(relationshipKey);
                // TODO: Check Cardinality here, if needed...
                // Recurse into record.
                if (!validate(relationship.getRecord())) {
                    rtn = Boolean.FALSE;
                }
            }
        }
        return rtn;
    }

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
                        if (searchFieldName.length > 1) {
                            field.setMaintainState(true);
                            if (field.getRepeat() > 1) {
                                throw new RuntimeException("Field: " + field.getName() + " with Ordering: " +
                                        fieldName + " is set for 'repeat' AND with 'state' (like start/stop).  This isn't supported");
                            }
                        }
                        found = Boolean.TRUE;
                        break;
                    }
                }
                if (!found) {
                    throw new RuntimeException("Ordering Field [" + searchFieldName[0] +
                            "] not found in record fields list for schema [" +
                            this.getTitle() + "]");
                }
            }
        }
    }

    public Map<FieldProperties, Object> getKeyMap() {
        return keyMap;
    }

    public Map<FieldProperties, Object> getValueMap() {
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

    public void next(Map<FieldProperties, Object> parentKeys) throws TerminateException {
        // Clear Maps holding previous record.
        keyMap.clear();
        valueMap.clear();
        if (parentKeys != null && !parentKeys.isEmpty()) {
            keyMap.putAll(parentKeys);
            valueMap.putAll(parentKeys);
        }

        // TODO: Stuck HERE.  Need to deal with maintained state, order, and repeat.
        Map<String, FieldProperties> fieldNextValues = new TreeMap<String, FieldProperties>();
        for (FieldBase field : fields) {
//            if (field.getRepeat() > 1) {
//                for (int i=1;i<field.getRepeat();i++) {
//                    String repeater = StringUtils.leftPad(Integer.toString(i), 3, '0');
//                    FieldProperties fp = field.getFieldProperties(repeater);
//                    fieldNextValues.put(fp, field.getNext());
//                }
//            } else {
            Object value = field.getNext();
            FieldProperties fp = field.getFieldProperties();
            fieldNextValues.put(field.getName(), fp);
//            }
        }

        Iterator<String> iFieldKeys = orderedFields.keySet().iterator();

        Map<String, Object> keys = null;

        while (iFieldKeys.hasNext()) {
            String iFieldKey = iFieldKeys.next();

//            FieldBase fbase = orderedFields.get(iFieldKey);
            String[] fieldNameParts = iFieldKey.split("\\.");
            String state = (fieldNameParts.length > 1 ? fieldNameParts[1] : null);
            // Get the FieldProperties from the Map.
            FieldProperties fp = fieldNextValues.get(fieldNameParts[0]);
            if (fp.getField().isMaintainState()) {
                if (state != null) {
                    // State items shouldn't be keys.
                    FieldProperties stateFp = new FieldProperties(iFieldKey, fp.getField());
                    valueMap.put(stateFp, fp.getField().getNextStateValue(state));
                } else {
                    // Shouldn't happen.
                }
            } else if (fp.getField().getRepeat() > 1) {
                for (int i = 1; i < fp.getField().getRepeat(); i++) {
                    String repeater = StringUtils.leftPad(Integer.toString(i), 3, '0');
                    FieldProperties repeatFp = new FieldProperties(fieldNameParts[0] + "_" + repeater, fp.getField());
                    valueMap.put(repeatFp, fp.getField().getNext());
                }
            } else {
                if (keyFields != null && keyFields.contains(fp.getName())) {
                    keyMap.put(fp, fp.getField().getLast());
                }
                valueMap.put(fp, fp.getField().getLast());
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

        Schema record = (Schema) o;

        return id != null ? id.equals(record.id) : record.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public int compareTo(Schema o) {
        return this.getId().compareTo(o.getId());
    }

    public static Schema deserialize(String configResource) throws IOException, JsonMappingException {
        Schema recDef = null;
//        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
//        StackTraceElement et = stacktrace[2];//maybe this number needs to be corrected
//        String methodName = et.getMethodName();
//        System.out.println("=========================");
//        System.out.println("Build Method: " + methodName);
//        System.out.println("-------------------------");
        String extension = FilenameUtils.getExtension(configResource);
        ObjectMapper mapper = null;
        if ("yaml".equals(extension.toLowerCase()) || "yml".equals(extension.toLowerCase())) {
            mapper = new ObjectMapper(new YAMLFactory());
        } else if ("json".equals(extension.toLowerCase()) || "jsn".equals(extension.toLowerCase())) {
            mapper = new ObjectMapper(new JsonFactory());
        } else {
            throw new RuntimeException(configResource + ": can't determine type by extension.  Require one of: ['yaml',yml,'json','jsn']");
        }

        // Try as a Resource (in classpath)
        URL configURL = mapper.getClass().getResource(configResource);
        if (configURL != null) {
            // Convert to String.
            String configDefinition = IOUtils.toString(configURL, "UTF-8");
            recDef = mapper.readerFor(Schema.class).readValue(configDefinition);
        } else {
            // Try on Local FileSystem.
            configURL = new URL("file", null, configResource);
            if (configURL != null) {
                String configDefinition = IOUtils.toString(configURL, "UTF-8");
                recDef = mapper.readerFor(Schema.class).readValue(configDefinition);
            } else {
                throw new RuntimeException("Couldn't locate 'Serialized Record File': " + configResource);
            }
        }

        return recDef;
    }

}
