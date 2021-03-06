package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.fields.*;
import com.streever.iot.data.utility.generator.output.CSVFormat;
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

    private CSVFormat output;
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

    public CSVFormat getOutput() {
        return output;
    }

    public void setOutput(CSVFormat output) {
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

    // With a fieldbase object, convert to the sql type.
    public String getSqlType(SqlType type, Object value) {
        return type.getSqlField(value);
    }

    public boolean hasParent() {
        if (parent != null) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }
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

    public Map<String, FieldBase> getOrderedFields() {
        if (orderedFields == null) {
            orderFields();
        }
        return orderedFields;
    }

    protected void orderFields() {
        if (this.fields != null && this.order != null) {
            orderedFields = new LinkedHashMap<String, FieldBase>();
            if (hasParent()) {
                for (String parentKey: getParent().getKeyFields()) {
                    String[] searchFieldName = parentKey.split("\\.");
                    boolean found = Boolean.FALSE;
                    for (FieldBase field : getParent().getFields()) {
                        if (field.getName().equals(searchFieldName[0])) {
                            orderedFields.put(parentKey, field);
                            if (searchFieldName.length > 1) {
                                field.setMaintainState(true);
                                if (field.getRepeat() > 1) {
                                    throw new RuntimeException("Field: " + field.getName() + " with Ordering: " +
                                            parentKey + " is set for 'repeat' AND with 'state' (like start/stop).  This isn't supported");
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
            if (getRelationships() != null) {
                for (Map.Entry<String, Relationship> entry : getRelationships().entrySet()) {
                    entry.getValue().getRecord().orderFields();
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

    public void next() throws TerminateException {
        // Clear Maps holding previous record.
        keyMap.clear();
        valueMap.clear();
        if (hasParent() && getParent().getKeyMap() != null) {
            keyMap.putAll(getParent().getKeyMap());
            valueMap.putAll(getParent().getKeyMap());
        }

        Map<String, FieldProperties> fieldNextValues = new TreeMap<String, FieldProperties>();
        for (FieldBase field : fields) {
            Object value = field.getNext();
            FieldProperties fp = field.getFieldProperties();
            fieldNextValues.put(field.getName(), fp);
        }

        Iterator<String> iFieldKeys = orderedFields.keySet().iterator();

        Map<String, Object> keys = null;

        while (iFieldKeys.hasNext()) {
            String iFieldKey = iFieldKeys.next();

            String[] fieldNameParts = iFieldKey.split("\\.");
            String state = (fieldNameParts.length > 1 ? fieldNameParts[1] : null);
            // Get the FieldProperties from the Map.
            FieldProperties fp = fieldNextValues.get(fieldNameParts[0]);
            if (fp != null) {
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
            } else {
                // The key has already been addressed from the parent.
            }

        }

        if (controlFieldInt != null && controlFieldInt.terminate()) {
            throw new TerminateException("Field " + controlField + " has reached it limit and terminated the record generating process");
        }
    }

    private void link(Schema schema, String id) {
        schema.setId(id);
        if (schema.getRelationships() != null) {
            Set<String> relationshipKeys = schema.getRelationships().keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = schema.getRelationships().get(key);
                Schema rSchema = relationship.getRecord();
                rSchema.setParent(schema);
                link(rSchema, key);
            }
        }

    }

    public void link(String id) {
        this.setId(id);
        if (this.getRelationships() != null) {
            Set<String> relationshipKeys = this.getRelationships().keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = this.getRelationships().get(key);
                Schema rSchema = relationship.getRecord();
                rSchema.setParent(this);
                link(rSchema, key);
            }
        }
        orderFields();
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
