package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.fields.*;
import com.streever.iot.data.utility.generator.fields.support.GeoLocation;
import com.streever.iot.data.utility.generator.output.CSVFormat;
//import com.sun.org.apache.xpath.internal.operations.String;
import com.streever.iot.data.utility.generator.output.Format;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.*;

@JsonIgnoreProperties({"format", "orderedFields", "cardinality", "parent", "id", "keyHash", "keyMap", "valueMap",
        "relationships", "pathMap", "fieldPropertiesMap", "records", "randomizer", "idFields", "idFieldPositions"})
public class Schema implements Comparable<Schema> {
    protected ObjectMapper om = new ObjectMapper();
    private Format format;
    private String id;
    private String title;
    private String description;
    //    private Schema parent;
    private Boolean partitioned = Boolean.FALSE;
//    private Cardinality cardinality;

//    private Map<Schema, String> pathMap = null;
    @JsonIgnore
    /*
    Lookup values
     */
    private List<List<Object>> records = null;
    @JsonIgnore
    private Map<String, FieldProperties> fieldPropertiesMap = new LinkedHashMap<String, FieldProperties>();
    private String controlField;
    private ControlField controlFieldInt;
    private CSVFormat output;
    private List<String> order;

//    public Schema getParent() {
//        return parent;
//    }

//    public void setParent(Schema parent) {
//        this.parent = parent;
//    }
    private Map<String, FieldBase> orderedFields;
    private JsonFactory jFactory = new JsonFactory();

//    public Cardinality getCardinality() {
//        return cardinality;
//    }

//    public void setCardinality(Cardinality cardinality) {
//        this.cardinality = cardinality;
//    }

    // Used to control if record gen is terminated
    // because of the control fields state.
    private List<FieldBase> fields;
    //    private Map<FieldProperties, Object> keyMap = new LinkedHashMap<FieldProperties, Object>();
    private int keyHash = 0;
    private Map<FieldProperties, Object> valueMap = new LinkedHashMap<FieldProperties, Object>();
    private Random randomizer = null;
    private Integer[] idFieldPositions = null;

    private Map<String, Relationship> relationships = new LinkedHashMap<String, Relationship>();

    private IdField[] idFields = null;

    public Map<String, Relationship> getRelationships() {
        return relationships;
    }

    public static Schema deserializeInputStream(InputStream inputStream) throws IOException {
        Schema recDef = null;
        ObjectMapper mapper = null;
//        if ("yaml".equals(extension.toLowerCase()) || "yml".equals(extension.toLowerCase())) {
        mapper = new ObjectMapper(new YAMLFactory());
//        } else if ("json".equals(extension.toLowerCase()) || "jsn".equals(extension.toLowerCase())) {
//            mapper = new ObjectMapper(new JsonFactory());
//        } else {
//            throw new RuntimeException(configResource + ": can't determine type by extension.  Require one of: ['yaml',yml,'json','jsn']");
//        }

        // Try as a Resource (in classpath)
//        URL configURL = mapper.getClass().getResource(configResource);
//        if (configURL != null) {
//            // Convert to String.
//            String configDefinition = IOUtils.toString(configURL, "UTF-8");
//            recDef = mapper.readerFor(Schema.class).readValue(configDefinition);
//        } else {
//            // Try on Local FileSystem.
//            configURL = new URL("file", null, configResource);
//            if (configURL != null) {
//                String configDefinition = IOUtils.toString(configURL, "UTF-8");
//                recDef = mapper.readerFor(Schema.class).readValue(configDefinition);
        recDef = mapper.readerFor(Schema.class).readValue(new BufferedReader(new InputStreamReader(inputStream)));
//            } else {
//                throw new RuntimeException("Couldn't locate 'Serialized Record File': " + configResource);
//            }
//        }

        return recDef;

    }

    public static Schema deserializeResource(String configResource) throws IOException, JsonMappingException {
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

    public Map<String, FieldProperties> getFieldPropertiesMap() {
        return fieldPropertiesMap;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Boolean getPartitioned() {
        return partitioned;
    }

    public void setPartitioned(Boolean partitioned) {
        this.partitioned = partitioned;
    }

    public String getControlField() {
        return controlField;
    }

//    public Map<String, Relationship> getRelationships() {
//        if (relationships == null)
//            relationships = new TreeMap<String, Relationship>();
//        return relationships;
//    }

//    public void setRelationships(Map<String, Relationship> relationships) {
//        this.relationships = relationships;
//    }

    public void setControlField(String controlField) {
        this.controlField = controlField;
    }

    public String getTitle() {
        return title.toLowerCase();
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

//    public boolean hasParent() {
//        if (parent != null) {
//            return Boolean.TRUE;
//        } else {
//            return Boolean.FALSE;
//        }
//    }

    public void setDescription(String description) {
        this.description = description;
    }

    public CSVFormat getOutput() {
        return output;
    }

//    public Map<Schema, String> getPathMap() {
//        return pathMap;
//    }

    public void setOutput(CSVFormat output) {
        this.output = output;
    }

    public List<FieldBase> getFields() {
        return fields;
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

        // Build fieldPropertiesMap
        Map<String, FieldProperties> fieldNextValues = new TreeMap<String, FieldProperties>();
        for (FieldBase field : fields) {
//            Object value = field.getNext();
            FieldProperties fp = field.getFieldProperties();
            fieldNextValues.put(field.getName(), fp);
        }

        Iterator<String> iFieldKeys = orderedFields.keySet().iterator();

        int fieldPos = 0;
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
                        fieldPropertiesMap.put(fieldNameParts[0] + "_" + state, stateFp);
//                            valueMap.put(stateFp, fp.getField().getNextStateValue(state));
                    } else {
                        // Shouldn't happen.
                    }
                } else if (fp.getField().getRepeat() > 1) {
                    for (int i = 1; i < fp.getField().getRepeat(); i++) {
                        // TODO: Need to clone field and set positions.
                        String repeater = StringUtils.leftPad(Integer.toString(i), 3, '0');
                        FieldProperties repeatFp = new FieldProperties(fieldNameParts[0] + "_" + repeater, fp.getField());
                        fieldPropertiesMap.put(fieldNameParts[0] + "_" + repeater, repeatFp);
//                            valueMap.put(repeatFp, fp.getField().getNext());
                    }
                } else {
//                    if (keyFields != null && keyFields.contains(fp.getName())) {
//                        Object key = fp.getField().getLast();
//                        keySb.append(key.toString());
//                        keyMap.put(fp, key);
//                    }
                    fp.getField().setFieldNum(fieldPos++);
                    fieldPropertiesMap.put(fieldNameParts[0], fp);
//                        valueMap.put(fp, fp.getField().getLast());
                }
            } else {
                // The key has already been addressed from the parent.
            }
        }
    }

    // With a fieldbase object, convert to the sql type.
    public String getSqlType(SqlType type, Object value) {
        return type.getSqlField(value);
    }

    public int getKeyHash() {
        return keyHash;
    }

    public void setKeyHash(int keyHash) {
        this.keyHash = keyHash;
    }

    /*
    Use this to loop through the fields and relationships to validate the configurations.
     */
    public Boolean validate() {
        return validate(null);
    }

//    public Map<FieldProperties, Object> getKeyMap() {
//        return keyMap;
//    }

    protected Boolean validate(List<String> reasons) {
        boolean rtn = Boolean.TRUE;
        for (FieldBase field : this.getFields()) {
            if (!field.validate(reasons)) {
                rtn = Boolean.FALSE;
            }
        }
        for (IdField idField: this.getIdFields()) {
            if (idField.getIdType() == null) {
                rtn = Boolean.FALSE;
                StringBuilder sb = new StringBuilder();
                sb.append("Schema: " + getTitle() + " with ID field: " + idField.getName() + " doesn't have " +
                        "it's idType defined. This may be due to the lack of a relationship that defines the ID field");
                reasons.add(sb.toString());
            }
        }
        return rtn;
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
//            if (getRelationships() != null) {
//                for (Map.Entry<String, Relationship> entry : getRelationships().entrySet()) {
//                    entry.getValue().getRecord().orderFields();
//                }
//            }
        }
    }

    public Map<FieldProperties, Object> getValueMap() {
        return valueMap;
    }

    public List<List<Object>> getRecords() {
        return records;
    }

    public void setRecords(List<List<Object>> records) {
        this.records = records;
    }

    private Random getRandomizer() {
        if (randomizer == null) {
            randomizer = new Random(new Date().getTime());
        }
        return randomizer;
    }

    protected List<Object> getStaticRecord() {
        List<Object> rtn = null;
        if (getRecords() != null) {
            int staticRecordCount = getRecords().size();
            int recordNum = getRandomizer().nextInt(staticRecordCount);
            rtn = getRecords().get(recordNum);
        }
        return rtn;
    }

    public Boolean isStatic() {
        if (records != null) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }

    }
    public Integer[] getIdFieldPositions() {
        if (idFieldPositions == null) {
            List<Integer> positionList = new ArrayList<Integer>();
            List<IdField> idFieldList = new ArrayList<IdField>();
            int pos = 0;
            for (String fieldName: getFieldPropertiesMap().keySet()) {
                FieldBase field = getFieldPropertiesMap().get(fieldName).getField();
                if (field instanceof IdField) {
                    IdField iField = (IdField)field;
                    if (iField.getIdType() != RelationshipType.SOURCE ||
                    iField.getParent().isStatic()) {
                        positionList.add(pos);
                        idFieldList.add((IdField) field);
                    }
                }
                pos++;
            }
            idFieldPositions = positionList.toArray(new Integer[0]);
            idFields = idFieldList.toArray(idFieldList.toArray(new IdField[0]));
        }
        return idFieldPositions;
    }

    /*
    When this schema is a Lookup for a relationship schema, find the id and get the id value from the static record.
     */
    public IdField[] getIdFields() {
        if (idFields == null) {
            getIdFieldPositions();
        }
        return idFields;
    }

    public Object[] getLookUpIdValues() {
        List<Object> staticRecord = getStaticRecord();
        Integer[] idPositions = getIdFieldPositions();
        Object[] rtn = new Object[idPositions.length];
        int lPos = 0;
        for (Integer position: idPositions) {
            rtn[lPos++] = staticRecord.get(position);
        }
        return rtn;
    }

    public void getRecordNode(ObjectNode node) throws TerminateException {
//        int recordSize = 0;
        Map<String, FieldProperties> fieldPropertiesMap = getFieldPropertiesMap();

        // Static Random Record from records.
        List<Object> staticRecord = getStaticRecord();

        int fieldPos = 0;
        Iterator<String> iFieldKeys = fieldPropertiesMap.keySet().iterator();
        while (iFieldKeys.hasNext()) {
            String fieldKey = iFieldKeys.next();
            FieldProperties fp = fieldPropertiesMap.get(fieldKey);
            // Testing for Id fields that are a CHILD.  If they are, don't add field to node because this
            //  record is a child of the parent record, where the id is.
            if (fp.getField() instanceof IdField) {
                IdField idField = (IdField)fp.getField();
                if (idField.getIdType() == RelationshipType.CHILD)
                    continue;
            }
            Object value = null;
            // When the static record is set, use that value.
            if (staticRecord == null) {
                value = fp.getField().getNext();
            } else {
                value = staticRecord.get(fieldPos++);
            }
//            recordSize += value.toString().length();
            switch (fp.getField().getFieldType()) {
                case LONG:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (Long lng: (List<Long>)value) {
                            lngArrayNode.add(lng);
                        }
                    } else {
                        node.put(fp.getName(), (Long) value);
                    }
                    break;
                case FLOAT:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (Float av: (List<Float>)value) {
                            lngArrayNode.add(av);
                        }
                    } else {
                        node.put(fp.getName(), (Float) value);
                    }
                    break;
                case SHORT:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (Short av: (List<Short>)value) {
                            lngArrayNode.add(av);
                        }
                    } else {
                        node.put(fp.getName(), (Short) value);
                    }
                    break;
                case DOUBLE:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (Double av: (List<Double>)value) {
                            lngArrayNode.add(av);
                        }
                    } else {
                        node.put(fp.getName(), (Double) value);
                    }
                    break;
                case STRING:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (String av: (List<String>)value) {
                            lngArrayNode.add(av);
                        }
                    } else {
                        node.put(fp.getName(), (String) value);
                    }
                    break;
                case GEOLOCATION:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (GeoLocation av: (List<GeoLocation>)value) {
                            final ObjectNode geoNode = lngArrayNode.objectNode();
                            geoNode.put("lat", av.getLatitude());
                            geoNode.put("long", av.getLongitude());
                            lngArrayNode.add(geoNode);
                        }
                    } else {
                        final ObjectNode geoNode = node.objectNode();
                        geoNode.put("lat", ((GeoLocation)value).getLatitude());
                        geoNode.put("long", ((GeoLocation)value).getLongitude());
                        node.put(fp.getName(), geoNode);
                    }
                    break;
                case BOOLEAN:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (Boolean av: (List<Boolean>)value) {
                            lngArrayNode.add(av);
                        }
                    } else {
                        node.put(fp.getName(), (Boolean) value);
                    }
                    break;
                case INTEGER:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (Integer av: (List<Integer>)value) {
                            lngArrayNode.add(av);
                        }
                    } else {
                        node.put(fp.getName(), (Integer) value);
                    }
                    break;
                case BIGDECIMAL:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (BigDecimal av: (List<BigDecimal>)value) {
                            lngArrayNode.add(av);
                        }
                    } else {
                        node.put(fp.getName(), (BigDecimal) value);
                    }
                    break;
                case BIGINTEGER:
                    if (value instanceof List) {
                        final ArrayNode lngArrayNode = node.putArray(fp.getName());
                        for (BigInteger av: (List<BigInteger>)value) {
                            lngArrayNode.add(av);
                        }
                    } else {
                        node.put(fp.getName(), (BigInteger) value);
                    }
                    break;
            }
        }
        // TODO: Deal with compound (start/stop) fields.
//        return recordSize;
    }

    public void next() throws TerminateException {
        // Clear Maps holding previous record.
//        keyMap.clear();
        valueMap.clear();
//        if (hasParent() && getParent().getKeyMap() != null) {
//            keyMap.putAll(getParent().getKeyMap());
//            valueMap.putAll(getParent().getKeyMap());
//            keyHash = getParent().keyHash;
//        }
        Map<String, FieldProperties> fieldNextValues = new TreeMap<String, FieldProperties>();
        for (FieldBase field : fields) {
            Object value = field.getNext();
            FieldProperties fp = field.getFieldProperties();
            fieldNextValues.put(field.getName(), fp);
        }

        Iterator<String> iFieldKeys = orderedFields.keySet().iterator();

//        Map<String, Object> keys = null;
//        StringBuilder keySb = new StringBuilder();

        // If the records object is set, it means this is a lookup dataset that was populated
        //    statically.
        if (records == null) {
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
//                    if (keyFields != null && keyFields.contains(fp.getName())) {
//                        Object key = fp.getField().getLast();
//                        keySb.append(key.toString());
//                        keyMap.put(fp, key);
//                    }
                        valueMap.put(fp, fp.getField().getLast());
                    }
                } else {
                    // The key has already been addressed from the parent.
                }
            }
            if (controlFieldInt != null && controlFieldInt.terminate()) {
                throw new TerminateException("Field " + controlField + " has reached it limit and terminated the record generating process");
            }
        } else {
            // Randomly select a list from records.
            Random random = fields.get(0).getRandomizer();
            int loc = random.nextInt(records.size());
            List<Object> staticRecord = records.get(loc);
            int fieldLoc = 0;
            // Assumes the order of the items in the list is the same as the
            // field order in the schema.
            for (String fieldKey : orderedFields.keySet()) {
                FieldProperties fp = fieldNextValues.get(fieldKey);
                valueMap.put(fp, staticRecord.get(fieldLoc++));
            }
        }
    }

//    public void link() {
//        link(null);
//    }
//
//    private void link(String id) {
//        if (id == null)
//            this.setId(getTitle().toLowerCase(Locale.ROOT));
//        else
//            this.setId(id);
//        if (getRelationships() != null) {
//            Set<String> relationshipKeys = getRelationships().keySet();
//            for (String key : relationshipKeys) {
//                Relationship relationship = getRelationships().get(key);
//                Schema rSchema = relationship.getRecord();
//                rSchema.setParent(this);
//                rSchema.link(key);
//            }
//        }
//        orderFields();
//    }

//    public String write() {
//        String rtn = null;
//        if (format != null) {
//            rtn = format.format(getValueMap());
//        }
//        return rtn;
//    }

    public int getMaxFileParts(int mappers) {
        int rtn = mappers;
//        for (Map.Entry<String, Relationship> entry : relationships.entrySet()) {
//            Relationship relationship = entry.getValue();
//            Schema schema = relationship.getRecord();
//            int range = relationship.getCardinality().getRange(); //getMax() - relationship.getCardinality().getMin();
//            double filepartBase = Math.log((double) range) * mappers;
//            int filepartBaseInt = new Double(filepartBase).intValue();
//            if (filepartBaseInt > rtn)
//                rtn = filepartBaseInt;
//        }
        return rtn;
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


}
