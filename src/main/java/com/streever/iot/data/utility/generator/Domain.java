package com.streever.iot.data.utility.generator;

import com.amazonaws.thirdparty.joda.time.format.DateTimeFormat;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.common.TokenReplacement;
import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.IdField;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Domain {
    private List<Schema> schemas = new ArrayList<Schema>();

    private List<Relationship> relationships = new ArrayList<Relationship>();

    private Map<String, List<List<Object>>> lookup = new HashMap<String, List<List<Object>>>();

    public static Domain deserializeInputStream(Map<String, Object> tokens, InputStream inputStream) throws IOException {
        Domain domain = null;
        ObjectMapper mapper = null;
        String configDefinition = null;

        mapper = new ObjectMapper(new YAMLFactory());

        List<String> configLines = new ArrayList<String>();

        LineIterator li = IOUtils.lineIterator(inputStream, "UTF-8");
        while (li.hasNext()) {
            configLines.add(li.nextLine());
        }

        /*
        Now that we have the config in a List<String>, go through it and look for tokens.

        Tokens in the format ${token:value} are a default. And added to the default token list.

        That token map is condensed with the input tokens and applied to the list.  The result is
        resolved List<String> which we turn into a String and deserialize the Domain with the
        parameters
         */
        Map<String, Object> defaultTokens = TokenReplacement.getInstance().getDefaultTokens(configLines);
        Map<String, Object> condensedTokens = condense(defaultTokens, tokens);

        setSpecialTokens(condensedTokens);

        List<String> resolvedConfigLines = TokenReplacement.getInstance().replace(configLines, condensedTokens);

        configDefinition = resolvedConfigLines.stream().map(n -> String.valueOf(n)).collect(Collectors.joining("\n"));
        domain = mapper.readerFor(Domain.class).readValue(configDefinition);

        return domain;
    }

    public static Domain deserializeResource(Map<String, Object> tokens, String configResource) throws IOException, JsonMappingException {
        Domain domain = null;
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
        if (configURL == null) {
            configURL = new URL("file", null, configResource);
            if (configURL == null) {
                throw new RuntimeException("Couldn't locate 'Serialized Record File': " + configResource);
            }
        }

        InputStream inputStream = configURL.openStream();
        try {
            domain = deserializeInputStream(tokens, inputStream);
        } finally {
            inputStream.close();
        }

        return domain;
    }

    protected static void setSpecialTokens(Map<String, Object> tokenMap) {
        Date now = new Date();
//        tokenMap.put("now-default", now.toString());
        tokenMap.put("now", new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss").format(now));
        tokenMap.put("now-yyyy-MM-dd", new SimpleDateFormat("yyyy-MM-dd").format(now));
        tokenMap.put("now-yyyy-MM-dd hh:mm:ss", new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss").format(now));
    }

    protected static Map<String, Object> condense(Map<String, Object>... mapSet) {
        Map<String, Object> rtn = new HashMap<String, Object>();
        for (Map<String, Object> map : mapSet) {
            if (map != null) {
                for (String key : map.keySet()) {
                    rtn.put(key, map.get(key));
                }
            }
        }
        return rtn;
    }

    public List<Schema> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<Schema> schemas) {
        this.schemas = schemas;
    }

    public List<Relationship> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<Relationship> relationships) {
        this.relationships = relationships;
        for (Relationship relationship : this.relationships) {
            relationship.setDomain(this);
        }
    }

    public Map<String, List<List<Object>>> getLookup() {
        return lookup;
    }

    public void setLookup(Map<String, List<List<Object>>> lookup) {
        this.lookup = lookup;
    }

    /*
    This is where we'll link all the schemas together.
     */
    public Boolean completeAssociations() {
        // Link Id's to Schema's
        for (Schema schema : getSchemas()) {
            for (FieldBase field : schema.getFields()) {
                field.setParent(schema);
            }
        }

        // Resolve Relationships
        for (Relationship relationship : relationships) {
            String[] from = relationship.getFrom().split("\\.");
            String[] to = relationship.getTo().split("\\.");
            assert from.length == 2 : "invalidate format for 'from' reference: " + relationship.getFrom();
            assert to.length == 2 : "invalidate format for 'from' reference: " + relationship.getTo();
            // Use the first element to location the schema.
            Schema fromSchema = null;
            Schema toSchema = null;
            for (Schema schema : this.getSchemas()) {
                if (schema.getTitle().equals(from[0])) {
                    fromSchema = schema;
                    relationship.setFromSchema(fromSchema);
                }
                if (schema.getTitle().equals(to[0])) {
                    toSchema = schema;
                    relationship.setToSchema(toSchema);
                }
            }
            // Check that both have been resolved.
            assert fromSchema != null : "(from)Couldn't resolve " + from[0] + " to a schema.title";
            assert toSchema != null : "(to)Couldn't resolve " + to[0] + " to a schema.title";

            IdField fromId = null;
            IdField toId = null;
            for (FieldBase field : fromSchema.getFields()) {
                if (field.getName().equals(from[1])) {
                    if (field instanceof IdField) {
                        fromId = (IdField) field;
                    } else {
                        throw new RuntimeException("Schema: " + fromSchema.getTitle() + " has a field called " + from[1] +
                                ", but it is not an IdField type.  Associations via the Relationships can't be made.");
                    }
                }
            }

            for (FieldBase field : toSchema.getFields()) {
                if (field.getName().equals(to[1])) {
                    if (field instanceof IdField) {
                        toId = (IdField) field;
                    } else {
                        throw new RuntimeException("Schema: " + toSchema.getTitle() + " has a field called " + to[1] +
                                ", but it is not an IdField type.  Associations via the Relationships can't be made.");
                    }
                }
            }

            if (relationship.getCardinality() != null && relationship.getCardinality().getRange() > 0) {
                fromSchema.getRelationships().put(toSchema.getTitle() + "s", relationship);
            }

            assert toId != null : "Couldn't resolve to.id for: " + relationship.getTo();
            assert fromId != null : "Couldn't resolve from.id for: " + relationship.getFrom();

//            assert fromId.getIdType() != null: "The 'from' id must have a 'type' set.";
            if (fromId.getIdType() == null) {
                if (toId.getIdType() != null && toId.getIdType() == RelationshipType.SOURCE) {
                    // When a relationship is defined from a reference table and the 'toId' is a
                    // SOURCE id type, set the 'fromId' to a REFERENCE
                    fromId.setIdType(RelationshipType.REFERENCE);
                    fromId.setRelationshipId(toId);
                } else {
                    // Happens for REFERENCE Id's. This is a reverse relationship.
                    toId.setIdType(RelationshipType.REFERENCE);
                    toId.setRelationshipId(fromId);
                }
            } else {
                switch (fromId.getIdType()) {
                    case SOURCE:
                        if (relationship != null && relationship.getCardinality() != null
                                && relationship.getCardinality().getRange() > 0)
                            toId.setIdType(RelationshipType.CHILD);
                        else
                            toId.setIdType(RelationshipType.REFERENCE);
                        toId.setRelationshipId(fromId);
                        break;
                    case TRANSACTIONAL:
                        toId.setIdType(RelationshipType.CHILD);
                        toId.setRelationshipId(fromId);
                        break;
                    case REFERENCE:
                    case LOOKUP:
                        fromId.setRelationshipId(toId);
                        toId.setIdType(RelationshipType.SOURCE);
                        break;
                    default:
                        // shouldn't happen.
                        throw new RuntimeException("From Relationships should only be one of: SOURCE, TRANSACTIONAL, LOOKUP, REFERENCE: " +
                                fromSchema.getTitle() + "." + fromId.getName());
                }
            }
        }

        for (String lookupKey : this.getLookup().keySet()) {
            // Look through schemas title for match.
            Boolean found = Boolean.FALSE;
            for (Schema schema : getSchemas()) {
                if (schema.getTitle().equals(lookupKey)) {
                    schema.setRecords(this.getLookup().get(lookupKey));
                    found = Boolean.TRUE;
                    break;
                }
            }
            assert found : "Couldn't match Lookup " + lookupKey + " to schema";
        }
        return Boolean.TRUE;
    }

    public Boolean validate(List<String> reasons) {
        // All ID Fields should have a type.
        Boolean rtn = Boolean.TRUE;
        for (Schema schema: this.schemas) {
            if (!schema.validate(reasons)) {
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }
}
