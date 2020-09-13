package com.streever.iot.data.utility.generator;

import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.FieldProperties;
import com.streever.iot.data.utility.generator.fields.SqlType;
import com.streever.iot.data.utility.generator.fields.TerminateException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class SqlBuilder {
    private Schema schema;
    private SqlType sqlType = SqlType.HIVE;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }

    public void link(String entityName) {
        if (schema != null) {
            schema.link(entityName);
        }
    }

    private void getNext(Schema schema) {
        try {
            schema.next();
        } catch (TerminateException e) {
            e.printStackTrace();
        }
        if (schema.getRelationships() != null) {
            for (Map.Entry<String, Relationship> entry : schema.getRelationships().entrySet()) {
                getNext(entry.getValue().getRecord());
            }
        }
    }

    public String build() {
        // Initial returns for one record.
        getNext(getSchema());
        StringBuilder sb = new StringBuilder();
        sb.append(write(getSchema())).append("\n\n");
        sb.append(writeRelationships(getSchema().getRelationships()));
        return sb.toString();
    }

    protected String write(Schema schema) {

//        writeRelationships(getSchema().getRelationships(), getSchema().getKeyMap());

//        Iterator<String> iFieldKeys = schema.getOrderedFields().keySet().iterator();

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE ");
        sb.append(schema.getId()).append(" \n");//
        // Go through the valuemap.
        // TODO: Use the keymap to create CONSTRAINTS.
        Iterator<FieldProperties> iValueKeys = schema.getValueMap().keySet().iterator();
        while (iValueKeys.hasNext()) {
            FieldProperties checkFp = iValueKeys.next();
            Object value = schema.getValueMap().get(checkFp);
            sb.append("\t" + checkFp.getName() + " " + schema.getSqlType(this.sqlType, value));
            if (checkFp.getField().getDesc() != null) {
                sb.append(" COMMENT \"" + checkFp.getField().getDesc() + "\"");
            }
            if (iValueKeys.hasNext()) {
                sb.append(",\n");
            } else {
                sb.append(");\n");
            }

        }
        return sb.toString();
    }

    protected String writeRelationships(Map<String, Relationship> relationships) {
        StringBuilder sb = new StringBuilder();
        if (relationships != null) {
            Set<String> relationshipKeys = relationships.keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = relationships.get(key);
                Schema rRecord = relationship.getRecord();
                sb.append(write(rRecord)).append("\n\n");
                // Recurse into hierarchy
                sb.append(writeRelationships(rRecord.getRelationships()));
            }
        }
        return sb.toString();
    }

}
