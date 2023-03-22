package com.streever.iot.data.utility.generator;

import com.streever.iot.data.utility.generator.fields.FieldProperties;
import com.streever.iot.data.utility.generator.fields.SqlType;
import com.streever.iot.data.utility.generator.fields.TerminateException;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class HiveSqlBuilder implements SqlBuilder {
    private Domain domain;
//    private SqlType sqlType = SqlType.HIVE;
    private FileFormat sourceFileFormat = FileFormat.TEXTFILE;
    private FileFormat targetFileFormat = FileFormat.ORC;
    private String sourceDb = "sourceDb";
    private String targetDb = "targetDb";

    private Boolean buildSweepSql = Boolean.TRUE;

    @Override
    public Domain getDomain() {
        return domain;
    }

    @Override
    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    protected String createUseDb(String db) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE IF NOT EXISTS " + db).append(";\n");
        sb.append("USE " +db).append(";\n");
        return sb.toString();
    }

    @Override
    public String build() {
        // Initial returns for one record.
        StringBuilder sb = new StringBuilder();
        sb.append(createUseDb(sourceDb));
//        sb.append(writeCreateStatement(getDomain(), getDomain().getTitle(), sourceFileFormat, TableType.EXTERNAL)).append("\n\n");
        sb.append(createUseDb(targetDb));
//        sb.append(writeCreateStatement(getDomain(), getDomain().getTitle(), targetFileFormat, TableType.MANAGED)).append("\n\n");
//        sb.append(writeSweepStatement(getDomain(), getDomain().getTitle(), sourceDb, targetDb));
        return sb.toString();
    }

    protected String writeCreateStatement(Schema schema, String tablename, FileFormat fileFormat, TableType tableType) {
        StringBuilder sb = new StringBuilder();
        switch (tableType) {
            case EXTERNAL:
                sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
                break;
            case MANAGED:
                sb.append("CREATE TABLE IF NOT EXISTS ");
                break;
        }

        sb.append(tablename).append(" (\n");//
        // Go through the valuemap.
        // TODO: Use the keymap to create CONSTRAINTS.
        Iterator<FieldProperties> iValueKeys = schema.getValueMap().keySet().iterator();
        while (iValueKeys.hasNext()) {
            FieldProperties checkFp = iValueKeys.next();
            Object value = schema.getValueMap().get(checkFp);
//            sb.append("\t" + checkFp.getName() + " " + schema.getSqlType(this.sqlType, value));
            if (checkFp.getField().getDesc() != null) {
                sb.append(" COMMENT \"" + checkFp.getField().getDesc() + "\"");
            }
            if (iValueKeys.hasNext()) {
                sb.append(",\n");
            } else {
                sb.append(")\n");
            }
        }
        switch (tableType) {
            case EXTERNAL:
                sb.append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'").append("\n");
                sb.append("WITH SERDEPROPERTIES (").append("\n");
                sb.append("\"separatorChar\" = \",\",").append("\n");
                sb.append("\"quoteChar\"     = \"\\\"\",").append("\n");
                sb.append("\"escapeChar\"    = \"\\\\\")").append("\n");

                /*
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                    WITH SERDEPROPERTIES (
                       "separatorChar" = ",",
                       "quoteChar"     = "\"",
                       "escapeChar"    = "\\"
                    )
                 */
        }
        sb.append("STORED AS ");
        sb.append(fileFormat.toString());
        sb.append(";\n");
//        sb.append(writeCreateRelationshipStatements(schema.getRelationships(), fileFormat, tableType));
        return sb.toString();
    }

    protected String writeCreateRelationshipStatements(Map<String, Relationship> relationships, FileFormat fileFormat, TableType tableType) {
        StringBuilder sb = new StringBuilder();
        if (relationships != null) {
            Set<String> relationshipKeys = relationships.keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = relationships.get(key);
//                Schema rRecord = relationship.getRecord();
//                for (int i = 1;i <= relationship.getCardinality().getRepeat();i++) {
//                    String tablename = null;
//                    if (relationship.getCardinality().getRepeat() <= 1) {
//                        tablename = key;
//                    } else {
//                        tablename = key + "_" + StringUtils.leftPad(Integer.toString(i), 4, "0");
//                    }
//                    sb.append(writeCreateStatement(rRecord, tablename, fileFormat, tableType)).append("\n\n");
//                    // Recurse into hierarchy
//                    sb.append(writeCreateRelationshipStatements(rRecord.getRelationships(), fileFormat, tableType));
//                }
            }
        }
        return sb.toString();
    }

    protected String writeSweepStatement(Schema schema, String tableName, String sourceDb, String targetDb) {
        StringBuilder sb = new StringBuilder();

        sb.append("FROM ").append(sourceDb).append(".").append(tableName).append("\n");
        sb.append("INSERT INTO TABLE ");
        sb.append(targetDb).append(".").append(tableName).append("\n");
        sb.append("SELECT *").append(";\n\n");

//        sb.append(writeSweepRelationshipStatements(schema.getRelationships(), sourceDb, targetDb));
        return sb.toString();
    }

    protected String writeSweepRelationshipStatements(Map<String, Relationship> relationships, String sourceDb, String targetDb) {
        StringBuilder sb = new StringBuilder();
        if (relationships != null) {
            Set<String> relationshipKeys = relationships.keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = relationships.get(key);
//                Schema rRecord = relationship.getRecord();
//                for (int i = 1;i <= relationship.getCardinality().getRepeat();i++) {
//                    String tablename = null;
//                    if (relationship.getCardinality().getRepeat() <= 1) {
//                        tablename = key;
//                    } else {
//                        tablename = key + "_" + StringUtils.leftPad(Integer.toString(i), 4, "0");
//                    }
//                    sb.append(writeSweepStatement(rRecord, tablename, sourceDb, targetDb));
//                    // Recurse into hierarchy
//                    sb.append(writeSweepRelationshipStatements(rRecord.getRelationships(),sourceDb, targetDb));
//                }
            }
        }
        return sb.toString();
    }


}
