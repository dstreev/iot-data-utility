package com.streever.iot.data.utility.generator;

public class SqlBuilder {
    public enum SqlType {
        HIVE;
    }

    private Schema schema;
    private SqlType sqlType;

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

    public void build() {

    }
}
