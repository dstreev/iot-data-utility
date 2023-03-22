package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;

public class ReferenceStringField extends ReferenceField<String> {

    @Override
    public FieldType getFieldType() {
        return FieldType.STRING;
    }

    protected Pool<String> getPool() {
        if (pool == null)
            pool = new Pool<String>();
        return pool;
    }
}
