package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Record;
import com.streever.iot.data.utility.generator.fields.FieldBase;

import java.util.Map;

public class StdErrOutput extends OutputBase {

    @Override
    public void link(Record record) {
        // Nothing needed.
    }

    @Override
    public void write(Map<FieldBase, Object> record) {
        System.err.println(record.toString());
    }

    @Override
    public boolean open(String prefix) {
        return true;
    }

    @Override
    public boolean close() {
        return true;
    }

}
