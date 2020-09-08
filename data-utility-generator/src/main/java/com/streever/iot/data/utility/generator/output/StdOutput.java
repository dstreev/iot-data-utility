package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Record;
import com.streever.iot.data.utility.generator.fields.FieldProperties;

import java.util.Map;

public class StdOutput extends OutputBase {
    enum FORMAT_TYPE {
        csv,json;
    }
    enum STD_TYPE {
        out, err;
    }
    private FORMAT_TYPE format = FORMAT_TYPE.csv;
    private STD_TYPE std = STD_TYPE.out;
    private boolean prefix = Boolean.TRUE;

    public FORMAT_TYPE getFormat() {
        return format;
    }

    public void setFormat(FORMAT_TYPE format) {
        this.format = format;
    }

    public STD_TYPE getStd() {
        return std;
    }

    public void setStd(STD_TYPE std) {
        this.std = std;
    }

    public boolean isPrefix() {
        return prefix;
    }

    public void setPrefix(boolean prefix) {
        this.prefix = prefix;
    }

    @Override
    public void link(Record record) {
        // Nothing needed.
    }

    @Override
    public void write(Map<FieldProperties, Object> record) {
        String line = null;
        switch (format) {
            case csv:
                line = CSVOutput.getLine(record, "\"", ",");
                break;
            case json:
                line = JSONOutput.getLine(record);
                break;
        }
        if (prefix) {
            line = getName() + "-->" + line;
        }
        switch (std) {
            case out:
                System.out.println(line);
                break;
            case err:
                System.err.println(line);
                break;
        }
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
