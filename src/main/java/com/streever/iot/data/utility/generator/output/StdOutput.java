package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Schema;
import com.streever.iot.data.utility.generator.fields.FieldProperties;

import java.io.IOException;
import java.util.Map;

public class StdOutput extends OutputBase {
    enum STD_TYPE {
        out, err;
    }
    private STD_TYPE std = STD_TYPE.out;
    private boolean prefix = Boolean.TRUE;

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
    public void link(Schema record) {
        // Nothing needed.
    }

    @Override
    protected void writeLine(String line) throws IOException {
        switch (std) {
            case out:
                System.out.println(line);
                break;
            case err:
                System.err.println(line);
        }
    }


    //    @Override
//    public long write(Map<FieldProperties, Object> record) {
//        String line = null;
//        long rtn = 0;
//        switch (format) {
//            case csv:
//                line = CSVFormat.getLine(record, "\"", ",");
//                break;
//            case json:
//                line = JSONFormat.getLine(record);
//                break;
//        }
//        if (prefix) {
//            line = getName() + "-->" + line;
//        }
//        rtn = line.length();
//        switch (std) {
//            case out:
//                System.out.println(line);
//                break;
//            case err:
//                System.err.println(line);
//                break;
//        }
//        return rtn;
//    }
//
    @Override
    public boolean open(String prefix) {
        return true;
    }

    @Override
    public boolean close() {
        return true;
    }

}
