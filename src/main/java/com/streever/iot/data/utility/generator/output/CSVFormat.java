package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.fields.FieldProperties;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CSVFormat extends FormatBase {
    private String separator = ",";
    private String newLine = "\n";
    private String quoteChar = "\"";

    public String getExtension() {
        return "csv";
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getNewLine() {
        return newLine;
    }

    public void setNewLine(String newLine) {
        this.newLine = newLine;
    }

    public String getQuoteChar() {
        return quoteChar;
    }

    public void setQuoteChar(String quoteChar) {
        this.quoteChar = quoteChar;
    }

    public static String getLine(Map<FieldProperties, Object> record, String quote, String separator) {
        List<String> values = new ArrayList<String>();
        Set<Map.Entry<FieldProperties, Object>> entries = record.entrySet();
        for (Map.Entry<FieldProperties, Object> entry: entries) {
            if (entry.getKey().isNumber()) {
                values.add(entry.getValue().toString());
            } else {
                // TODO: Need to encode/escape special characters.
                values.add(quote + entry.getValue().toString() + quote);
            }
        }
        String recLine = StringUtils.join(values, separator);
        return recLine;
    }

    public String write(Map<FieldProperties, Object> record) throws IOException {
        String rtn = null;
//        if (isOpen()) {
            rtn = getLine(record, getQuoteChar(), getSeparator());
//            rtn = recLine.length() + 1;
//            writeLine(recLine);
//        } else {
            // TODO: Throw not open exception.
//        }
        return rtn;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        CSVFormat clone = (CSVFormat)super.clone();
        clone.setNewLine(this.newLine);
        clone.setQuoteChar(this.quoteChar);
        clone.setSeparator(this.separator);
        return clone;
    }
}
