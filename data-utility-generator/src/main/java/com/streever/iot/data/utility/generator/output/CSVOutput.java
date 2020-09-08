package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Record;
import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.FieldProperties;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CSVOutput extends FileOutput {
    private String separator = ",";
    private String newLine = "\n";
    private String quoteChar = "\"";

    @Override
    protected String getExtension() {
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

    @Override
    public void write(Map<FieldProperties, Object> record) throws IOException {
        if (isOpen()) {
            List<String> values = new ArrayList<String>();
            Set<Map.Entry<FieldProperties, Object>> entries = record.entrySet();
            for (Map.Entry<FieldProperties, Object> entry: entries) {
                if (entry.getKey().isNumber()) {
                    values.add(entry.getValue().toString());
                } else {
                    // TODO: Need to encode/escape special characters.
                    values.add(getQuoteChar() + entry.getValue().toString() + getQuoteChar());
                }
            }
            String recLine = StringUtils.join(values, this.getSeparator());
            writeLine(recLine);
//            getWriteStream().println(recLine);
        } else {
            // TODO: Throw not open exception.
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        CSVOutput clone = (CSVOutput)super.clone();
        clone.setNewLine(this.newLine);
        clone.setQuoteChar(this.quoteChar);
        clone.setSeparator(this.separator);
        return clone;
    }
}
