package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvWriteException;
import org.apache.commons.lang3.StringUtils;

public class CSVFormat extends FormatBase {
    private char separator = ',';
    //    private String newLine = "\n";
    private char quoteChar = '"';
    private char escapeChar = '\\';

    public String getExtension() {
        return "csv";
    }

    public char getSeparator() {
        return separator;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    public char getQuoteChar() {
        return quoteChar;
    }

    public void setQuoteChar(char quoteChar) {
        this.quoteChar = quoteChar;
    }

    public char getEscapeChar() {
        return escapeChar;
    }

    public void setEscapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    private CsvSchema csvSchema = null;
    private CsvMapper csvMapper = new CsvMapper();

    private String getLine(ObjectNode node) {
        // TODO: Doesn't support nested Objects.
        //
        // WARNING: We might have an issue with nested items.  When they aren't present
        //          how do we consistently ensure we have a 'placeholder' for it.
        if (csvSchema == null) {
            CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
            node.fieldNames().forEachRemaining(fieldName -> {
                csvSchemaBuilder.addColumn(fieldName);
            });
            csvSchema = csvSchemaBuilder.build().withColumnSeparator(separator)
                    .withQuoteChar(quoteChar).withEscapeChar(escapeChar);
        }

        try {
            // chop removes last character, which is a line feed in this case.
            return StringUtils.chop(csvMapper.writerFor(JsonNode.class)
                    .with(csvSchema).writeValueAsString(node));
        } catch (JsonProcessingException e) {
            if (e instanceof CsvWriteException) {
                System.err.println("We can't support csv output for nested records.  Use the `-json` format.");
            }
            throw new RuntimeException(e);
        }
    }

    public String format(ObjectNode node) {
        String rtn = null;
        rtn = getLine(node);
        return rtn;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        CSVFormat clone = (CSVFormat) super.clone();
        clone.setQuoteChar(this.quoteChar);
        clone.setSeparator(this.separator);
        return clone;
    }
}
