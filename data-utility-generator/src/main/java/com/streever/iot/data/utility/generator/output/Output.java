package com.streever.iot.data.utility.generator.output;

public class Output {
    private OutputFormat format;

    private String separator = ",";
    private String newLine = "\n";
    private String quoteChar = "\"";

    public OutputFormat getFormat() {
        return format;
    }

    public void setFormat(OutputFormat format) {
        this.format = format;
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
}
