package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

@JsonIgnoreProperties({ "outputBuffers" })
public class Output {
    // Output format of record.
    private OutputFormat format;
    private String baseDirectory;
    private boolean timestampFile = false;
    private String timestamp = null;
    private String delimiter = "\t";
    private String newLine = "\n";
    private Map<String, BufferedWriter> outputBuffers = new TreeMap<String, BufferedWriter>();

    public OutputFormat getFormat() {
        return format;
    }

    public void setFormat(OutputFormat format) {
        this.format = format;
    }

    public String getBaseDirectory() {
        return baseDirectory;
    }

    public void setBaseDirectory(String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    public boolean isTimestampFile() {
        return timestampFile;
    }

    public void setTimestampFile(boolean timestampFile) {
        this.timestampFile = timestampFile;
        if (isTimestampFile() && timestamp == null) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-MM");
            timestamp = df.format(new Date());
        }
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getNewLine() {
        return newLine;
    }

    public void setNewLine(String newLine) {
        this.newLine = newLine;
    }

    public BufferedWriter getWriter(String name) {
        // TODO: May need to create directory.
        if (outputBuffers.containsKey(name)) {
            return outputBuffers.get(name);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(baseDirectory);
            sb.append(System.getProperty("path.separator"));
            sb.append(name);
            if (isTimestampFile()) {
                sb.append("_");
                sb.append(timestamp);
            }
            sb.append(format.toString().toLowerCase());
            BufferedWriter writer = null;
            try {
                writer = new BufferedWriter(new FileWriter(sb.toString()));
            } catch (IOException e) {
                throw new RuntimeException(e);
//                e.printStackTrace();
            }
            outputBuffers.put(name, writer);
            return writer;
        }
    }
}
