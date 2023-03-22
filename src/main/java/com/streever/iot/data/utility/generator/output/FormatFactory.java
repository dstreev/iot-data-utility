package com.streever.iot.data.utility.generator.output;

public class FormatFactory {
    public static Format getFormatImplementation(String type) {
        Format format = null;
        switch (type.toUpperCase()) {
            case "CSV":
                format = new CSVFormat();
                break;
            case "JSON":
                format = new JSONFormat();
                break;
            case "YAML":
            case "YML":
                format = new YAMLFormat();
                break;
            default:
                throw new RuntimeException("Unknown format type: " + type + ". Use: CSV, JSON, YML, or YAML.");
        }
        return format;
    }
}
