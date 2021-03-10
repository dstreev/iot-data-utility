package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.output.FileOutput;
import com.streever.iot.data.utility.generator.output.OutputBase;
import org.apache.commons.io.IOUtils;

import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

public class OutputSpec {
    private OutputBase default_ = new FileOutput();
    private Map<String, OutputBase> relationships = new TreeMap<String, OutputBase>();

    public void setDefault(OutputBase default_) {
        this.default_ = default_;
    }

    @JsonProperty("default")
    public OutputBase getDefault() {
        return default_;
    }

    public Map<String, OutputBase> getRelationships() {
        return relationships;
    }

    public void setRelationships(Map<String, OutputBase> relationships) {
        this.relationships = relationships;
    }

    public static OutputSpec deserialize(String configResource) {
        if (configResource == null) {
            return null;
        }
        OutputSpec outputSpec = null;
//        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
//        StackTraceElement et = stacktrace[2];//maybe this number needs to be corrected
//        String methodName = et.getMethodName();
//        System.out.println("=========================");
//        System.out.println("Build Method: " + methodName);
//        System.out.println("-------------------------");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {
            URL configURL = mapper.getClass().getResource(configResource);
            if (configURL != null) {
                String yamlConfigDefinition = IOUtils.toString(configURL, "UTF-8");
                outputSpec = mapper.readerFor(OutputSpec.class).readValue(yamlConfigDefinition);
            } else {
                throw new RuntimeException("Couldn't locate 'Serialized Record File': " + configResource);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return outputSpec;
    }

}
