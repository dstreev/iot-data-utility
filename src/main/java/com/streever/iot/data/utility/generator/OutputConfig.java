package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.output.LocalFileOutput;
import com.streever.iot.data.utility.generator.output.OutputBase;
import org.apache.commons.io.IOUtils;

import java.net.URL;

public class OutputConfig {
    private OutputBase config = new LocalFileOutput();

    public void setConfig(OutputBase config) {
        this.config = config;
    }

    public OutputBase getConfig() {
        return config;
    }

    public static OutputConfig deserialize(String configResource) {
        if (configResource == null) {
            return null;
        }
        OutputConfig outputSpec = null;
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
                outputSpec = mapper.readerFor(OutputConfig.class).readValue(yamlConfigDefinition);
            } else {
                throw new RuntimeException("Couldn't locate 'Serialized Record File': " + configResource);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return outputSpec;
    }

}
