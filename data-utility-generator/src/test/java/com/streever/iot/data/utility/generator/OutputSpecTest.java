package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.*;

public class OutputSpecTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void loadDefaultSpec_001() {
        OutputSpec o1 = OutputSpecTest.deserialize("/outputspec/default.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadRelationSpec_001() {
        OutputSpec o1 = OutputSpecTest.deserialize("/outputspec/relationship001.yaml");
        System.out.println("Made it");
    }

    public static OutputSpec deserialize(String configResource) {
        OutputSpec outputSpec = null;
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        StackTraceElement et = stacktrace[2];//maybe this number needs to be corrected
        String methodName = et.getMethodName();
        System.out.println("=========================");
        System.out.println("Build Method: " + methodName);
        System.out.println("-------------------------");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {
            URL configURL = mapper.getClass().getResource(configResource);
            if (configURL != null) {
                String yamlConfigDefinition = IOUtils.toString(configURL);
                outputSpec = mapper.readerFor(OutputSpec.class).readValue(yamlConfigDefinition);
            } else {
                throw new RuntimeException("Couldn't locate 'Serialized Record File': " +
                        configURL.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return outputSpec;
    }

}
