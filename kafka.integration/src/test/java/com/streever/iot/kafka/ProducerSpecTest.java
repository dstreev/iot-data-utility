package com.streever.iot.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.output.kafka.ProducerSpec;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;

public class ProducerSpecTest {

    private ClassLoader cl;

    @Before
    public void before() {
        cl = getClass().getClassLoader();
    }


    @Test
    public void Test001() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            File file = new File(cl.getResource("outputspec/kafka-test.yaml").getFile());
            String jsonFromFile = FileUtils.readFileToString(file, Charset.forName("UTF-8"));

            ProducerSpec producerSpec = mapper.readerFor(ProducerSpec.class).readValue(jsonFromFile);
            System.out.println("Test001");
            System.out.println("Producer Spec isValid: " + producerSpec.isValid());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
