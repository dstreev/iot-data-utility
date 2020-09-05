package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;

public class RecordTest {

    @Before
    public void before() {
    }

    @Test
    public void loadTest_001() {
        Record r1 = RecordTest.deserialize("/generator_v2/cc_account_with_relationships.yaml");
//        Record r1 = deserialize("/generator_v2/cc_account.yaml");
        System.out.println("Made it");
//        assertTrue()
    }

    @Test
    public void recordTest_001() {
        Record record = RecordTest.deserialize("/generator_v2/cc_account.yaml");
        try {
            for (int i = 0; i < 10; i++) {
                record.next(null);
                Map keys = record.getKeyMap();
                Map values = record.getValueMap();
                System.out.println("KeyMap:" + keys.toString());
                System.out.println("ValueMap:" + values.toString());
            }
        } catch (TerminateException te) {

        }
    }

    public static Record deserialize(String configResource) {
        Record recDef = null;
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
                // Convert to String.
                String yamlConfigDefinition = IOUtils.toString(configURL,"UTF-8");
                recDef = mapper.readerFor(Record.class).readValue(yamlConfigDefinition);
            } else {
                throw new RuntimeException("Couldn't locate 'Serialized Record File': " + configResource);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return recDef;
    }

}