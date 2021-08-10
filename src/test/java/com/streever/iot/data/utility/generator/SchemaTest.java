package com.streever.iot.data.utility.generator;

import com.streever.iot.data.utility.generator.fields.TerminateException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class SchemaTest {

    @Before
    public void before() {
    }

    @Test
    public void loadTest_001() {
        try {
            Schema r1 = Schema.deserializeResource("/generator_v2/cc_account_with_relationships.yaml");
            r1.link();
            r1.validate(null);
            System.out.println("Made it");
        } catch (IOException e) {
            assertTrue(false);
        }
    }

    @Test
    public void recordTest_001() {
        Schema record = null;
        try {
            record = Schema.deserializeResource("/generator_v2/cc_account.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        try {
            for (int i = 0; i < 10; i++) {
                record.next();
                Map keys = record.getKeyMap();
                Map values = record.getValueMap();
                System.out.println("KeyMap:" + keys.toString());
                System.out.println("ValueMap:" + values.toString());
            }
        } catch (TerminateException te) {

        }
    }

}