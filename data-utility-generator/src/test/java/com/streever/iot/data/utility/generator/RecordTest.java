package com.streever.iot.data.utility.generator;

import com.streever.iot.data.utility.generator.fields.TerminateException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class RecordTest {

    @Before
    public void before() {
    }

    @Test
    public void loadTest_001() {
        try {
            Schema r1 = Schema.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        System.out.println("Made it");
    }

    @Test
    public void recordTest_001() {
        Schema record = null;
        try {
            record = Schema.deserialize("/generator_v2/cc_account.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
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

}