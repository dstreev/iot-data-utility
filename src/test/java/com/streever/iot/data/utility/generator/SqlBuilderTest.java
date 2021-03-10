package com.streever.iot.data.utility.generator;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class SqlBuilderTest {

    @Before
    public void setUp() throws Exception {
    }

    private String[] cpResources = {"/generator/array.yaml", "/generator/cc_trans.yaml", "/generator/cc_account.yaml",
            "/generator/date-as.yaml", "/generator/ip-as.yaml", "/generator/date-as-repeat.yaml", "/generator/date-increment.yaml",
            "/generator/date-late-arriving_day.yaml", "/generator/date-late-arriving_hour.yaml",
            "/generator/date-late-arriving_minute.yaml", "/generator/date-late-arriving_month.yaml",
            "/generator/date-late-arriving_year.yaml", "/generator/date-terminate.yaml",
            "/generator/ip-as.yaml", "/generator/record-definition.yaml",
            "/generator/ref-string.yaml", "/generator/date-start_stop.yaml", "/generator/wide-table.yaml"};

    @Test
    public void test_001() {
        for (String resource: cpResources) {
            System.out.println("Running Resource: " + resource);
            doIt(resource);
        }
    }

    protected void doIt(String resource) {
        SqlBuilder sb = new SqlBuilder();
        Schema r1 = null;
        try {
            r1 = Schema.deserialize(resource);
            sb.setSchema(r1);
            sb.link("cc_acct");
            System.out.println(sb.build());
        } catch (IOException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}