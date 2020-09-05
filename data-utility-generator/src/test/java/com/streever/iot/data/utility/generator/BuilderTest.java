package com.streever.iot.data.utility.generator;

import org.junit.Test;

import static org.junit.Assert.*;

public class BuilderTest {

    @Test
    public void init_01() {
        Builder builder = new Builder();
        Record record = RecordTest.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        OutputSpec outputSpec = OutputSpecTest.deserialize("/outputspec/default.yaml");
        builder.setRecord(record);
        builder.setOutputSpec(outputSpec);
        builder.init();
        System.out.println("Hello");
    }

    @Test
    public void init_02() {
        Builder builder = new Builder();
        Record record = RecordTest.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        OutputSpec outputSpec = OutputSpecTest.deserialize("/outputspec/cc_account_with_relationships.yaml");
        builder.setRecord(record);
        builder.setOutputSpec(outputSpec);
        builder.init();
        builder.run();
        System.out.println("Hello");
    }

}