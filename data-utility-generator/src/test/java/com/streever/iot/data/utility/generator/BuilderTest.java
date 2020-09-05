package com.streever.iot.data.utility.generator;

import org.junit.Test;

import static org.junit.Assert.*;

public class BuilderTest {

    @Test
    public void init_01() {
        Builder builder = new Builder();
        Record record = Record.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        OutputSpec outputSpec = OutputSpec.deserialize("/outputspec/default.yaml");
        builder.setRecord(record);
        builder.setOutputSpec(outputSpec);
        builder.init();
        System.out.println("Hello");
    }

    @Test
    public void init_02() {
        Builder builder = new Builder();
        Record record = Record.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        OutputSpec outputSpec = OutputSpec.deserialize("/outputspec/cc_account_with_relationships.yaml");
        builder.setRecord(record);
        builder.setOutputSpec(outputSpec);
        builder.init();
        builder.run();
        System.out.println("Hello");
    }

    @Test
    public void init_no_outspec_03() {
        Builder builder = new Builder();
        Record record = Record.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        builder.setRecord(record);
        builder.init();
        builder.run();
        System.out.println("Hello");
    }

    @Test
    public void init_csv_03() {
        Builder builder = new Builder();
        Record record = Record.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix("target/csv_03");
        builder.init();
        builder.run();
    }

}