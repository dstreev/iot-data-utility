package com.streever.iot.data.utility.generator;

import org.junit.Test;

import java.util.Date;

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

    @Test
    public void init_csv_04() {
        Builder builder = new Builder();
        Record record = Record.deserialize("/generator_v2/cc_account_v2.yaml");
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix("target/csv_04");
        builder.init();
        builder.run();
    }

    @Test
    public void init_json_05() {
        Builder builder = new Builder();
        Record record = Record.deserialize("/generator_v2/cc_account_v2.yaml");
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/json_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix("target/json_05");
        builder.init();
        builder.run();
    }

    @Test
    public void init_json_06() {
        Builder builder = new Builder();
        Record record = Record.deserialize("/generator_v2/cc_account_v2.yaml");
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/json_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix("target/json_06");
        int count = 100000;
        builder.setCount(count);
        builder.init();
        Date start = new Date();
        builder.run();
        Date end = new Date();
        long diff = end.getTime() - start.getTime();
        double perSecRate = ((double) count / diff) * 1000;

        System.out.println("Time: " + diff + " Loops: " + count);
        System.out.println("Rate (perSec): " + perSecRate);

    }

    @Test
    public void init_csv_06() {
        Builder builder = new Builder();
        Record record = Record.deserialize("/generator_v2/cc_account_v2.yaml");
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix("target/csv_06");
        int count = 100000;
        builder.setCount(count);
        builder.init();
        Date start = new Date();
        builder.run();
        Date end = new Date();
        long diff = end.getTime() - start.getTime();
        double perSecRate = ((double) count / diff) * 1000;

        System.out.println("Time: " + diff + " Loops: " + count);
        System.out.println("Rate (perSec): " + perSecRate);

    }

    // Old Tests
    @Test
    public void init_csv_07() {
        Builder builder = new Builder();
        Record record = Record.deserialize("/generator_v2/array.yaml");
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix("target/csv_07");
        int count = 1000;
        builder.setCount(count);
        builder.init();
        Date start = new Date();
        builder.run();
        Date end = new Date();
        long diff = end.getTime() - start.getTime();
        double perSecRate = ((double) count / diff) * 1000;

        System.out.println("Time: " + diff + " Loops: " + count);
        System.out.println("Rate (perSec): " + perSecRate);

    }

    // Basic Tests
    @Test
    public void init_default_all_01() {
        Builder builder = new Builder();
        String[] testResources = {"/generator/array.yaml", "/generator/cc_trans.yaml", "/generator/cc_account.yaml",
                "/generator/date-as.yaml", "/generator/date-as-repeat.yaml", "/generator/date-increment.yaml",
                "/generator/date-late-arriving_day.yaml", "/generator/date-late-arriving_hour.yaml",
                "/generator/date-late-arriving_minute.yaml", "/generator/date-late-arriving_month.yaml",
                "/generator/date-late-arriving_year.yaml"};
        for (String resource: testResources) {
            Record record = Record.deserialize(resource);
            builder.setRecord(record);
            OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
            builder.setOutputSpec(outputSpec);
            builder.setOutputPrefix("." + resource);
            int count = 1000;
            builder.setCount(count);
            builder.init();
            Date start = new Date();
            builder.run();
            Date end = new Date();
            long diff = end.getTime() - start.getTime();
            double perSecRate = ((double) count / diff) * 1000;

            System.out.println("Time: " + diff + " Loops: " + count);
            System.out.println("Rate (perSec): " + perSecRate);
        }
    }

}