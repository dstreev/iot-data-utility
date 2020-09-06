package com.streever.iot.data.utility.generator;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BuilderTest {
    private String BASE_OUTPUT_DIR = "data-utility-generator/target/testcases/";

    @Test
    public void init_01() {
        Builder builder = new Builder();
        Record record = null;
        try {
            record = Record.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        OutputSpec outputSpec = OutputSpec.deserialize("/outputspec/default.yaml");
        builder.setRecord(record);
        builder.setOutputSpec(outputSpec);
        builder.init();
        System.out.println("Hello");
    }

    @Test
    public void init_02() {
        Builder builder = new Builder();
        Record record = null;
        try {
            record = Record.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        OutputSpec outputSpec = OutputSpec.deserialize("/outputspec/cc_account_with_relationships.yaml");
        builder.setRecord(record);
        builder.setOutputSpec(outputSpec);
        builder.init();
        builder.run();
    }

    @Test
    public void init_no_outspec_03() {
        Builder builder = new Builder();
        Record record = null;
        try {
            record = Record.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        builder.setRecord(record);
        builder.init();
        builder.run();
    }

    @Test
    public void init_csv_03() {
        Builder builder = new Builder();
        Record record = null;
        try {
            record = Record.deserialize("/generator_v2/cc_account_with_relationships.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix(BASE_OUTPUT_DIR + "/csv_03");
        builder.init();
        builder.run();
    }

    @Test
    public void init_csv_04() {
        Builder builder = new Builder();
        Record record = null;
        try {
            record = Record.deserialize("/generator_v2/cc_account_v2.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix(BASE_OUTPUT_DIR + "/csv_04");
        builder.init();
        builder.run();
    }

    @Test
    public void init_json_05() {
        Builder builder = new Builder();
        Record record = null;
        try {
            record = Record.deserialize("/generator_v2/cc_account_v2.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/json_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix(BASE_OUTPUT_DIR + "/json_05");
        builder.init();
        builder.run();
    }

    @Test
    public void init_json_06() {
        Builder builder = new Builder();
        Record record = null;
        try {
            record = Record.deserialize("/generator_v2/cc_account_v2.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/json_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix(BASE_OUTPUT_DIR + "/json_06");
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
        Record record = null;
        try {
            record = Record.deserialize("/generator_v2/cc_account_v2.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix(BASE_OUTPUT_DIR + "/csv_06");
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
        Record record = null;
        try {
            record = Record.deserialize("/generator_v2/array.yaml");
        } catch (IOException e) {
            assertTrue(false);
        }
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
        builder.setOutputSpec(outputSpec);
        builder.setOutputPrefix(BASE_OUTPUT_DIR + "/csv_07");
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

    private String[] cpResources = {"/generator/array.yaml", "/generator/cc_trans.yaml", "/generator/cc_account.yaml",
            "/generator/date-as.yaml", "/generator/ip-as.yaml", "/generator/date-as-repeat.yaml", "/generator/date-increment.yaml",
            "/generator/date-late-arriving_day.yaml", "/generator/date-late-arriving_hour.yaml",
            "/generator/date-late-arriving_minute.yaml", "/generator/date-late-arriving_month.yaml",
            "/generator/date-late-arriving_year.yaml", "/generator/one.yaml"};

    // Basic Tests
    @Test
    public void init_csv_cpr_all_01() {
        Builder builder = new Builder();
        for (String resource : cpResources) {
            Record record = null;
            try {
                record = Record.deserialize(resource);
            } catch (IOException e) {
                System.err.println("Processing: " + resource);
                e.printStackTrace();
                assertTrue(false);
            }
            builder.setRecord(record);
            OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
            builder.setOutputSpec(outputSpec);
            String filename = FilenameUtils.getName(resource);
            builder.setOutputPrefix(BASE_OUTPUT_DIR + filename);
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

    @Test
    public void init_json_cpr_all_01() {
        Builder builder = new Builder();
        for (String resource : cpResources) {
            Record record = null;
            try {
                record = Record.deserialize(resource);
            } catch (IOException e) {
                e.printStackTrace();
                assertTrue(false);
            }
            builder.setRecord(record);
            OutputSpec outputSpec = OutputSpec.deserialize("/json_out.yaml");
            builder.setOutputSpec(outputSpec);
            String filename = FilenameUtils.getName(resource);
            builder.setOutputPrefix(BASE_OUTPUT_DIR + filename);
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

    private String[] fileResources = {"data-utility-generator/src/schemas/file-date-as.yaml"};

    @Test
    public void init_csv_fr_all_02() {
        Builder builder = new Builder();
        for (String resource : fileResources) {
            Record record = null;
            try {
                record = Record.deserialize(resource);
            } catch (IOException e) {
                System.err.println("Processing: " + resource);
                e.printStackTrace();
                assertTrue(false);
            }
            builder.setRecord(record);
            OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
            builder.setOutputSpec(outputSpec);
            // Strip off path.
            String filename = FilenameUtils.getName(resource);
            builder.setOutputPrefix(BASE_OUTPUT_DIR + filename);
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


    // Exceptions Test
    private String[] cpExceptionResources = {"/bad_schemas/repeat-too-high.yaml", "/bad_schemas/ip-as.yaml",
            "/bad_schemas/ip-as_2.yaml"};

    @Test
    public void init_exception_all_01() {
        Builder builder = new Builder();
        for (String resource : cpExceptionResources) {
            try {
                Record record = Record.deserialize(resource);
                assertFalse(true);
            } catch (IOException rte) {
                rte.printStackTrace();
                assertTrue(true);
            }
        }
    }

}