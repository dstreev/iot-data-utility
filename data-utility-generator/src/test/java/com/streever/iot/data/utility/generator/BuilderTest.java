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
        runResource("/generator_v2/cc_account_with_relationships.yaml", 10, "/outputspec/default.yaml");
    }

    @Test
    public void init_02() {
        runResource("/generator_v2/cc_account_with_relationships.yaml", 10, "/outputspec/cc_account_with_relationships.yaml");
    }

    @Test
    public void init_no_outspec_03() {
        runResource("/generator_v2/cc_account_with_relationships.yaml", 10, null);
    }

    @Test
    public void init_csv_07() {
        runResourceToCSV("/generator_v2/array.yaml", 1000);
    }

    @Test
    public void init_relationship_one_to_many() {
        runResourceToCSV("/generator_v2/one-many.yaml", 5);
    }

    @Test
    public void init_relationship_one_to_many_unique() {
        runResource("/generator_v2/one-many.yaml", 5, "/csv_unique_out.yaml");
    }

    @Test
    public void init_relationship_one_to_many_unique_alt_ts() {
        runResource("/generator_v2/one-many.yaml", 5, "/outputspec/csv_unique_alt_tsf_out.yaml");
    }

    @Test
    public void init_relationship_one_to_many_unique_uuid() {
        runResource("/generator_v2/one-many.yaml", 5, "/outputspec/csv_unique_uuid_out.yaml");
    }

    private String[] cpResources = {"/generator/array.yaml", "/generator/cc_trans.yaml", "/generator/cc_account.yaml",
            "/generator/date-as.yaml", "/generator/ip-as.yaml", "/generator/date-as-repeat.yaml", "/generator/date-increment.yaml",
            "/generator/date-late-arriving_day.yaml", "/generator/date-late-arriving_hour.yaml",
            "/generator/date-late-arriving_minute.yaml", "/generator/date-late-arriving_month.yaml",
            "/generator/date-late-arriving_year.yaml", "/generator/date-terminate.yaml", "/generator/one.yaml",
            "/generator/ip-as.yaml", "/generator/one.json", "/generator/record-definition.yaml",
            "/generator/ref-string.yaml", "/generator/date-start_stop.yaml", "/generator/wide-table.yaml"};

    // Basic Tests
    @Test
    public void init_csv_cpr_all_01() {
        Builder builder = new Builder();
        for (String resource : cpResources) {
            runResourceToCSV(resource, 1000);
        }
    }

    @Test
    public void init_json_cpr_all_01() {
        Builder builder = new Builder();
        for (String resource : cpResources) {
            runResourceToJson(resource, 1000);
        }
    }

    private String[] fileResources = {"data-utility-generator/src/schemas/file-date-as.yaml"};

    @Test
    public void init_csv_fr_all_02() {
        Builder builder = new Builder();
        for (String resource : fileResources) {
            runResourceToCSV(resource, 1000);
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

    @Test
    public void dateTerminate_01() {
        long createNumRecords = 1000;
        long[] recordsCreated = runResourceToCSV("/generator/date-terminate.yaml", createNumRecords);
        // Schema setup should terminate record creation BEFORE reaching the requested count.
        assertTrue("Termination Test Failed", recordsCreated[0] < createNumRecords);
    }

    @Test
    public void dateStartStop_01() {
        long createNumRecords = 1000;
        long[] recordsCreated = runResourceToCSV("/generator/date-start_stop.yaml", createNumRecords);
        // Schema setup should terminate record creation BEFORE reaching the requested count.
//        assertTrue("Termination Test Failed", recordsCreated < createNumRecords);
    }

    protected long[] runResourceToCSV(String resource, long count) {
        return runResource(resource, count, "/csv_out.yaml");
    }

    protected long[] runResourceToJson(String resource, long count) {
        return runResource(resource, count, "/json_out.yaml");
    }

    protected long[] runResource(String resource, long count, String outputSpecResource) {
        long recordsCreated[] = {0,0};
        Builder builder = new Builder();
        Record record = null;
        try {
            record = Record.deserialize(resource);
        } catch (IOException e) {
            System.err.println("Processing: " + resource);
            e.printStackTrace();
            assertTrue(false);
        }
        builder.setRecord(record);
        OutputSpec outputSpec = OutputSpec.deserialize(outputSpecResource);
        builder.setOutputSpec(outputSpec);
        // Strip off path.
        String filename = FilenameUtils.getName(resource);
        builder.setOutputPrefix(BASE_OUTPUT_DIR + filename);
        builder.setCount(count);
        builder.init();
        Date start = new Date();
        recordsCreated = builder.run();
        Date end = new Date();
        long diff = end.getTime() - start.getTime();
        double perSecRate = ((double) recordsCreated[0] / diff) * 1000;

        System.out.println("Time: " + diff + " Loops: " + recordsCreated[0]);
        System.out.println("Rate (perSec): " + perSecRate);
        System.out.println("Records Created: " + recordsCreated[0] + ":" + recordsCreated[1] );
        return recordsCreated;
    }

}