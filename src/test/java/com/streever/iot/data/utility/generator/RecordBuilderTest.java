package com.streever.iot.data.utility.generator;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RecordBuilderTest {
    String BASE_OUTPUT_DIR = null;

    @Before
    public void setUp() throws Exception {
        BASE_OUTPUT_DIR = System.getProperty("user.home") + System.getProperty("file.separator") + "DATAGEN_JUNIT";
        File bd = new File(BASE_OUTPUT_DIR);
        if (!bd.exists()) {
            bd.mkdirs();
        }
    }

    @Test
    public void init_csv_07() {
        runResourceToCSV("/generator_v3/array.yaml", 100000);
    }

    @Test
    public void init_relationship_one_to_many() {
        runResourceToCSV("/generator_v3/cc_account_v2.yaml", 100000, 10000000);
    }

    @Test
    public void init_relationship_one_to_many_unique() {
        runResource("/generator_v2/one-many.yaml", 5, "/standard/csv_local_ts.yaml");
    }

//    @Test
    // TODO: Geo Test needed once GeoLocation Field completed.
//    public void init_geo_test_01() {
//        runResource("/generator/geo.yaml", 5, "/csv_out.yaml");
//    }

    @Test
    public void init_relationship_one_to_many_unique_alt_ts() {
        runResource("/generator_v2/one-many.yaml", 5, "/outputspec/csv_unique_alt_tsf_out.yaml");
    }

    @Test
    public void init_relationship_one_to_many_unique_uuid() {
        runResource("/generator_v2/one-many.yaml", 5, "/outputspec/csv_unique_uuid_out.yaml");
    }

    @Test
    public void to_hcfs_001() {
        runResourceToHCFS("/generator_v2/one-many.yaml", 5);
    }

    private String[] cpResources = {"/generator/array.yaml", "/generator/cc_trans.yaml", "/generator/cc_account.yaml",
            "/generator/date-as.yaml", "/generator/ip-as.yaml", "/generator/date-as-repeat.yaml", "/generator/date-increment.yaml",
            "/generator/date-late-arriving_day.yaml", "/generator/date-late-arriving_hour.yaml",
            "/generator/date-late-arriving_minute.yaml", "/generator/date-late-arriving_month.yaml",
            "/generator/date-late-arriving_year.yaml", "/generator/date-terminate.yaml",
            "/generator/ip-as.yaml", "/generator/record-definition.yaml",
            "/generator/ref-state.yaml", "/generator/date-start_stop.yaml", "/generator/wide-table.yaml"};

    // Basic Tests
//    @Test
//    public void init_csv_cpr_all_01() {
//        DomainBuilder builder = new DomainBuilder();
//        for (String resource : cpResources) {
//            System.out.println("Processing Resource: " + resource);
//            runResourceToCSV(resource, 1000);
//        }
//    }
//
//    @Test
//    public void init_json_cpr_all_01() {
//        DomainBuilder builder = new DomainBuilder();
//        for (String resource : cpResources) {
//            runResourceToJson(resource, 1000);
//        }
//    }

    private String[] fileResources = {"src/schemas/file-date-as.yaml"};

//    @Test
    // TODO: Test fails with Maven.  It's a current path issue.
//    public void init_csv_fr_all_02() {
//        DomainBuilder builder = new DomainBuilder();
//        for (String resource : fileResources) {
//            runResourceToCSV(resource, 1000);
//        }
//    }
//
    // Exceptions Test
    private String[] cpExceptionResources = {"/bad_schemas/repeat-too-high.yaml", "/bad_schemas/ip-as.yaml",
            "/bad_schemas/ip-as_2.yaml", "/bad_schemas/one-many.yaml"};

//    @Test
//    public void init_exception_all_01() {
//        DomainBuilder builder = new DomainBuilder();
//        for (String resource : cpExceptionResources) {
//            try {
//                Schema record = Schema.deserializeResource(resource);
//                assertFalse(true);
//            } catch (IOException rte) {
//                rte.printStackTrace();
//                assertTrue(true);
//            }
//        }
//    }

    @Test
    public void dateTerminate_01() {
        long createNumRecords = 1000;
        long[] recordsCreated = runResourceToCSV("/generator/date-terminate.yaml", createNumRecords);
        // Schema setup should terminate record creation BEFORE reaching the requested count.
        assertTrue("Termination Test Failed", recordsCreated[0] < createNumRecords);
    }

    @Test
    public void dateTerminate_02() {
        long createNumRecords = 1000;
        long[] recordsCreated = runResource("/generator/date-terminate.yaml", createNumRecords, null);
        // Schema setup should terminate record creation BEFORE reaching the requested count.
        assertTrue("Termination Test Failed", recordsCreated[0] < createNumRecords);
    }

    @Test
    public void dateStartStop_01() {
        long createNumRecords = 10;
        long[] recordsCreated = runResourceToCSV("/generator/date-start_stop.yaml", createNumRecords);
        long[] recordsCreated2 = runResourceToJson("/generator/date-start_stop.yaml", createNumRecords);
        // Schema setup should terminate record creation BEFORE reaching the requested count.
//        assertTrue("Termination Test Failed", recordsCreated < createNumRecords);
    }

    @Test
//    public void runBuilderWithoutPrefixdir() {
//        long recordsCreated[] = {0, 0};
//        DomainBuilder builder = new DomainBuilder();
//        Schema record = null;
//        try {
//            record = Schema.deserializeResource("/generator/cc_account.yaml");
//        } catch (IOException e) {
//            System.err.println("Processing: " + "/generator/cc_account.yaml");
//            e.printStackTrace();
//            assertTrue(false);
//        }
//        builder.setSchema(record);
//        OutputConfig outputSpec = OutputConfig.deserialize("/standard/csv_std.yaml");
//        builder.setOutputConfig(outputSpec);
//        // Strip off path.
////        String filename = FilenameUtils.getName(resource);
////        builder.setOutputPrefix(BASE_OUTPUT_DIR + filename);
//        builder.setCount(10);
//        builder.init();
//        Date start = new Date();
//        recordsCreated = builder.run();
//        Date end = new Date();
//        long diff = end.getTime() - start.getTime();
//        double perSecRate = ((double) recordsCreated[0] / diff) * 1000;
//
//        System.out.println("Time: " + diff + " Loops: " + recordsCreated[0]);
//        System.out.println("Rate (perSec): " + perSecRate);
//        System.out.println("Records Created: " + recordsCreated[0] + ":" + recordsCreated[1]);
//
//    }

    protected long[] runResourceToHCFS(String resource, long count) {
        return runResource(resource, count, "/standard/json_hcfs_ts.yaml");
    }

    protected long[] runResourceToCSV(String resource, long count, long size) {
        return runResource(resource, count, size, "/standard/csv_std.yaml");
    }

    protected long[] runResourceToCSV(String resource, long count) {
        return runResource(resource, count, "/standard/csv_std.yaml");
    }

    protected long[] runResourceToJson(String resource, long count) {
        return runResource(resource, count, "/standard/json_std.yaml");
    }

    protected long[] runResource(String resource, long count) {
        String outputSpecResource = "/standard/csv_std.yaml";
        return runResource(resource, count, -1l, outputSpecResource);
    }

    protected long[] runResource(String resource, long count, String outputSpecResource) {
        String spec = outputSpecResource;
        if (spec == null) {
            spec = "/standard/csv_std.yaml";
        }
        return runResource(resource, count, -1l, spec);
    }

    protected long[] runResource(String resource, long count, long size, String outputSpecResource) {
        long recordsCreated[] = {0, 0};
//        DomainBuilder builder = new DomainBuilder();
//        Schema record = null;
//        try {
//            record = Schema.deserializeResource(resource);
//        } catch (IOException e) {
//            System.err.println("Processing: " + resource);
//            e.printStackTrace();
//            assertTrue(false);
//        }
//        builder.setSchema(record);
//        if (outputSpecResource != null) {
//            OutputConfig outputSpec = OutputConfig.deserialize(outputSpecResource);
//            builder.setOutputConfig(outputSpec);
//        }
//        // Strip off path.
//        String filename = FilenameUtils.getName(resource);
//        // Check if LOCAL or HCFS is the target FileSystem
//        if (builder.getOutputConfig().getDefault() instanceof LocalFileOutput) {
//            if ((builder.getOutputConfig().getDefault()) instanceof LocalFileOutput) {
//                // Only set root dir for LOCAL Filesytem.
//                builder.setOutputPrefix(BASE_OUTPUT_DIR + System.getProperty("file.separator") + filename);
//            } else {
//                builder.setOutputPrefix("/user/" + System.getProperty("user.name") + System.getProperty("file.separator") +
//                        "DATAGEN_JUNIT" + System.getProperty("file.separator") + filename);
//            }
//        }
//        builder.setCount(count);
//        builder.setSize(size);
//        builder.init();
//        Date start = new Date();
//        recordsCreated = builder.run();
//        Date end = new Date();
//        long diff = end.getTime() - start.getTime();
//        double perSecRate = ((double) recordsCreated[0] / diff) * 1000;
//
//        System.out.println("Time: " + diff + " Loops: " + recordsCreated[0]);
//        System.out.println("Rate (perSec): " + perSecRate);
//        System.out.println("Records Created: " + recordsCreated[0] + ":" + recordsCreated[1]);
        return recordsCreated;
    }

    /*
           ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            File kfile = new File(cl.getResource(kafkaConfig).getFile());
            String jsonFromkFile = FileUtils.readFileToString(kfile, Charset.forName("UTF-8"));

            ProducerSpec producerSpec = mapper.readerFor(ProducerSpec.class).readValue(jsonFromkFile);

            Boolean transactional = null;
            if (producerSpec.getConfigs().get(KafkaProducerConfig.TRANSACTIONAL_ID.getConfig()) != null &&
                    producerSpec.getConfigs().get(KafkaProducerConfig.ACKS.getConfig()) != null &&
                    producerSpec.getConfigs().get(KafkaProducerConfig.ACKS.getConfig()).toString().equals("all")) {
                transactional = true;
            } else {
                transactional = false;
            }

            System.out.println("Transaction: " + transactional);

            Producer<String, String> producer = (Producer<String, String>) ProducerCreator.createProducer(producerSpec);

            File file = new File(cl.getResource(genConfig).getFile());
            String jsonFromFile = FileUtils.readFileToString(file, Charset.forName("UTF-8"));

            RecordGenerator recGen = mapper.readerFor(com.streever.iot.data.utility.generator.RecordGenerator.class).readValue(jsonFromFile);
            Date start = new Date();
            if (transactional) {
                producer.initTransactions();
                producer.beginTransaction();
            }
            Map<Integer, Long> offsets = new TreeMap<Integer, Long>();
            for (int i = 1; i < loops + 1; i++) {
                recGen.next();
                Object key = recGen.getKey();
                Object value = recGen.getValue();
                if (i % 5000l == 0) {
                    if (transactional) {
                        producer.commitTransaction();
                        producer.beginTransaction();
                    }
                    System.out.println("Key: " + key + " Value: " + value);
                }

                final ProducerRecord<String, String> record = new ProducerRecord<String, String>(producerSpec.getTopic().getName(), key.toString(), value.toString());
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    offsets.put(metadata.partition(), metadata.offset());
//                    System.out.println("Record sent with key " + key.toString() + " to partition " + metadata.partition()
//                            + " with offset " + metadata.offset());
                } catch (ExecutionException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                } catch (InterruptedException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                }

            }
            if (transactional) {
                producer.commitTransaction();
            }

     */
}