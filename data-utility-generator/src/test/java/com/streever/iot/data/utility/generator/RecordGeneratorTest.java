/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.kafka.producer.KafkaProducerConfig;
import com.streever.iot.kafka.producer.ProducerCreator;
import com.streever.iot.kafka.spec.ProducerSpec;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

public class RecordGeneratorTest {

    private ClassLoader cl;

    @Before
    public void before() {
        cl = getClass().getClassLoader();
    }

    @Test
    public void Test001() {
        System.out.println("Test001");
        build("generator/one.json", 10l);

        System.out.println("Test001-Terminate");
    }

    @Test
    public void Test0014() {
        System.out.println("Test0014");
        build("generator/one.yaml", 500000l);

        System.out.println("Test0014-Terminate");
    }

    @Test
    public void Test0015() {
        System.out.println("Test0015");
        build("generator/one.yaml", 10l);

        System.out.println("Test0015-Terminate");
    }

    @Test
    public void Test0016() {
        System.out.println("Test0016");
        build("generator/array.yaml", 10l);

        System.out.println("Test0016-Terminate");


    }

    @Test
    public void Test0017() {
        System.out.println("Test0017");
        build("generator/date-increment.yaml", 10l);

        System.out.println("Test0017-Terminate");

    }

    @Test
    public void Test0018() {
        System.out.println("Test0018");
        build("generator/date-increment.yaml", null);

        System.out.println("Test0018-Terminate");

    }

    @Test
    public void Test0019() {
        System.out.println("Test0019");
        build("generator/date-terminate.yaml", null);

        System.out.println("Test0018-Terminate");

    }

    @Test
    public void Test001901() {
        System.out.println("Test001901");
        build("generator/date-as.yaml", 10l);

        System.out.println("Test001901-Terminate");

    }

    @Test
    public void Test00191() {
        System.out.println("Test00191");
        runPerfConfig("generator/one.yaml", 1000000l);
    }

    @Test
    public void Test00192() {
        System.out.println("Test00192");
        runPerfConfig("generator/two.yaml", 1000000l);
    }

    @Test
    public void Test0020_0() {
        System.out.println("Test0020_0");
        runKafkaLoad("outputspec/kafka-0.yaml", "generator/one.yaml", 200000l);
    }

    @Test
    public void Test0020_01() {
        System.out.println("Test0020_01");
        runKafkaLoad("outputspec/kafka-ccn_0.yaml", "generator/ccn_trans.yaml", 200000l);
    }

    @Test
    public void Test0020_1() {
        System.out.println("Test0020_1");
        runKafkaLoad("outputspec/kafka-1.yaml", "generator/one.yaml", 200000l);
    }

    @Test
    public void Test0020_2() {
        System.out.println("Test0020_2");
        runKafkaLoad("outputspec/kafka-trans.yaml", "generator/one.yaml", 20000l);
    }

    @Test
    public void Test0021_0() {
        System.out.println("Test0021_0");
        runKafkaLoad("outputspec/kafka-0.yaml", "generator/two.yaml", 200000l);
    }

    @Test
    public void Test0021_1() {
        System.out.println("Test0021_1");
        runKafkaLoad("outputspec/kafka-1.yaml", "generator/two.yaml", 20000l);
    }

    @Test
    public void Test0021_2() {
        System.out.println("Test0021_2");
        runKafkaLoad("outputspec/kafka-trans.yaml", "generator/two.yaml", 200000l);
    }

    protected void build(String configResource, Long count) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Date start = new Date();
        int i = 0;
        // Prevent runaway output.
        boolean print = count != null && count < 101;
        try {
            File file = new File(cl.getResource(configResource).getFile());
            String jsonFromFile = FileUtils.readFileToString(file, Charset.forName("UTF-8"));

            RecordGenerator recGen = mapper.readerFor(com.streever.iot.data.utility.generator.RecordGenerator.class).readValue(jsonFromFile);
            boolean go = true;
            while (go) {
                recGen.next();
                i++;
                if (count != null && i > count)
                    go = false;
                if (print)
                    System.out.println(recGen.getValue());
            }
            i--;
        } catch (TerminateException te) {
            System.out.println(te.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Date end = new Date();
            long diff = end.getTime() - start.getTime();
            double perSecRate = ((double) i / diff) * 1000;

            System.out.println("Time: " + diff + " Loops: " + i);
            System.out.println("Rate (perSec): " + perSecRate);

        }

    }

    protected void runPerfConfig(String genConfig, Long loops) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            File file = new File(cl.getResource(genConfig).getFile());
            String jsonFromFile = FileUtils.readFileToString(file, Charset.forName("UTF-8"));

            RecordGenerator recGen = mapper.readerFor(com.streever.iot.data.utility.generator.RecordGenerator.class).readValue(jsonFromFile);

            Date start = new Date();

            for (int i = 1; i < loops + 1; i++) {
                recGen.next();
                if (i % 200000l == 0)
                    System.out.println(".");
                else if (i % 2000l == 0) {
                    System.out.print(".");
                }
            }

            Date end = new Date();
            long diff = end.getTime() - start.getTime();
            double perSecRate = ((double) loops / diff) * 1000;

            System.out.println("Time: " + diff + " Loops: " + loops);
            System.out.println("Rate (perSec): " + perSecRate);

        } catch (TerminateException te) {
            System.out.println(te.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    protected void runKafkaLoad(String kafkaConfig, String genConfig, Long loops) {
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
            Date end = new Date();
            long diff = end.getTime() - start.getTime();
            double perSecRate = ((double) loops / diff) * 1000;
            for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
                System.out.println("Partition: " + entry.getKey() + " Offset: " + entry.getValue());
            }
            System.out.println("Time: " + diff + " Loops: " + loops);
            System.out.println("Rate (perSec): " + perSecRate);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    //    private void buildFile(RecordGenerator recGen,long count) {
//        String fileName = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss").format(new Date()) + ".txt";
//        try {
//            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
//            for (long i = 0; i < count; i++) {
//                writer.append(recGen.next());
//                writer.append("\n");
//            }
//            writer.close();
//        } catch (Throwable t) {
//            System.out.println(t.getMessage());
//        }
//    }
//
//    private void groupGen(RecordGenerator recGen) {
//        System.out.println(recGen.next());
//        System.out.println(recGen.next());
//        System.out.println(recGen.next());
//        System.out.println(recGen.next());
//        System.out.println(recGen.next());
//        System.out.println(recGen.next());
//        System.out.println(recGen.next());
//    }
}
