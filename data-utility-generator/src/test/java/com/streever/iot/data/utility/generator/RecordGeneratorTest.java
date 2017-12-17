/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RecordGeneratorTest {

    private ClassLoader cl;

    @Before
    public void before() {
        cl = getClass().getClassLoader();
    }

    @Test
    public void Test001() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File(cl.getResource("sample-record-generator.json").getFile());
            JsonNode rootNode = mapper.readValue(file, JsonNode.class);

            RecordGenerator recGen = new RecordGenerator(rootNode);

            groupGen(recGen);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void Test002() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File(cl.getResource("sample-record-ordered-generator.json").getFile());
            JsonNode rootNode = mapper.readValue(file, JsonNode.class);

            RecordGenerator recGen = new RecordGenerator(rootNode);

            groupGen(recGen);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test003() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File(cl.getResource("sample-null-generator.json").getFile());
            JsonNode rootNode = mapper.readValue(file, JsonNode.class);

            RecordGenerator recGen = new RecordGenerator(rootNode);

            groupGen(recGen);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test004() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File(cl.getResource("sample-startstop-generator.json").getFile());
            JsonNode rootNode = mapper.readValue(file, JsonNode.class);

            RecordGenerator recGen = new RecordGenerator(rootNode);

            groupGen(recGen);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test050() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File(cl.getResource("sample-record-generator.json").getFile());
            JsonNode rootNode = mapper.readValue(file, JsonNode.class);

            RecordGenerator recGen = new RecordGenerator(rootNode);
            long start = new Date().getTime();
            System.out.println("Starting.. " );
            for (int i=0;i<1000000;i++) {
                String value = recGen.next();
            }
            long end = new Date().getTime();
            System.out.println("Finished generating 1,000,000 in (ms): " + (end-start));


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test060() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File(cl.getResource("validation-generator.json").getFile());
            JsonNode rootNode = mapper.readValue(file, JsonNode.class);
            
            RecordGenerator recGen = new RecordGenerator(rootNode);
            long start = new Date().getTime();
            System.out.println("Starting.. " );
            for (int i=0;i<10;i++) {
                String value = recGen.next();
                System.out.println(value);
            }
            long end = new Date().getTime();
            System.out.println("Finished generating 10 in (ms): " + (end-start));


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test070() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            InputStream stream = getClass().getResourceAsStream("/validation-generator.json");
            JsonNode rootNode = mapper.readValue(stream, JsonNode.class);

            RecordGenerator recGen = new RecordGenerator(rootNode);
//            RecordGenerator recGen = new RecordGenerator(null);
            long start = new Date().getTime();
            System.out.println("Starting.. (from resource) " );
            for (int i=0;i<10;i++) {
                String value = recGen.next();
                System.out.println(value);
            }
            long end = new Date().getTime();
            System.out.println("Finished generating 10 in (ms): " + (end-start));


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test080() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File(cl.getResource("sample-entity-transaction-generator.json").getFile());
            JsonNode rootNode = mapper.readValue(file, JsonNode.class);

            RecordGenerator recGen = new RecordGenerator(rootNode);
            long start = new Date().getTime();
            System.out.println("Starting.. " );
            int j = 0;
            for (int i=0;i<100;i++) {
                String value = recGen.next();
                j++;
                System.out.println(value);
            }
            long end = new Date().getTime();
            System.out.println("Finished generating " + j + " records in (ms): " + (end-start));


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test200() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File(cl.getResource("sample-record-ordered-generator.json").getFile());
            JsonNode rootNode = mapper.readValue(file, JsonNode.class);

            RecordGenerator recGen = new RecordGenerator(rootNode);

            buildFile(recGen,10000000);
            Thread.sleep(1000);
            buildFile(recGen,50000);
            Thread.sleep(1000);
            buildFile(recGen,20000);
            Thread.sleep(1000);
            buildFile(recGen,100000);
            Thread.sleep(1000);
            buildFile(recGen,700);
            Thread.sleep(1000);
            buildFile(recGen,3000000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void buildFile(RecordGenerator recGen,long count) {
        String fileName = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss").format(new Date()) + ".txt";
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            for (long i = 0; i < count; i++) {
                writer.append(recGen.next());
                writer.append("\n");
            }
            writer.close();
        } catch (Throwable t) {
            System.out.println(t.getMessage());
        }
    }

    private void groupGen(RecordGenerator recGen) {
        System.out.println(recGen.next());
        System.out.println(recGen.next());
        System.out.println(recGen.next());
        System.out.println(recGen.next());
        System.out.println(recGen.next());
        System.out.println(recGen.next());
        System.out.println(recGen.next());
    }
}
