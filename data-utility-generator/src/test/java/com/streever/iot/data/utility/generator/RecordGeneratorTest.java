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
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;

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
            File file = new File(cl.getResource("generator/one.json").getFile());
            String jsonFromFile = FileUtils.readFileToString(file, Charset.forName("UTF-8"));

            RecordGenerator recGen = mapper.readerFor(com.streever.iot.data.utility.generator.RecordGenerator.class).readValue(jsonFromFile);

            System.out.println("Test001");
            for (int i = 0; i < 10; i++) {
                String check = recGen.next();
                System.out.println(check);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test0015() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            File file = new File(cl.getResource("generator/one.yaml").getFile());
            String jsonFromFile = FileUtils.readFileToString(file, Charset.forName("UTF-8"));

            RecordGenerator recGen = mapper.readerFor(com.streever.iot.data.utility.generator.RecordGenerator.class).readValue(jsonFromFile);

            System.out.println("Test001");
            for (int i = 0; i < 10; i++) {
                String check = recGen.next();
                System.out.println(check);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test002() {
        ObjectMapper mapper = new ObjectMapper();
        try {
//            RecordGenerator recGen = new RecordGenerator();
//            recGen.setTitle("Hello");
//            TreeSet<FieldBase> fields = new TreeSet<FieldBase>();
//            FieldBase iField = new IntegerField();
//            iField.setOrder(1);
//            fields.add(iField);
//            FieldBase sField = new StringField();
//            sField.setOrder(1);
//            fields.add(sField);
//            recGen.setFields(fields);
//
//
//            recGen.setFields(fields);
//
//            String result = new ObjectMapper().writeValueAsString(recGen);
//
//            System.out.println(result);

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
