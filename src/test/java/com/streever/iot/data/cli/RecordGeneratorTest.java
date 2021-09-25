package com.streever.iot.data.cli;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class RecordGeneratorTest {
    String BASE_DIR = null;

    @Before
    public void setUp() throws Exception {
        BASE_DIR = System.getProperty("user.home") + System.getProperty("file.separator") + "DATAGEN_JUNIT";
        File bd = new File(BASE_DIR);
        if (!bd.exists()) {
            bd.mkdirs();
        }
    }

    protected void doIt(String[] args) {
        RecordGenerator cli = new RecordGenerator();
        try {
            cli.run(args);
            assertTrue(true);
        } catch (IOException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void test_001() {
        String[] options = {"-csv", "-std", "-s", "/sample_schemas/one-many.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_002() {
        String[] options = {"-json", "-std", "-s", "/sample_schemas/one-many.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_002_1() {
        String[] options = {"-json", "-std", "-s", "/sample_schemas/one-many.yaml", "-mb", "1"};
        doIt(options);
    }

    @Test
    public void test_003() {
        String[] options = {"-csv", "-local", "-s", "/sample_schemas/one-many.yaml", "-c", "5", "-p", BASE_DIR};
        doIt(options);
    }
    @Test
    public void test_004() {
        String[] options = {"-json", "-local", "-s", "/sample_schemas/one-many.yaml", "-c", "5", "-p", BASE_DIR};
        doIt(options);
    }
    @Test
    public void test_005() {
        String[] options = {"-csv", "-local", "-s", "/sample_schemas/one-many.yaml", "-c", "5", "-p", BASE_DIR, "-ts"};
        doIt(options);
    }
    @Test
    public void test_006() {
        String[] options = {"-csv", "-local", "-s", "/sample_schemas/one-many.yaml", "-c", "5", "-p", BASE_DIR, "-uuid"};
        doIt(options);
    }
//    @Test
//    public void test_006_01() {
//        String[] options = {"-csv", "-hcfs", "-s", "/sample_schemas/one-many.yaml", "-c", "5", "-p", "s3a://dstreev-cdp/datagen_001", "-uuid"};
//        doIt(options);
//    }

    @Test
    public void test_007() {
        String[] options = {"-json", "-local", "-s", "/sample_schemas/one-many.yaml", "-c", "5", "-p", BASE_DIR, "-ts"};
        doIt(options);
    }

    @Test
    public void test_007_01() {
        String[] options = {"-csv", "-local", "-s", "/validation/multi-default.yaml", "-c", "5", "-p", BASE_DIR, "-uuid"};
        doIt(options);
    }
    @Test
    public void test_007_02() {
        String[] options = {"-json", "-local", "-s", "/validation/multi-default.yaml", "-c", "50", "-p", BASE_DIR, "-uuid"};
        doIt(options);
    }

    @Test
    public void test_008() {
        String[] options = {"-json", "-local", "-s", "/sample_schemas/one-many.yaml", "-c", "5", "-p", BASE_DIR, "-uuid"};
        doIt(options);
    }

//    @Test
//    public void test_009() {
//        String[] options = {"-csv", "-std", "-s", "/sample_schemas/one-many.yaml", "-c", "5"};
//        doIt(options);
//    }
//    @Test
//    public void test_010() {
//        String[] options = {"-csv", "-std", "-s", "/sample_schemas/one-many.yaml", "-c", "5"};
//        doIt(options);
//    }
//    @Test
//    public void test_011() {
//        String[] options = {"-csv", "-std", "-s", "/sample_schemas/one-many.yaml", "-c", "5"};
//        doIt(options);
//    }
    
}