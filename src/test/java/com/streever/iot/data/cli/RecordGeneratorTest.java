package com.streever.iot.data.cli;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class RecordGeneratorTest extends RecordGeneratorTestBase {

    @Test
    public void test_001() {
        String[] options = {"-csv", "-std", "-d", "/validation/simple-default.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_002() {
        String[] options = {"-json", "-std", "-d", "/validation/simple-default.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_002_1() {
        String[] options = {"-json", "-std", "-d", "/validation/simple-default.yaml", "-mb", "1"};
        doIt(options);
    }

    @Test
    public void test_003() {
        String[] options = {"-csv", "-local", "-d", "/validation/simple-default.yaml", "-c", "5", "-p", BASE_DIR};
        doIt(options);
    }
    @Test
    public void test_004() {
        String[] options = {"-json", "-local", "-d", "/validation/simple-default.yaml", "-c", "5", "-p", BASE_DIR};
        doIt(options);
    }
    @Test
    public void test_005() {
        String[] options = {"-csv", "-local", "-d", "/validation/simple-default.yaml", "-c", "5", "-p", BASE_DIR, "-ts"};
        doIt(options);
    }
    @Test
    public void test_006() {
        String[] options = {"-csv", "-local", "-d", "/validation/simple-default.yaml", "-c", "5", "-p", BASE_DIR, "-uuid"};
        doIt(options);
    }
    @Test
    public void test_006_01() {
        String[] options = {"-csv", "-hcfs", "-d", "/validation/simple-default.yaml", "-c", "5", "-p", "s3a://dstreev-cdp/datagen_001", "-uuid"};
        doIt(options);
    }

    @Test
    public void test_007() {
        String[] options = {"-json", "-local", "-d", "/validation/simple-default.yaml", "-c", "5", "-p", BASE_DIR, "-ts"};
        doIt(options);
    }

    @Test
    public void test_007_01() {
        String[] options = {"-csv", "-local", "-d", "/validation/multi-default.yaml", "-s", "account", "-c", "5", "-p", BASE_DIR, "-uuid"};
        doIt(options);
    }
    @Test
    public void test_007_02() {
        String[] options = {"-json", "-local", "-d", "/validation/multi-default.yaml", "-s", "account", "-c", "50", "-p", BASE_DIR, "-uuid"};
        doIt(options);
    }

    @Test
    public void test_008() {
        String[] options = {"-json", "-local", "-d", "/validation/simple-default.yaml", "-c", "5", "-p", BASE_DIR, "-uuid"};
        doIt(options);
    }

    @Test
    public void test_009() {
        String[] options = {"-csv", "-local", "-d", "/validation/simple-default.yaml", "-c", "5", "-p", BASE_DIR, "-uuid"};
        doIt(options);
    }

    @Test
    public void test_009_01() {
        String[] options = {"-sql", "-d", "/validation/simple-default.yaml"};
        doIt(options);
    }




}