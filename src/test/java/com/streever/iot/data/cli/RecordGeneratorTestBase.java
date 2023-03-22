package com.streever.iot.data.cli;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class RecordGeneratorTestBase {
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
        DomainGenerator cli = new DomainGenerator();
        try {
            cli.run(args);
            assertTrue(true);
        } catch (IOException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}