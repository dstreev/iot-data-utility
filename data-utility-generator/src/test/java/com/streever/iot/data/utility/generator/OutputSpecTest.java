package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.*;

public class OutputSpecTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void loadDefaultSpec_001() {
        OutputSpec o1 = OutputSpec.deserialize("/outputspec/default.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadRelationSpec_001() {
        OutputSpec o1 = OutputSpec.deserialize("/outputspec/relationship001.yaml");
        System.out.println("Made it");
    }

}
