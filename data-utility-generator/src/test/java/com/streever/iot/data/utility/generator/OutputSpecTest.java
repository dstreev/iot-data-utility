package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.output.OutputBase;
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

    @Test
    public void loadRelationSpec_002() {
        OutputSpec o1 = OutputSpec.deserialize("/outputspec/relationship001.yaml");
        // Clone Check
        try {
            OutputBase clone1 = (OutputBase)o1.getDefault().clone();
            System.out.println("Clone created");
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        System.out.println("Made it");
    }

    @Test
    public void loadKafkaSpec_001() {
        OutputSpec o1 = OutputSpec.deserialize("/outputspec/kafka-0.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadKafkaSpec_002() {
        OutputSpec o1 = OutputSpec.deserialize("/outputspec/kafka-1.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadKafkaSpec_003() {
        OutputSpec o1 = OutputSpec.deserialize("/outputspec/kafka-ccn_0.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadKafkaSpec_004() {
        OutputSpec o1 = OutputSpec.deserialize("/outputspec/kafka-trans.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadKafkaSpec_005() {
        OutputSpec o1 = OutputSpec.deserialize("/outputspec/kafka-trans.yaml");
        // Clone Check
        try {
            OutputBase clone1 = (OutputBase)o1.getDefault().clone();
            System.out.println("Clone created");
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        System.out.println("Made it");
    }

    @Test
    public void loadKafkaSpec_006() {
        OutputSpec o1 = OutputSpec.deserialize("/outputspec/kafka-trans.yaml");
        // Clone Check
        try {
            OutputBase clone1 = (OutputBase)o1.getRelationships().get("acct").clone();
            System.out.println("Clone created");
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        System.out.println("Made it");
    }


}
