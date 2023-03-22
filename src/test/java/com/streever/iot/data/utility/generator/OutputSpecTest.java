package com.streever.iot.data.utility.generator;

import com.streever.iot.data.utility.generator.output.OutputBase;
import org.junit.Before;
import org.junit.Test;

public class OutputSpecTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void spec_csv_hcfs_001() {
        OutputConfig o1 = OutputConfig.deserialize("/standard/csv_hcfs.yaml");
        System.out.println("Made it");
    }

    @Test
    public void spec_json_hcfs_001() {
        OutputConfig o1 = OutputConfig.deserialize("/standard/json_hcfs.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadDefaultSpec_001() {
        OutputConfig o1 = OutputConfig.deserialize("/outputspec/default.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadKafkaSpec_001() {
        OutputConfig o1 = OutputConfig.deserialize("/outputspec/kafka-0.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadKafkaSpec_002() {
        OutputConfig o1 = OutputConfig.deserialize("/outputspec/kafka-1.yaml");
        System.out.println("Made it");
    }

    @Test
    public void loadKafkaSpec_003() {
        OutputConfig o1 = OutputConfig.deserialize("/outputspec/kafka-ccn_0.yaml");
        System.out.println("Made it");
    }

//    @Test
//    public void loadKafkaSpec_004() {
//        OutputConfig o1 = OutputConfig.deserialize("/outputspec/kafka-trans.yaml");
//        System.out.println("Made it");
//    }

//    @Test
//    public void loadKafkaSpec_005() {
//        OutputConfig o1 = OutputConfig.deserialize("/outputspec/kafka-trans.yaml");
//        // Clone Check
//        try {
//            OutputBase clone1 = (OutputBase)o1.getConfig().clone();
//            System.out.println("Clone created");
//        } catch (CloneNotSupportedException e) {
//            e.printStackTrace();
//        }
//        System.out.println("Made it");
//    }

//    @Test
//    public void loadKafkaSpec_006() {
//        OutputConfig o1 = OutputConfig.deserialize("/outputspec/kafka-trans.yaml");
        // Clone Check
//        try {
//            OutputBase clone1 = (OutputBase)o1.getRelationships().get("acct").clone();
//            System.out.println("Clone created");
//        } catch (CloneNotSupportedException e) {
//            e.printStackTrace();
//        }
//        System.out.println("Made it");
//    }


}
