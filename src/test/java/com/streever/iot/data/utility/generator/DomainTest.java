package com.streever.iot.data.utility.generator;

import com.mifmif.common.regex.Generex;
import com.streever.iot.data.cli.RecordGeneratorTestBase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DomainTest extends RecordGeneratorTestBase {

    @Before
    public void before() {
    }

    @Test
    public void genTest_001() {
        Generex generex = new Generex("[a-z]{10}\\@[A-Z]{15}\\.com");
        System.out.println(generex.random());
    }

    @Test
    public void test_array_001() {
        String[] options = {"-json", "-std", "-d", "/generator/array.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_account_001() {
        String[] options = {"-json", "-std", "-d", "/generator/cc_account.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_transaction_001() {
        String[] options = {"-json", "-std", "-d", "/generator/cc_trans.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_as_001() {
        String[] options = {"-json", "-std", "-d", "/generator/date-as.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_as_repeat_001() {
        String[] options = {"-json", "-std", "-d", "/generator/date-as-repeat.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_increment_001() {
        String[] options = {"-json", "-std", "-d", "/generator/date-increment.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_late_arriving_001() {
        String[] options = {"-json", "-std", "-d", "/generator/date-late-arriving_day.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_late_arriving_002() {
        String[] options = {"-json", "-std", "-d", "/generator/date-late-arriving_hour.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_late_arriving_003() {
        String[] options = {"-json", "-std", "-d", "/generator/date-late-arriving_minute.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_late_arriving_004() {
        String[] options = {"-json", "-std", "-d", "/generator/date-late-arriving_month.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_late_arriving_005() {
        String[] options = {"-json", "-std", "-d", "/generator/date-late-arriving_year.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_start_stop_001() {
        String[] options = {"-json", "-std", "-d", "/generator/date-start_stop.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_date_terminate_001() {
        String[] options = {"-json", "-std", "-d", "/generator/date-terminate.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_geo_001() {
        String[] options = {"-json", "-std", "-d", "/generator/geo.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_ip_as_001() {
        String[] options = {"-json", "-std", "-d", "/generator/ip-as.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_002() {
        String[] options = {"-json", "-std", "-d", "/generator/record-definition.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_reference_001() {
        String[] options = {"-json", "-std", "-d", "/generator/ref-state.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_reference_002() {
        String[] options = {"-json", "-std", "-d", "/generator/ref-city-state.yaml", "-c", "5"};
        doIt(options);
    }

    @Test
    public void test_repeat_001() {
        String[] options = {"-json", "-std", "-d", "/generator/wide-table.yaml", "-c", "5"};
        doIt(options);
    }

}