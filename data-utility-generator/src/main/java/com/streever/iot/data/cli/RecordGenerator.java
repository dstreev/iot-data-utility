package com.streever.iot.data.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
//import com.streever.iot.data.utility.generator.Child;
//import com.streever.iot.data.utility.generator.Record;
import com.jcabi.manifests.Manifests;
import com.streever.iot.data.utility.generator.Builder;
import com.streever.iot.data.utility.generator.OutputSpec;
import com.streever.iot.data.utility.generator.Record;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.CSVOutput;
import com.streever.iot.data.utility.generator.output.OutputBase;
import com.streever.iot.kafka.producer.KafkaProducerConfig;
import com.streever.iot.kafka.producer.ProducerCreator;
import com.streever.iot.kafka.spec.ProducerSpec;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.producer.Producer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RecordGenerator {

    private Options options;
    private CommandLine line;

    private void buildOptions() {
        options = new Options();

//        Option HELP_OPTION = new Option("h", "help", false, "Help");
        Option HELP_OPTION = Option.builder("h")
                .argName("help")
                .desc("This Help")
                .longOpt("help")
                .hasArg(false)
                .required(false)
                .build();

//        Option OUTPUT_PREFIX_OPTION = new Option("p", "prefix", true,
//                "Prefix for Output Spec.  For filesystems, this is an output directory.");
//        OUTPUT_PREFIX_OPTION.setRequired(false);
        Option OUTPUT_PREFIX_OPTION = Option.builder("p")
                .argName("OUTPUT_PREFIX")
                .desc("Prefix for Output Spec.  For filesystems, this is an output directory.")
                .longOpt("prefix")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(false)
                .build();

//        Option SCHEMA_CONFIG_OPTION = new Option("s", "schema", true,
//                "Schema File.");
//        SCHEMA_CONFIG_OPTION.setRequired(true);
//        SCHEMA_CONFIG_OPTION.setType(String.class);
        Option SCHEMA_CONFIG_OPTION = Option.builder("cfg")
                .argName("SCHEMA_CONFIG_FILE")
                .desc("Schema Filename")
                .longOpt("schema-cfg")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(true)
                .build();

//        Option COUNT_OPTION = new Option("c", "count", true,
//                "Record count.");
//        COUNT_OPTION.setRequired(false);
//        COUNT_OPTION.setType(Long.class);
        Option COUNT_OPTION = Option.builder("c")
                .argName("COUNT")
                .desc("Record Count")
                .longOpt("count")
                .hasArg(true)
                .numberOfArgs(1)
                .type(Long.class)
                .required(false)
                .build();

        Option SIZE_OPTION = Option.builder("s")
                .argName("SIZE")
                .desc("Record(s) Size")
                .longOpt("size")
                .hasArg(true)
                .numberOfArgs(1)
                .type(Long.class)
                .required(false)
                .build();


//        Option OUTPUT_CONFIG_OPTION = new Option("o", "output", true,
//                "Output Configuration.");
//        OUTPUT_CONFIG_OPTION.setRequired(false);
//        OUTPUT_CONFIG_OPTION.setType(String.class);
        Option OUTPUT_CONFIG_OPTION = Option.builder("o")
                .argName("OUTPUT_CONFIG_FILE")
                .longOpt("output")
                .desc("Output Configuration")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(false)
                .build();

//        Option STD_OPTION = new Option("std", "std", false,
//                "STD Output.");
//        STD_OPTION.setRequired(false);
//        STD_OPTION.setType(String.class);
        Option STD_OPTION = Option.builder("std")
                .argName("std")
                .longOpt("std")
                .desc("STD Output")
                .hasArg(false)
                .type(String.class)
                .required(false)
                .build();

//        Option CSV_OPTION = new Option("csv", "csv", false,
//                "CSV Output.");
//        CSV_OPTION.setRequired(false);
//        CSV_OPTION.setType(String.class);
        Option CSV_OPTION = Option.builder("csv")
                .argName("csv")
                .longOpt("csv")
                .desc("CSV Output")
                .hasArg(false)
                .type(String.class)
                .required(false)
                .build();

//        Option JSON_OPTION = new Option("json", "json", false,
//                "JSON Output.");
//        JSON_OPTION.setRequired(false);
//        JSON_OPTION.setType(String.class);
        Option JSON_OPTION = Option.builder("json")
                .argName("json")
                .longOpt("json")
                .desc("json Output")
                .hasArg(false)
                .type(String.class)
                .required(false)
                .build();

        /* MOVE ALL THIS TO THE OUTPUT SPEC
        Option STREAMING_DURATION_OPTION = Option.builder("sd")
                .argName("STREAMING_DURATION")
                .desc("Streaming Duration (default -1) (requires '-scfg' option)")
                .type(Integer.class)
                .hasArg(true)
                .required(false)
                .build();

        Option BURST_MAX_OPTION = Option.builder("bm")
                .argName("BURST_MAX")
                .desc("Burst Max (requires '-scfg' option)")
                .type(Integer.class)
                .hasArg(true)
                .required(false)
                .build();

        Option PAUSE_OPTION = Option.builder("p")
                .argName("PAUSE_MAX")
                .desc("pause max millis (requires '-scfg' option)")
                .type(Integer.class)
                .hasArg(true)
                .required(false)
                .build();

        Option RANDOMIZE_BURST_OPTION = Option.builder("rb")
                .argName("RANDOMIZE_BURST")
                .desc("randomize burst (requires '-scfg' option)")
                .type(Boolean.class)
                .hasArg(false)
                .required(false)
                .build();

        Option RANDOMIZE_PAUSE_OPTION = Option.builder("rp")
                .argName("RANDOMIZE_PAUSE")
                .desc("randomize pause (requires '-scfg' option)")
                .type(Boolean.class)
                .hasArg(false)
                .required(false)
                .build();
        */

        Option DEBUG_OPTION = new Option("debug", "debug", false,
                "Debug.  Pause to allow remote jvm attachment.");
        DEBUG_OPTION.setRequired(false);

//        Option GEN_HIVE_SCHEMA_OPTION = new Option("hive", "hive", false,
//                "Generate Hive Table.");
//        GEN_HIVE_SCHEMA_OPTION.setRequired(false);
        Option GEN_HIVE_SCHEMA_OPTION = Option.builder("hive")
                .argName("HIVE_TABLE")
                .desc("Generate Hive Table")
                .longOpt("gen-hive-schema")
                .required(false)
                .build();

        OptionGroup outputGroup = new OptionGroup();
        outputGroup.setRequired(false);

        outputGroup.addOption(OUTPUT_CONFIG_OPTION);
        outputGroup.addOption(GEN_HIVE_SCHEMA_OPTION);
        outputGroup.addOption(STD_OPTION);
        outputGroup.addOption(CSV_OPTION);
        outputGroup.addOption(JSON_OPTION);

        options.addOptionGroup(outputGroup);

        options.addOption(HELP_OPTION);
        options.addOption(OUTPUT_PREFIX_OPTION);
        options.addOption(SCHEMA_CONFIG_OPTION);

//        options.addOption(TIMESTAMP_OPTION);

        options.addOption(COUNT_OPTION);
        options.addOption(SIZE_OPTION);
        options.addOption(DEBUG_OPTION);

//        options.addOption(BURST_MAX_OPTION);
//        options.addOption(STREAMING_DURATION_OPTION);
//        options.addOption(PAUSE_OPTION);
//        options.addOption(RANDOMIZE_BURST_OPTION);
//        options.addOption(RANDOMIZE_PAUSE_OPTION);

    }

    private void printUsage(String statement) {
        HelpFormatter formatter = new HelpFormatter();
        String footer = RecordGenerator.substituteVariables("v.${Implementation-Version}");
        formatter.printHelp("java " + this.getClass().getCanonicalName(), statement, options, footer, true);
    }

    private boolean checkUsage(String[] args) {
        boolean rtn = true;
        buildOptions();

        CommandLineParser parser = new PosixParser();
//        CommandLineParser parser = new DefaultParser();
//        CommandLine line = null;

        try {
            line = parser.parse(options, args, true);
        } catch (ParseException pe) {
            printUsage("Missing Required Elements");
            return false;
        }

        if (line.hasOption("h")) {
            printUsage("HELP");
            return false;
        }
        return rtn;
    }

    public int run(String[] args) throws IOException {
        int rtn = 0;

        if (!checkUsage(args)) {
            rtn = -1;
        } else {
            if (line.hasOption("debug")) {
                Scanner sc = new Scanner(System.in);
                System.out.println("Attached Remote Debugger <enter to proceed>");
                sc.nextLine();
            }
            Builder builder = new Builder();
            Record record = Record.deserialize(line.getOptionValue("cfg"));
            builder.setRecord(record);
            if (line.hasOption("c")) {
                builder.setCount(Long.valueOf(line.getOptionValue("c")));
            }
            if (line.hasOption("s")) {
                builder.setSize(Long.valueOf(line.getOptionValue("s")));
            }
            if (line.hasOption("o")) {
                OutputSpec outputSpec = OutputSpec.deserialize(line.getOptionValue("o"));
                builder.setOutputSpec(outputSpec);
            }
            if (line.hasOption("std")) {
                OutputSpec outputSpec = OutputSpec.deserialize("/default_out.yaml");
                builder.setOutputSpec(outputSpec);
            }
            if (line.hasOption("csv")) {
                OutputSpec outputSpec = OutputSpec.deserialize("/csv_out.yaml");
                builder.setOutputSpec(outputSpec);
            }
            if (line.hasOption("json")) {
                OutputSpec outputSpec = OutputSpec.deserialize("/json_out.yaml");
                builder.setOutputSpec(outputSpec);
            }

            if (line.hasOption("p")) {
                builder.setOutputPrefix(line.getOptionValue("p"));
            }
            builder.init();
            long[] counts = builder.run();
            rtn = 0;
        }
        return rtn;
    }

    public static String substituteVariables(String template) {
        Pattern pattern = Pattern.compile("\\$\\{(.+?)\\}");
        Matcher matcher = pattern.matcher(template);
        // StringBuilder cannot be used here because Matcher expects StringBuffer
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String matchStr = matcher.group(1);
            try {
                String replacement = Manifests.read(matchStr);
                if (replacement != null) {
                    // quote to work properly with $ and {,} signs
                    matcher.appendReplacement(buffer, replacement != null ? Matcher.quoteReplacement(replacement) : "null");
                } else {
//                    System.out.println("No replacement found for: " + matchStr);
                }
            } catch (IllegalArgumentException iae) {
                //iae.printStackTrace();
                // Couldn't locate MANIFEST Entry.
                // Silently continue. Usually happens in IDE->run.
            }
        }
        matcher.appendTail(buffer);
        String rtn = buffer.toString();
        return rtn;
    }

    public static void main(String[] args) {
        int result;
        RecordGenerator cli = new RecordGenerator();
        try {
            result = cli.run(args);
        } catch (IOException e) {
            result = -1;
            e.printStackTrace();
        }
        System.exit(result);
    }

}
