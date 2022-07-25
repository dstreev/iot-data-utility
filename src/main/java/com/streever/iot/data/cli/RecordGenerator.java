package com.streever.iot.data.cli;

import com.jcabi.manifests.Manifests;
import com.streever.iot.data.utility.generator.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RecordGenerator {
    private static Logger LOG = LogManager.getLogger(RecordGenerator.class);

    public static String DEFAULT_VALIDATION_YAML = "/validation/simple-default.yaml";
    public static String DEFAULT_VALIDATION_MULTI_YAML = "/validation/multi-default.yaml";
    public static Long DEFAULT_COUNT = 500l;

    private Options options;
    private CommandLine line;

    private void buildOptions() {
        options = new Options();

        Option HELP_OPTION = new Option("h", "help", false, "Help");

        Option OUTPUT_PREFIX_OPTION = new Option("p", "prefix", true,
                "Prefix for Output Spec.  For filesystems, this is an output directory.");
        OUTPUT_PREFIX_OPTION.setRequired(false);


        Option SCHEMA_CONFIG_OPTION = new Option("s", "schema", true,
                "Schema Config File.");
        SCHEMA_CONFIG_OPTION.setArgName("schema-file");
        SCHEMA_CONFIG_OPTION.setArgs(1);
        SCHEMA_CONFIG_OPTION.setRequired(false);
        SCHEMA_CONFIG_OPTION.setType(String.class);

        Option SCHEMA_DEFAULT_OPTION = new Option("ds", "default-schema", false,
                "Default (Sample) Schema Config File.");
        SCHEMA_DEFAULT_OPTION.setRequired(false);
        SCHEMA_DEFAULT_OPTION.setType(String.class);

        Option SCHEMA_DEFAULT_MULTI_OPTION = new Option("dms", "default-multi-schema", false,
                "Default (Sample) Multi Schema Config File.");
        SCHEMA_DEFAULT_MULTI_OPTION.setRequired(false);
        SCHEMA_DEFAULT_MULTI_OPTION.setType(String.class);

        OptionGroup schemaGroup = new OptionGroup();
        schemaGroup.setRequired(false);
        schemaGroup.addOption(SCHEMA_CONFIG_OPTION);
        schemaGroup.addOption(SCHEMA_DEFAULT_OPTION);
        schemaGroup.addOption(SCHEMA_DEFAULT_MULTI_OPTION);

        options.addOptionGroup(schemaGroup);

        Option COUNT_OPTION = new Option("c", "count", true,
                "Record count.");
        COUNT_OPTION.setArgName("count");
        COUNT_OPTION.setRequired(false);
        COUNT_OPTION.setType(Long.class);

        Option SIZE_MB_OPTION = new Option("mb", "megabyte", true,
                "Output size(mb).");
        SIZE_MB_OPTION.setArgName("megabytes");
        SIZE_MB_OPTION.setArgs(1);
        SIZE_MB_OPTION.setRequired(false);
        SIZE_MB_OPTION.setType(Long.class);

        Option SIZE_GB_OPTION = new Option("gb", "gigabyte", true,
                "Output size(gb).");
        SIZE_GB_OPTION.setArgName("gigabytes");
        SIZE_GB_OPTION.setRequired(false);
        SIZE_GB_OPTION.setType(Long.class);

        OptionGroup volumeGroup = new OptionGroup();
        volumeGroup.setRequired(false);
        volumeGroup.addOption(COUNT_OPTION);
        volumeGroup.addOption(SIZE_MB_OPTION);
        volumeGroup.addOption(SIZE_GB_OPTION);
        options.addOptionGroup(volumeGroup);

        Option OUTPUT_CONFIG_OPTION = new Option("o", "output", true,
                "Output Configuration.");
        OUTPUT_CONFIG_OPTION.setRequired(false);
        OUTPUT_CONFIG_OPTION.setType(String.class);

        Option LOCAL_OPTION = new Option("l", "local", false,
                "Local Filesystem Output.");
        LOCAL_OPTION.setRequired(false);
        LOCAL_OPTION.setType(String.class);

        Option HCFS_OPTION = new Option("hcfs", "hcfs", false,
                "HCFS (Hadoop Compatible File System) Output. ");
        HCFS_OPTION.setRequired(false);
        HCFS_OPTION.setType(String.class);

        Option STD_OPTION = new Option("std", "std", false,
                "STD Output.");
        STD_OPTION.setRequired(false);
        STD_OPTION.setType(String.class);

        Option CSV_OPTION = new Option("csv", "csv", false,
                "CSV Format");
        CSV_OPTION.setRequired(false);
        CSV_OPTION.setType(String.class);

        Option JSON_OPTION = new Option("json", "json", false,
                "JSON Format.");
        JSON_OPTION.setRequired(false);
        JSON_OPTION.setType(String.class);

        OptionGroup formatGroup = new OptionGroup();
        formatGroup.setRequired(false);
        formatGroup.addOption(CSV_OPTION);
        formatGroup.addOption(JSON_OPTION);
        options.addOptionGroup(formatGroup);

        Option DEBUG_OPTION = new Option("debug", "debug", false,
                "Debug.  Pause to allow remote jvm attachment.");
        DEBUG_OPTION.setRequired(false);

        Option GEN_SQL_SCHEMA_OPTION = new Option("sql", "sql", false,
                "Generate Hive Table.");
        GEN_SQL_SCHEMA_OPTION.setRequired(false);

        OptionGroup outputGroup = new OptionGroup();
        outputGroup.setRequired(false);

        outputGroup.addOption(OUTPUT_CONFIG_OPTION);
        outputGroup.addOption(GEN_SQL_SCHEMA_OPTION);
        outputGroup.addOption(STD_OPTION);
        outputGroup.addOption(HCFS_OPTION);
        outputGroup.addOption(LOCAL_OPTION);

        options.addOptionGroup(outputGroup);

        options.addOption(HELP_OPTION);
        options.addOption(OUTPUT_PREFIX_OPTION);
        options.addOption(SCHEMA_CONFIG_OPTION);

        OptionGroup uniqueGroup = new OptionGroup();
        uniqueGroup.setRequired(false);

        Option UUID_OPTION = new Option("uuid", "uuid", false,
                "Append UUID to output reference.");
        UUID_OPTION.setRequired(false);
        UUID_OPTION.setType(String.class);

        Option TIMESTAMP_OPTION = new Option("ts", "timestamp", false,
                "Append `Timestamp` to output reference.");
        TIMESTAMP_OPTION.setRequired(false);
        TIMESTAMP_OPTION.setType(String.class);

        uniqueGroup.addOption(UUID_OPTION);
        uniqueGroup.addOption(TIMESTAMP_OPTION);
        options.addOptionGroup(uniqueGroup);

        options.addOption(DEBUG_OPTION);

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

        try {
            line = parser.parse(options, args, true);
        } catch (ParseException pe) {
            printUsage("Missing Required Elements. " + StringUtils.join(args,","));
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
            Schema record = null;
            if (line.hasOption("s")) {
                String schemaFile = line.getOptionValue("s");
                LOG.info("Schema File: " + schemaFile);
                record = Schema.deserializeResource(schemaFile);
            } else {
                // use a default for testing.
                if (line.hasOption("dms")) {
                    LOG.info("Default MULTI schema specified.  Using default 'multi-validation' schema in 'resources' for testing: " + DEFAULT_VALIDATION_MULTI_YAML);
                    record = Schema.deserializeResource(DEFAULT_VALIDATION_MULTI_YAML);
                } else if (line.hasOption("ds")) {
                        LOG.info("Default schema specified.  Using default 'validation' schema in 'resources' for testing: " + DEFAULT_VALIDATION_YAML);
                        record = Schema.deserializeResource(DEFAULT_VALIDATION_YAML);
                } else {
                    LOG.info("No schema specified.  Using default 'validation' schema in 'resources' for testing: " + DEFAULT_VALIDATION_YAML);
                    record = Schema.deserializeResource(DEFAULT_VALIDATION_YAML);
                }
            }
            RecordBuilder builder = new RecordBuilder();
            builder.setSchema(record);

            if (line.hasOption("sql")) {
                builder.init();
                SqlBuilder sBuilder = new HiveSqlBuilder();
                sBuilder.setSchema(record);
                System.out.println(sBuilder.build());
            } else {
                if (line.hasOption("c")) {
                    Long count = Long.valueOf(line.getOptionValue("c"));
                    LOG.info("Count: " + count);
                    builder.setCount(count);
                } else if (line.hasOption("mb")) {
                    Long mbSize = Long.valueOf(line.getOptionValue("mb"));
                    LOG.info("MB Size: " + mbSize);
                    Long mb = mbSize * (1024 * 1024);
                    builder.setSize(mb);
                } else if (line.hasOption("gb")) {
                    Long gbSize = Long.valueOf(line.getOptionValue("gb"));
                    LOG.info("GB Size: " + gbSize);
                    Long gb = gbSize * (1024 * 1024 * 1024);
                    builder.setSize(gb);
                } else {
                    // Limit when nothing specified.
                    LOG.info("No Size/Count specified, using default: " + DEFAULT_COUNT);
                    builder.setCount(DEFAULT_COUNT);
                }

                // Use the supplied ouput spec
                if (line.hasOption("o")) {
                    String ops = line.getOptionValue("o");
                    LOG.info("Output Configuration: " + ops);
                    OutputConfig outputCfg = OutputConfig.deserialize(ops);
                    builder.setOutputConfig(outputCfg);
                } else {
                    // or build the reference based in flags.
                    String[] specOutput = new String[2];
                    if (line.hasOption("csv")) {
                        LOG.info("Output Spec: csv");
                        specOutput[0] = "csv";
                    }
                    if (line.hasOption("json")) {
                        LOG.info("Output Spec: json");
                        specOutput[0] = "json";
                    }
                    if (line.hasOption("std")) {
                        LOG.info("Output Spec: std");
                        specOutput[1] = "std";
                    }
                    if (line.hasOption("local")) {
                        LOG.info("Output Spec: local");
                        specOutput[1] = "local";
                    }
                    if (line.hasOption("hcfs")) {
                        LOG.info("Output Spec: hcfs");
                        specOutput[1] = "hcfs";
                    }

                    String specFile = null;
                    if (specOutput[0] != null && specOutput[1] != null) {
                        specFile = specOutput[0] + "_" + specOutput[1];
                    } else if (specOutput[0] != null && specOutput[1] == null) {
                        specFile = specOutput[0] + "_std";
                    } else if (specOutput[0] == null && specOutput[1] != null) {
                        specFile = "csv_" + specOutput[1];
                    } else {
                        specFile = "csv_std";
                    }

                    if (!line.hasOption("std") && line.hasOption("ts")) {
                        specFile = specFile + "_ts";
                    } else if (!line.hasOption("std") && line.hasOption("uuid")) {
                        specFile = specFile + "_uuid";
                    }
                    specFile = "/standard/" + specFile + ".yaml";
                    LOG.info("SpecFile: " + specFile);

                    OutputConfig outputSpec = OutputConfig.deserialize(specFile);
                    builder.setOutputConfig(outputSpec);
                }
                if (line.hasOption("p")) {
                    String outputPrefix = line.getOptionValue("p");
                    LOG.info("Output Prefix: " + outputPrefix);
                    builder.setOutputPrefix(outputPrefix);
                }
                builder.init();
                long[] counts = builder.run();
            }
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
