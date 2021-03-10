package com.streever.iot.data.cli;

import com.jcabi.manifests.Manifests;
import com.streever.iot.data.utility.generator.RecordBuilder;
import com.streever.iot.data.utility.generator.OutputSpec;
import com.streever.iot.data.utility.generator.Schema;
import com.streever.iot.data.utility.generator.SqlBuilder;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RecordGenerator {

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
        SCHEMA_CONFIG_OPTION.setRequired(true);
        SCHEMA_CONFIG_OPTION.setType(String.class);

        Option COUNT_OPTION = new Option("c", "count", true,
                "Record count.");
        COUNT_OPTION.setRequired(false);
        COUNT_OPTION.setType(Long.class);

        Option SIZE_MB_OPTION = new Option("mb", "megabyte", true,
                "Output size(mb).");
        SIZE_MB_OPTION.setRequired(false);
        SIZE_MB_OPTION.setType(Long.class);

        Option SIZE_GB_OPTION = new Option("gb", "gigabyte", true,
                "Output size(gb).");
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
            if (line.hasOption("sql")) {
                SqlBuilder sBuilder = new SqlBuilder();
                Schema schema = Schema.deserialize(line.getOptionValue("s"));
                sBuilder.setSchema(schema);
                System.out.println(sBuilder.build());

            } else {
                RecordBuilder builder = new RecordBuilder();
                Schema record = Schema.deserialize(line.getOptionValue("s"));
                builder.setSchema(record);

                if (line.hasOption("c")) {
                    builder.setCount(Long.valueOf(line.getOptionValue("c")));
                } else if (line.hasOption("mb")) {
                    Long mb = Long.valueOf(line.getOptionValue("mb")) * (1024 * 1024);
                    builder.setSize(mb);
                } else if (line.hasOption("gb")) {
                    Long gb = Long.valueOf(line.getOptionValue("gb")) * (1024 * 1024 * 1024);
                    builder.setSize(gb);
                }

                // Use the supplied ouput spec
                if (line.hasOption("o")) {
                    OutputSpec outputSpec = OutputSpec.deserialize(line.getOptionValue("o"));
                    builder.setOutputSpec(outputSpec);
                } else {
                    // or build the reference based in flags.
                    String[] specOutput = new String[2];
                    if (line.hasOption("csv")) {
                        specOutput[0] = "csv";
                    }
                    if (line.hasOption("json")) {
                        specOutput[0] = "json";
                    }
                    if (line.hasOption("std")) {
                        specOutput[1] = "std";
                    }
                    if (line.hasOption("local")) {
                        specOutput[1] = "local";
                    }
                    if (line.hasOption("hcfs")) {
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
                    OutputSpec outputSpec = OutputSpec.deserialize("/standard/" + specFile + ".yaml");
                    builder.setOutputSpec(outputSpec);
                }
                if (line.hasOption("p")) {
                    builder.setOutputPrefix(line.getOptionValue("p"));
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
