package com.streever.iot.data.cli;

import com.jcabi.manifests.Manifests;
import com.streever.iot.data.utility.generator.*;
import com.streever.iot.data.utility.generator.output.FileOutput;
import org.apache.commons.cli.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DomainGenerator {
    private static Logger LOG = LogManager.getLogger(DomainGenerator.class);

    public static String DEFAULT_VALIDATION_YAML = "/validation/simple-default.yaml";
    public static String DEFAULT_VALIDATION_MULTI_YAML = "/validation/multi-default.yaml";
    public static Long DEFAULT_COUNT = 500l;

    //    private Options options;
    private CommandLine line;

    public static Options getOptions() {
        Options options = new Options();

        Option HELP_OPTION = new Option("h", "help", false, "Help");

        Option OUTPUT_DIRECTORY_OPTION = new Option("o", "output-dir", true,
                "Output directory.");
        OUTPUT_DIRECTORY_OPTION.setRequired(false);


        Option DOMAIN_CONFIG_OPTION = new Option("d", "domain", true,
                "Domain Config File.");
        DOMAIN_CONFIG_OPTION.setArgName("domain-file");
        DOMAIN_CONFIG_OPTION.setArgs(1);
        DOMAIN_CONFIG_OPTION.setRequired(false);
        DOMAIN_CONFIG_OPTION.setType(String.class);


        Option DOMAIN_DEFAULT_OPTION = new Option("dd", "default-domain", false,
                "Default (Sample) Domain Config File.");
        DOMAIN_DEFAULT_OPTION.setRequired(false);
        DOMAIN_DEFAULT_OPTION.setType(String.class);

        OptionGroup domainGroup = new OptionGroup();
        domainGroup.setRequired(true);
        domainGroup.addOption(DOMAIN_CONFIG_OPTION);
        domainGroup.addOption(DOMAIN_DEFAULT_OPTION);

        options.addOptionGroup(domainGroup);

        Option ANCHOR_SCHEMA_OPTION = new Option("s", "schema", true,
                "Anchor Schema name.");
        ANCHOR_SCHEMA_OPTION.setArgName("schema-name");
        ANCHOR_SCHEMA_OPTION.setArgs(1);
        ANCHOR_SCHEMA_OPTION.setRequired(false);
        ANCHOR_SCHEMA_OPTION.setType(String.class);
        options.addOption(ANCHOR_SCHEMA_OPTION);

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

        Option TOKEN_REPLACEMENT = new Option("tr", "token-replacement", true,
                "Comma separated key=value pairs used to replace tokens in the config.");
        TOKEN_REPLACEMENT.setArgName("key=value");
        TOKEN_REPLACEMENT.setRequired(Boolean.FALSE);
        TOKEN_REPLACEMENT.setValueSeparator(',');
        TOKEN_REPLACEMENT.setArgs(100);
        options.addOption(TOKEN_REPLACEMENT);

        Option OUTPUT_CONFIG_OPTION = new Option("oc", "output-configuration", true,
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
        options.addOption(OUTPUT_DIRECTORY_OPTION);
        options.addOption(DOMAIN_CONFIG_OPTION);

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

        return options;
    }

    private void printUsage(String statement) {
        HelpFormatter formatter = new HelpFormatter();
        String footer = DomainGenerator.substituteVariables("v.${Implementation-Version}");
        formatter.printHelp("java " + this.getClass().getCanonicalName(), statement, getOptions(), footer, true);
    }

    private boolean init(String[] args) {
        boolean rtn = true;
        Options options = getOptions();

        CommandLineParser parser = new PosixParser();

        try {
            line = parser.parse(options, args, true);
        } catch (ParseException pe) {
            HelpFormatter formatter = new HelpFormatter();
            // TODO: Proper cli usage...
            formatter.printHelp(100, "hello", "Missing Required Elements", options, "footer");
//            printUsage("Missing Required Elements. " + StringUtils.join(args, ","));
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

        if (!init(args)) {
            rtn = -1;
        } else {
            if (line.hasOption("debug")) {
                Scanner sc = new Scanner(System.in);
                System.out.println("Attached Remote Debugger <enter to proceed>");
                sc.nextLine();
            }
            Domain domain = null;
            String anchorSchema = null;
            String domainFile = null;
            if (line.hasOption("d")) {
                domainFile = line.getOptionValue("d");
            } else {
                // use a default for testing.
                if (line.hasOption("dd")) {
                    LOG.info("Default domain specified.  Using default 'validation' domain in 'resources' for testing: " + DEFAULT_VALIDATION_MULTI_YAML);
                    domainFile = DEFAULT_VALIDATION_MULTI_YAML;
                    if (anchorSchema == null)
                        anchorSchema = "transaction";
                } else {
                    LOG.info("No domain specified.  Using default 'validation' domain in 'resources' for testing: " + DEFAULT_VALIDATION_YAML);
                    domainFile = DEFAULT_VALIDATION_YAML;
                    if (anchorSchema == null)
                        anchorSchema = "account";
                }
            }
            if (line.hasOption("s")) {
                anchorSchema = line.getOptionValue("s");
            }
            LOG.info("Domain File: " + domainFile);
            Map<String, Object> tokenReplacements = null;
            if (line.hasOption("tr")) {
                // token replacements
                tokenReplacements = new HashMap<String, Object>();
                String[] tokenReplacementStrs = line.getOptionValues("tr");
                for (String trstr : tokenReplacementStrs) {
                    String[] parts = trstr.split("=");
                    if (parts.length == 2) {
                        tokenReplacements.put(parts[0], parts[1]);
                    }
                }
            }
            domain = Domain.deserializeResource(tokenReplacements, domainFile);
            domain.completeAssociations();
            List<String> reasons = new ArrayList<String>();
            if (!domain.validate(reasons)) {
                System.out.println(reasons.stream().map(r -> r.toString()).collect(Collectors.joining("\n")));
                throw new RuntimeException("Config didn't pass validation");
            }

            DomainBuilder builder = new DomainBuilder(domain, anchorSchema);

            if (line.hasOption("sql")) {
                builder.init();
                SqlBuilder sBuilder = new HiveSqlBuilder();
                // TODO: Fix sqlBuilder
//                sBuilder.setSchema(domain);
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
                    specFile = "json_std";
                }

                if (!line.hasOption("std") && line.hasOption("ts")) {
                    specFile = specFile + "_ts";
                } else if (!line.hasOption("std") && line.hasOption("uuid")) {
                    specFile = specFile + "_uuid";
                }
                specFile = "/standard/" + specFile + ".yaml";
                LOG.info("SpecFile: " + specFile);

                OutputConfig outputSpec = OutputConfig.deserialize(specFile);
                if (outputSpec.getConfig() instanceof FileOutput) {
                    ((FileOutput) outputSpec.getConfig()).setFilename(builder.getAnchorSchema().getTitle());
                }
                builder.setOutputConfig(outputSpec);

                if (line.hasOption("o")) {
                    String outputPrefix = line.getOptionValue("o");
                    LOG.info("Output Prefix: " + outputPrefix);
                    builder.setOutputPrefix(outputPrefix);
                }
                builder.init();
                long[] counts = builder.run();
                System.out.println(builder.getTerminationReason());
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
        DomainGenerator cli = new DomainGenerator();
        try {
            result = cli.run(args);
        } catch (IOException e) {
            result = -1;
            e.printStackTrace();
        }
        System.exit(result);
    }

}
