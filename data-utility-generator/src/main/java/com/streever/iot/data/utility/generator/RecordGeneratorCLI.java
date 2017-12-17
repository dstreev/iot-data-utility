package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RecordGeneratorCLI {
    private Options options;
    private Long count;
    private String outputFilename;
    private String configurationFile;
    private Boolean tsOnFile;

    private void buildOptions() {
        options = new Options();

        Option oHelp = Option.builder("h")
                .argName("help")
                .desc("This Help")
                .hasArg(false)
                .required(false)
                .build();

        Option oOutput = Option.builder("o")
                .argName("output")
                .desc("Output Filename")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(true)
                .build();

        Option oConfig = Option.builder("cfg")
                .argName("config")
                .desc("Configuration Filename")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(true)
                .build();

        Option oCount = Option.builder("c")
                .argName("count")
                .desc("Record Count")
                .hasArg(true)
                .numberOfArgs(1)
                .type(Long.class)
                .required(true)
                .build();

        Option oTimestamp = Option.builder("t")
                .argName("timestamp")
                .desc("Add Timestamp to Filename")
                .hasArg(false)
                .required(false)
                .build();

        options.addOption(oHelp);
        options.addOption(oOutput);
        options.addOption(oConfig);
        options.addOption(oCount);
        options.addOption(oTimestamp);

    }

    private void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java " + this.getClass().getCanonicalName(), options);
    }

    private boolean checkUsage(String[] args) {
        boolean rtn = true;
        buildOptions();

        CommandLineParser clParser = new DefaultParser();
        CommandLine line = null;
        try {
            line = clParser.parse(options, args, true);
        } catch (ParseException pe) {
            return false;
        }

        if (line.hasOption("h")) {
            return false;
        }

        count = Long.parseLong(line.getOptionValue("c"));

        outputFilename = line.getOptionValue("o");
        configurationFile = line.getOptionValue("cfg");
        if (line.hasOption("t")) {
            tsOnFile = true;
        } else {
            tsOnFile = false;
        }

        return rtn;
    }


    public int run(String[] args) throws Exception {
        int rtn = 0;

        if (!checkUsage(args)) {
            System.out.println("Check Usage:");
            printUsage();
            rtn = -1;
        } else {
            ObjectMapper mapper = new ObjectMapper();

            File file = new File(configurationFile);
            JsonNode rootNode = mapper.readValue(file, JsonNode.class);

            RecordGenerator recGen = new RecordGenerator(rootNode);
            buildFile(recGen, count);

        }

        return rtn;
    }

    private void buildFile(RecordGenerator recGen, long count) {
        try {
            String output = null;

            if (tsOnFile) {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HHmmss");

                String extension = FilenameUtils.getExtension(outputFilename);
                String baseName = FilenameUtils.getBaseName(outputFilename);
                String fullPath = FilenameUtils.getFullPath(outputFilename);
                String now = df.format(new Date());

                output = fullPath + File.separator + baseName + "_" + now + "." + extension;
            } else {
                output = outputFilename;
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(output));
            for (long i = 0; i < count; i++) {
                writer.append(recGen.next());
                writer.append("\n");
            }
            writer.close();
        } catch (Throwable t) {
            System.out.println(t.getMessage());
        }
    }


    public static void main(String[] args) throws Exception {
        int result;
        RecordGeneratorCLI cli = new RecordGeneratorCLI();
        result = cli.run(args);
        System.exit(result);
    }

}
