/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streever.iot.data.mapreduce;

//import spec.bridge.hadoop2.KafkaOutputFormat;

import com.streever.iot.data.utility.generator.Relationship;
import com.streever.iot.data.utility.generator.Schema;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

//import static com.streever.iot.data.mapreduce.DataGenMapper.SCHEMA_FILE;

public class DataGenTool extends Configured implements Tool {

    static private Logger LOG = Logger.getLogger(DataGenTool.class.getName());
    public static final String SCHEMA_FILE = "schema.file";
//    public static final String DATAGEN_PARTITION = "datagen.partition";

    private Options options;
    private Path outputPath;
//    private String partitionPath;
    private Boolean forceFlat = Boolean.FALSE;

    private Schema schema;

    //        private Sink sink;
    public static final int DEFAULT_MAPPERS = 2;
    public static final long DEFAULT_COUNT = 100;

    public enum Sink {HDFS, KAFKA}

    ;

    public DataGenTool() {
        buildOptions();
    }

    private void buildOptions() {
        options = new Options();

        Option help = new Option("h", "help", false, "Help");
        help.setRequired(false);

//        Option help = Option.builder("h")
//                .argName("help")
//                .desc("This Help")
//                .hasArg(false)
//                .required(false)
//                .build();

        Option outputDir = new Option("d", "directory", true, "Output Directory");
        outputDir.setRequired(false);
//        Option outputDir = Option.builder("d")
//                .argName("directory")
//                .desc("Output Directory")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .type(String.class)
//                .required(true)
//                .build();

        Option mappers = new Option("p", "parallelism", true, "Parallelism");
        mappers.setRequired(false);
//        Option mappers = Option.builder("m")
//                .argName("mappers")
//                .desc("Parallelism")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .type(Integer.class)
//                .required(true)
//                .build();

        OptionGroup schemaGroup = new OptionGroup();

        Option nestedOption = new Option("dn", "default-nested", false, "Nested Hierarchy Sample Schema");
        Option defaultOption = new Option("ds", "default-simple", false, "Sample Schema");
        Option config = new Option("s", "schema", true, "Schema File in HDFS");

        schemaGroup.addOption(nestedOption);
        schemaGroup.addOption(defaultOption);
        schemaGroup.addOption(config);
        schemaGroup.setRequired(true);
        options.addOptionGroup(schemaGroup);


        OptionGroup layoutOptionGroup = new OptionGroup();

        Option partitionOption = new Option("partition", "partition", true, "Partition");
        partitionOption.setRequired(false);
        options.addOption(partitionOption);

        Option forceFlatOption = new Option("ff", "force-flat", true, "Force Flat Output");

//        layoutOptionGroup.addOption(partitionOption);
        layoutOptionGroup.addOption(forceFlatOption);
        layoutOptionGroup.setRequired(false);
        options.addOptionGroup(layoutOptionGroup);

//        Option config = Option.builder("cfg")
//                .argName("config")
//                .desc("Configuration Filename (in HDFS)")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .type(String.class)
//                .required(true)
//                .build();

        Option count = new Option("c", "count", true, "Record Count");
        count.setRequired(false);
//        Option count = Option.builder("c")
//                .argName("count")
//                .desc("Record Count")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .type(Long.class)
//                .required(true)
//                .build();


        options.addOption(help);
        options.addOption(mappers);
        options.addOption(outputDir);
        options.addOption(count);
    }

    private void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop jar <jar-file> " + this.getClass().getCanonicalName(), options);
    }

    protected void setup(Job job) {
        // Get the conf location from the job conf.

        String config = job.getConfiguration().get(SCHEMA_FILE);
        if (config.equals(DataGenMultiMapper.DEFAULT_MULTI_CONFIG_RESOURCE_FILE) || config.equals(DataGenMapper.DEFAULT_CONFIG_RESOURCE_FILE)) {
            // Use the default validation file.
            try {
                InputStream configInputStream = getClass().getResourceAsStream(config);
                schema = Schema.deserializeInputStream(configInputStream);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } else {
            LOG.info("Config File: " + config);

            // Read the Config from the path.
            FileSystem FS1 = null;
            FSDataInputStream dfsConfigInputStream = null;
            try {
                FS1 = FileSystem.get(job.getConfiguration());

                Path path = new Path(config);

                dfsConfigInputStream = FS1.open(path);

                schema = Schema.deserializeInputStream(dfsConfigInputStream);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                IOUtils.closeStream(dfsConfigInputStream);
            }
        }

        schema.link();
        schema.validate();

        // Multi-File Output.
        if (schema.getRelationships().size() > 0) {
            // There is a hierarchy.
            LOG.info("Multi-File Output");
            if (!forceFlat) {
                job.setMapperClass(DataGenMultiMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                // Remove write of mapper.
                LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
                // Causes NPE
//                job.setOutputFormatClass(LazyOutputFormat.class);

                // Set Reducer, since there is a hierarchy.
                job.setReducerClass(DataGenReducer.class);

                // Force Reduce to push different writes.
                for (Map.Entry<String, Relationship> relationshipEntry : schema.getRelationships().entrySet()) {
                    LOG.info("Multi-part: " + relationshipEntry.getKey());
                    MultipleOutputs.addNamedOutput(job, relationshipEntry.getKey(), TextOutputFormat.class, Text.class, Text.class);
                }
            } else {
                // Flat Output (even when hierarchy present)
                job.setMapperClass(DataGenMapper.class);
                job.setMapOutputKeyClass(NullWritable.class);
                // Map Only.
                job.setNumReduceTasks(0);
            }
        } else {
            // Map Only Job
            job.setMapperClass(DataGenMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setNumReduceTasks(0);
        }

    }

    private boolean checkUsage(String[] args, Job job) {
        boolean rtn = true;
        Configuration configuration = job.getConfiguration();

        CommandLineParser clParser = new PosixParser();

        CommandLine line = null;
        try {
            line = clParser.parse(options, args);
        } catch (ParseException pe) {
            pe.printStackTrace();
            return false;
        }

        if (line.hasOption("h")) {
            return false;
        }

        job.setInputFormatClass(DataGenInputFormat.class);

        if (line.hasOption("c")) {
            DataGenInputFormat.setNumberOfRows(job, Long.parseLong(line.getOptionValue("c")));
        } else {
            DataGenInputFormat.setNumberOfRows(job, DEFAULT_COUNT);
        }

        if (line.hasOption("p")) {
            int parallelism = Integer.parseInt(line.getOptionValue("p"));
            job.setNumReduceTasks(parallelism);
            configuration.setInt(MRJobConfig.NUM_MAPS, Integer.parseInt(line.getOptionValue("p")));
//            configuration.setInt(MRJobConfig.NUM_REDUCES, Integer.parseInt(line.getOptionValue("p")));
        } else {
            // Default
            configuration.setInt(MRJobConfig.NUM_MAPS, DEFAULT_MAPPERS);
            job.setNumReduceTasks(DEFAULT_MAPPERS);
//            configuration.setInt(MRJobConfig.NUM_REDUCES, DEFAULT_MAPPERS);
        }

        if (line.hasOption("ff")) {
            forceFlat = Boolean.TRUE;
        }

//        if (line.hasOption("p")) {
//            configuration.set(DATAGEN_PARTITION, line.getOptionValue("p"));
//            partitionPath = line.getOptionValue("p");
//        }

//        if (line.hasOption("sink")) {
//            String sinkOption = line.getOptionValue("sink");
//            try {
//                sink = Sink.valueOf(sinkOption.toUpperCase());
//
//                job.setInputFormatClass(DataGenInputFormat.class);
//
//                LOG.info("Using Sink:" + sink.toString());
//
//                switch (sink) {
//                    case HDFS:
//                        job.setOutputFormatClass(TextOutputFormat.class);
//                        if (line.hasOption("output")) {
//                            outputPath = new Path(line.getOptionValue("output"));
//                            FileOutputFormat.setOutputPath(job, outputPath);
//                            job.setMapperClass(DataGenMapper.class);
//                            job.setMapOutputKeyClass(NullWritable.class);
//                            job.setMapOutputValueClass(Text.class);
//                        } else {
//                            return false;
//                        }
//                        break;
//                    case KAFKA:
//                        job.setOutputFormatClass(KafkaOutputFormat.class);
//                        if (line.hasOption("output")) {
//                            outputPath = new Path(line.getOptionValue("output"));
//                            // The Topic should be included in the URL as well.
//                            job.setMapperClass(KafkaDataGenMapper.class);
////                            job.setMapOutputKeyClass(NullWritable.class);
////                            job.setMapOutputValueClass(byte[].class);
//                            KafkaOutputFormat.setOutputPath(job, outputPath);
//                        } else {
//                            return false;
//                        }
//                        break;
//                }
//
//            } catch (IllegalArgumentException iae) {
//                return false;
//            }
//        } else {
        // Default HDFS.
//        LOG.info("No SINK specified, using DEFAULT (HDFS)");

        job.setInputFormatClass(DataGenInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        job.setMapperClass(DataGenMapper.class);
//        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(Text.class);

        String outputDir = line.getOptionValue("d", "datagen-default");
        String tempDir = null;

        // When a partition element is specified, we need to push the results to a temp dir and
        //   then move those result to the partition AFTER the MR job is complete.
        if (line.hasOption("part")) {
            tempDir = UUID.randomUUID().toString();
            job.getConfiguration().set("partition-temp-dir", tempDir);
        }

        if (tempDir != null)
            outputDir = outputDir + "/" + tempDir;

        outputPath = new Path(line.getOptionValue("d", "datagen-default"));
        FileOutputFormat.setOutputPath(job, outputPath);

        // One of these is set.
        if (line.hasOption("s")) {
            configuration.set(SCHEMA_FILE, line.getOptionValue("s"));
        }
        if (line.hasOption("dn")) {
            configuration.set(SCHEMA_FILE, DataGenMultiMapper.DEFAULT_MULTI_CONFIG_RESOURCE_FILE);
            LOG.info("Using DEFAULT Config File: " + DataGenMultiMapper.DEFAULT_MULTI_CONFIG_RESOURCE_FILE + " from package resources.");

        }
        if (line.hasOption("ds")) {
            configuration.set(SCHEMA_FILE, DataGenMapper.DEFAULT_CONFIG_RESOURCE_FILE);
            LOG.info("Using DEFAULT Config File: " + DataGenMapper.DEFAULT_CONFIG_RESOURCE_FILE + " from package resources.");

        }

        return rtn;
    }


    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf()); // new Job(conf, this.getClass().getCanonicalName());

        System.out.println("Checking Input");
        if (!checkUsage(args, job)) {
            printUsage();
            return -1;
        }

        setup(job);

        job.setJarByClass(DataGenTool.class);

        if (outputPath == null || outputPath.getFileSystem(job.getConfiguration()).exists(outputPath)) {
            throw new IOException("Output directory " + outputPath +
                    " already exists OR is missing from parameters list.");
        }

        int rtn_code = 0;

        try {
            rtn_code = job.waitForCompletion(true) ? 0 : 1;
            if (job.getConfiguration().getBoolean("partition-temp-dir", false)) {
                // Move the data elements from the main dir to partition dirs.
                // if schema has children.
                // TODO: Left off here...
//                if (schema.getPartitioned() || schema.get)
            }
        } catch (RuntimeException rte) {
            rte.fillInStackTrace();
            rte.printStackTrace();
        }
        return rtn_code;

    }


    public static void main(String[] args) throws Exception {
        int result;
        result = ToolRunner.run(new Configuration(), new DataGenTool(), args);
        System.exit(result);
    }
}
