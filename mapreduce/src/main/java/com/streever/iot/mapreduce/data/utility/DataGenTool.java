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

package com.streever.iot.mapreduce.data.utility;

//import spec.bridge.hadoop2.KafkaOutputFormat;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DataGenTool extends Configured implements Tool {

    static private Logger LOG = Logger.getLogger(DataGenTool.class.getName());

    private Options options;
    private Path outputPath;
    //        private Sink sink;
    public static final int DEFAULT_MAPPERS = 2;
    public static final long DEFAULT_COUNT = 100;

    public enum Sink {HDFS, KAFKA};

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
        outputDir.setRequired(true);
//        Option outputDir = Option.builder("d")
//                .argName("directory")
//                .desc("Output Directory")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .type(String.class)
//                .required(true)
//                .build();

        Option mappers = new Option("m", "mappers", true, "Parallelism");
        mappers.setRequired(true);
//        Option mappers = Option.builder("m")
//                .argName("mappers")
//                .desc("Parallelism")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .type(Integer.class)
//                .required(true)
//                .build();


        Option config = new Option("cfg", "config", true, "Config File in HDFS");
        config.setRequired(true);
//        Option config = Option.builder("cfg")
//                .argName("config")
//                .desc("Configuration Filename (in HDFS)")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .type(String.class)
//                .required(true)
//                .build();

        Option count = new Option("c", "count", true, "Record Count");
        count.setRequired(true);
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
        options.addOption(config);
        options.addOption(count);
    }

    private void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop jar <jar-file> " + this.getClass().getCanonicalName(), options);
    }

    private boolean checkUsage(String[] args, Job job) {
        boolean rtn = true;
        Configuration configuration = job.getConfiguration();

        CommandLineParser clParser = new DefaultParser();

        CommandLine line = null;
        try {
            line = clParser.parse(options, args);
        } catch (ParseException pe) {
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

        if (line.hasOption("m")) {
            configuration.set(MRJobConfig.NUM_MAPS, line.getOptionValue("m"));
        } else {
            // Default
            configuration.setInt(MRJobConfig.NUM_MAPS, DEFAULT_MAPPERS);
        }

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
        job.setMapperClass(DataGenMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        outputPath = new Path(line.getOptionValue("d"));
        FileOutputFormat.setOutputPath(job, outputPath);

        configuration.set(DataGenMapper.CONFIG_FILE, line.getOptionValue("cfg"));

        return rtn;
    }


    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf()); // new Job(conf, this.getClass().getCanonicalName());

        System.out.println("Checking Input");
        if (!checkUsage(args, job)) {
            printUsage();
            return -1;
        }

        job.setJarByClass(DataGenTool.class);

        if (outputPath == null || outputPath.getFileSystem(job.getConfiguration()).exists(outputPath)) {
            throw new IOException("Output directory " + outputPath +
                    " already exists OR is missing from parameters list.");
        }

        // Map Only Job
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;

    }


    public static void main(String[] args) throws Exception {
        int result;
        result = ToolRunner.run(new Configuration(), new DataGenTool(), args);
        System.exit(result);
    }
}
