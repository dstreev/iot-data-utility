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

import com.streever.iot.data.cli.DomainGenerator;
import com.streever.iot.data.utility.generator.Domain;
import com.streever.iot.data.utility.generator.DomainBuilder;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.streever.iot.data.cli.DomainGenerator.DEFAULT_VALIDATION_YAML;

//import static com.streever.iot.data.mapreduce.DataGenMapper.SCHEMA_FILE;

public class DataGenTool extends Configured implements Tool {

    static private Logger LOG = Logger.getLogger(DataGenTool.class.getName());
    public static final String DOMAIN_FILE = "datagen.domain.file";
    public static final String ANCHOR_SCHEMA = "datagen.anchor.schema";
    public static final String FORMAT_TYPE = "datagen.output.format.type";
    private Path outputPath;
//    private String partitionPath;
    private Boolean forceFlat = Boolean.FALSE;

    private DomainBuilder domainBuilder;

    public static final int DEFAULT_MAPPERS = 2;
    public static final long DEFAULT_COUNT = 100;

    public enum Sink {HDFS, KAFKA};

    private Options getOptions() {
        Options options = DomainGenerator.getOptions();

        Option MAPPER_NUM_OPTION = new Option("m", "mapper-count", true, "Number of Mappers");
        MAPPER_NUM_OPTION.setRequired(false);
        options.addOption(MAPPER_NUM_OPTION);

        return options;
    }

    private void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop jar <jar-file> " + this.getClass().getCanonicalName(), getOptions());
    }

    protected void setup(Job job) {
        // Get the conf location from the job conf.
        // Get the conf location from the job conf.
        String config = job.getConfiguration().get(DataGenTool.DOMAIN_FILE, DEFAULT_VALIDATION_YAML);

        // Get Anchor.schema from config.
        String anchorSchema = job.getConfiguration().get(DataGenTool.ANCHOR_SCHEMA);

        // TODO: Get Tokens from cli.
        Map<String, Object> tokens = null;

        Domain domain = null;
        if (config.equals(DEFAULT_VALIDATION_YAML)) {
            // Use the default validation file.
            try {
                InputStream configInputStream = getClass().getResourceAsStream(DEFAULT_VALIDATION_YAML);
                domain = Domain.deserializeInputStream(tokens, configInputStream);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } else {
            LOG.info("Config File: " + config);

            try {
                InputStream configInputStream = getClass().getResourceAsStream(config);
                domain = Domain.deserializeInputStream(tokens, configInputStream);
            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException(e);
                // Didn't find as resource, try FS.
            }
            // Read the Config from the path.
            if (domain == null) {
                FileSystem FS1 = null;
                FSDataInputStream dfsConfigInputStream = null;
                try {
                    FS1 = FileSystem.get(job.getConfiguration());

                    Path path = new Path(config);

                    dfsConfigInputStream = FS1.open(path);

                    domain = Domain.deserializeInputStream(tokens, dfsConfigInputStream);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } finally {
                    IOUtils.closeStream(dfsConfigInputStream);
                }
            }
        }

        domain.completeAssociations();
        List<String> reasons = new ArrayList<String>();
        if (!domain.validate(reasons)) {
            System.out.println(reasons.stream().map(r -> r.toString()).collect(Collectors.joining("\n")));
            throw new RuntimeException("Config didn't pass validation");
        }

        domainBuilder = new DomainBuilder(domain, anchorSchema);
        domainBuilder.init();


    }

    private boolean init(String[] args, Job job) {
        boolean rtn = true;
        Configuration configuration = job.getConfiguration();

        CommandLineParser clParser = new PosixParser();

        CommandLine line = null;
        Options options = getOptions();
        try {
            line = clParser.parse(options, args);
        } catch (ParseException pe) {
            pe.printStackTrace();
            return false;
        }

        if (line.hasOption("h")) {
            return false;
        }

        if (line.hasOption("csv")) {
            configuration.set(FORMAT_TYPE, "csv");
        }

        if (line.hasOption("json")) {
            configuration.set(FORMAT_TYPE, "json");
        }
        if (line.hasOption("s")) {
            configuration.set(ANCHOR_SCHEMA, line.getOptionValue("s"));
        }
        if (line.hasOption("c")) {
            DataGenInputFormat.setNumberOfRows(job, Long.parseLong(line.getOptionValue("c")));
        } else {
            DataGenInputFormat.setNumberOfRows(job, DEFAULT_COUNT);
        }

        if (line.hasOption("m")) {
            configuration.setInt(MRJobConfig.NUM_MAPS, Integer.parseInt(line.getOptionValue("m")));
        } else {
            // Default
            configuration.setInt(MRJobConfig.NUM_MAPS, DEFAULT_MAPPERS);
        }

        // Set Datagen Mapper Class
        job.setMapperClass(DataGenMapper.class);
        job.setInputFormatClass(DataGenInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Map Only Job
        job.setNumReduceTasks(0);

        outputPath = new Path(line.getOptionValue("o", "datagen-default"));
        FileOutputFormat.setOutputPath(job, outputPath);

        // One of these is set.
        if (line.hasOption("d")) {
            job.getConfiguration().set(DOMAIN_FILE, line.getOptionValue("d"));
        }
        if (line.hasOption("dd")) {
            job.getConfiguration().set(DOMAIN_FILE, DataGenMapper.DEFAULT_CONFIG_RESOURCE_FILE);
            LOG.info("Using DEFAULT Config File: " + DataGenMapper.DEFAULT_CONFIG_RESOURCE_FILE + " from package resources.");
        }

        return rtn;
    }


    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf()); // new Job(conf, this.getClass().getCanonicalName());

        System.out.println("Checking Input");
        if (!init(args, job)) {
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
