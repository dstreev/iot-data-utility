package com.streever.iot.data.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.Child;
import com.streever.iot.data.utility.generator.Record;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.Output;
import com.streever.iot.kafka.producer.KafkaProducerConfig;
import com.streever.iot.kafka.producer.ProducerCreator;
import com.streever.iot.kafka.spec.ProducerSpec;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.producer.Producer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class RecordGenerator {
    public enum FILE_TYPE {
        JSON, YAML;
    }

    private Options options;
    private Long count = null;
    private String outputDirectory = null;
    private String generatorConfigurationFile = null;
    private String outputConfigurationFile = null;
    private Boolean genHiveTable = Boolean.FALSE;
    private Boolean tsOnFile;
    Integer transactionCommitCount = 5000;
    Integer progressIndicatorCount = 5000;
    private Integer streamingDuration = null;
    private Integer burstCount = null;
    private Integer pauseMax = null;
    private boolean randomBurst = false;
    private boolean randomPause = false;
    private Random random = new Random(new Date().getTime());

    private com.streever.iot.data.utility.generator.RecordGenerator recordGenerator = null;
    private Output outputSpec = null;

    private ProducerSpec streamingSpec = null;

    private void buildOptions() {
        options = new Options();

        Option HELP_OPTION = Option.builder("h")
                .argName("help")
                .desc("This Help")
                .longOpt("help")
                .hasArg(false)
                .required(false)
                .build();

        Option OUTPUT_OPTION = Option.builder("d")
                .argName("OUTPUT_DIR")
                .desc("Output Directory")
                .longOpt("output-directory")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(false)
                .build();

//        Option dOutput = Option.builder("d")
//                .argName("directory")
//                .desc("Output Directory")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .type(String.class)
//                .required(false)
//                .build();

        Option GENERATOR_CONFIG_OPTION = Option.builder("gc")
                .argName("GENERATOR_CONFIG_FILE")
                .desc("Generator Configuration Filename")
                .longOpt("generator-config")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(true)
                .build();

        Option COUNT_OPTION = Option.builder("c")
                .argName("COUNT")
                .desc("Record Count")
                .longOpt("count")
                .hasArg(true)
                .numberOfArgs(1)
                .type(Long.class)
                .required(false)
                .build();

        Option TIMESTAMP_OPTION = Option.builder("t")
                .argName("TIMESTAMP")
                .desc("Add Timestamp to Filename (requires '-d' option)")
                .longOpt("timestamp")
                .hasArg(false)
                .required(false)
                .build();

        Option OUTPUT_CONFIG_OPTION = Option.builder("oc")
                .argName("OUTPUT_CONFIG_FILE")
                .longOpt("output-config")
                .desc("Output Configuration")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(false)
                .build();

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

        Option GEN_HIVE_SCHEMA_OPTION = Option.builder("hive")
                .argName("HIVE_TABLE")
                .desc("Generate Hive Table")
                .longOpt("gen-hive-schema")
                .required(false)
                .build();

        OptionGroup outputGroup = new OptionGroup();
        outputGroup.setRequired(true);

        outputGroup.addOption(HELP_OPTION);
        outputGroup.addOption(OUTPUT_OPTION);
        outputGroup.addOption(OUTPUT_CONFIG_OPTION);
        outputGroup.addOption(GEN_HIVE_SCHEMA_OPTION);

        options.addOptionGroup(outputGroup);

        options.addOption(GENERATOR_CONFIG_OPTION);

        options.addOption(TIMESTAMP_OPTION);

        options.addOption(COUNT_OPTION);

        options.addOption(BURST_MAX_OPTION);
        options.addOption(STREAMING_DURATION_OPTION);
        options.addOption(PAUSE_OPTION);
        options.addOption(RANDOMIZE_BURST_OPTION);
        options.addOption(RANDOMIZE_PAUSE_OPTION);


    }

    private void printUsage(String statement) {
        HelpFormatter formatter = new HelpFormatter();
        String footer = "TBD";
        formatter.printHelp("java " + this.getClass().getCanonicalName(), statement, options, footer, true);
    }

    private boolean checkUsage(String[] args) {
        boolean rtn = true;
        buildOptions();

        CommandLineParser clParser = new DefaultParser();
        CommandLine line = null;

        try {
            line = clParser.parse(options, args, true);
        } catch (ParseException pe) {
            printUsage("Missing Required Elements");
            return false;
        }

        if (line.hasOption("h")) {
            printUsage("HELP");
            return false;
        }

        if (line.hasOption("c")) {
            if (line.hasOption("cfg") || line.hasOption("scfg")) {
                count = Long.parseLong(line.getOptionValue("c"));
            } else {
                printUsage("Count option only valid with '-cfg'");
                return false;
            }
        }

        Map<String, String> outputMap = new TreeMap<String, String>();

        if (line.hasOption("d")) {
            if (!line.hasOption("c") || !line.hasOption("cfg")) {
                printUsage("-d option requires the config (-cfg) and count (-c) options.");
                return false;
            }
            outputDirectory = line.getOptionValue("o");
        }

        if (line.hasOption("scfg")) {
            if (!line.hasOption("c") && !line.hasOption("sd")) {
                System.out.println("Streaming configuration will run until manually terminated.");
            }
            if (line.hasOption("sd")) {
                streamingDuration = Integer.valueOf(line.getOptionValue("sd"));
            }
            outputConfigurationFile = line.getOptionValue("scfg");
        }

        if (line.hasOption("bm")) {
            String t = line.getOptionValue("bm");
            burstCount = Integer.parseInt(t);
        }

        if (line.hasOption("p")) {
            String p = line.getOptionValue("p");
            pauseMax = Integer.parseInt(p);
        }

        if (line.hasOption("rb")) {
            randomBurst = true;
        }
        if (line.hasOption("rp")) {
            randomPause = true;
        }

        generatorConfigurationFile = line.getOptionValue("cfg");

        if (line.hasOption("t")) {
            tsOnFile = true;
        } else {
            tsOnFile = false;
        }

        if (line.hasOption("hive")) {
            genHiveTable = Boolean.TRUE;
        }

        return rtn;
    }


    public int run(String[] args) throws Exception {
        int rtn = 0;

        if (!checkUsage(args)) {
//            System.out.println("Check Usage:");
//            printUsage();
            rtn = -1;
        } else {
            String ext = FilenameUtils.getExtension(generatorConfigurationFile);

            ObjectMapper mapper = null;
            if (ext.toUpperCase().equals(FILE_TYPE.JSON.toString())) {
                mapper = new ObjectMapper();
            } else if (ext.toUpperCase().equals(FILE_TYPE.YAML.toString())) {
                mapper = new ObjectMapper(new YAMLFactory());
            } else {
                throw new RuntimeException("Unknown file extension: " + ext + ".  json or yaml supported.");
            }

            mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            // Build Record Generator
            File gfile = new File(generatorConfigurationFile);
            String generatorCfg = FileUtils.readFileToString(gfile, Charset.forName("UTF-8"));

            recordGenerator = mapper.readerFor(com.streever.iot.data.utility.generator.RecordGenerator.class).readValue(generatorCfg);


            if (genHiveTable) {
                System.out.print(recordGenerator.hiveTableLayout());
                return 0;
            }

            if (outputConfigurationFile != null) {
                File ofile = new File(outputConfigurationFile);
                String outputCfg = FileUtils.readFileToString(ofile, Charset.forName("UTF-8"));

                outputSpec = mapper.readerFor(Output.class).readValue(outputCfg);

                if (outputDirectory != null) {
                    outputSpec.setBaseDirectory(outputDirectory);
                }
            }

            Map<String, String> output = new HashMap<String, String>();
            // Determine Output modes.
            if (streamingSpec != null) {
                output.put("Stream", streamingSpec.getTopic().getName());
            }
            if (outputDirectory != null) {
                output.put("File", outputDirectory);
            }
            if (output.size() > 0) {
                System.out.println(output.toString());
            } else {
                System.out.println("No Output Specified");
            }
            build();

        }

        return rtn;
    }

    private void build() {
        boolean toFile = false;
        boolean toStream = false;
        boolean transactional = false;
        if (outputDirectory != null)
            toFile = true;
        if (streamingSpec != null) {
            toStream = true;
            if (streamingSpec.getConfigs().get(KafkaProducerConfig.TRANSACTIONAL_ID.getConfig()) != null &&
                    streamingSpec.getConfigs().get(KafkaProducerConfig.ACKS.getConfig()) != null &&
                    streamingSpec.getConfigs().get(KafkaProducerConfig.ACKS.getConfig()).toString().equals("all")) {
                transactional = true;
            } else {
                transactional = false;
            }
        }

        Producer<String, String> producer = null;
        String topic = null;

        if (toStream) {
            producer = (Producer<String, String>) ProducerCreator.createProducer(streamingSpec);
            topic = streamingSpec.getTopic().getName();
            if (transactional) {
                producer.initTransactions();
                producer.beginTransaction();
            }
        }

        Date start = new Date();
        long progressCount = 0;

        Map<Integer, Long> offsets = new TreeMap<Integer, Long>();



        try {
            if (toFile && tsOnFile) {
                // Go through the record and it's children to build a list of fileOutputs.
                Record parentRecord = recordGenerator.getRecord();

                for (Child child: parentRecord.getChildren()) {

                }


                DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HHmmss");

                String extension = FilenameUtils.getExtension(outputDirectory);
                String baseName = FilenameUtils.getBaseName(outputDirectory);
                String fullPath = FilenameUtils.getFullPath(outputDirectory);
                String now = df.format(new Date());

                outputDirectory = fullPath + File.separator + baseName + "_" + now + "." + extension;
            }
            
            if (toFile)
                writer = new BufferedWriter(new FileWriter(outputDirectory));

            long burstStageCount = 0l;
            long lclBurstCount = 0;
            if (burstCount != null)
                lclBurstCount = burstCount;

            if (randomBurst && burstCount != null) {
                lclBurstCount = random.nextInt(burstCount);
            }
//            long lclCount = 0;
            boolean go = true;
            Date tempStart = new Date();
            while (go) {
//            for (long i = 1; i < count + 1; i++) {
                recordGenerator.next();
                progressCount++;
//                lclCount++;

                if (count != null && progressCount >= count) {
                    go = false;
                }

                if (burstCount != null)
                    burstStageCount++;

//                Object key = recordGenerator.getKey();
//                Object value = recordGenerator.getValue();

                if (toFile) {
//                    writer.append(value.toString());
//                    writer.append(recordGenerator.getOutput().getNewLine());
                }

                if (toStream && transactional && progressCount % transactionCommitCount == 0) {
                    producer.commitTransaction();
                    producer.beginTransaction();
                }
                if (toStream) {
//                    final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key.toString(), value.toString());

//                    RecordMetadata metadata = producer.send(record).get();
//                    offsets.put(metadata.partition(), metadata.offset());
                }

                if (progressCount % progressIndicatorCount == 0) {
                    System.out.print(".");
                }
                if (progressCount % (progressIndicatorCount * 100) == 0) {
                    System.out.println(".");
                    Date tempEnd = new Date();
                    long tempDiff = tempEnd.getTime() - tempStart.getTime();
                    double tempPerSecRate = ((double) (progressIndicatorCount * 100) / tempDiff) * 1000;
                    System.out.println("Interval Time: " + tempDiff + " Loops: " + (progressIndicatorCount * 100));
                    System.out.println("Interval Rate (perSec): " + tempPerSecRate);
                    tempStart = new Date();
                }

                if (count != null && progressCount >= count) {
                    go = false;
                }
                
                if (pauseMax != null && burstCount != null && burstStageCount > lclBurstCount) {
                    int realPause = pauseMax;
                    if (randomPause) {
                        realPause = random.nextInt(pauseMax);
                    }
                    System.out.println("Pause Activated after generating " + burstStageCount + " records.  Sleeping for " + realPause + " ms.");
                    burstStageCount = 0;
                    Thread.sleep(realPause);
                    if (randomBurst) {
                        lclBurstCount = random.nextInt(burstCount);
                    }

                }
//            }
            }


        } catch (InterruptedException e) {
            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
        } catch (TerminateException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (toFile && writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (toStream && transactional) {
                producer.commitTransaction();
            }
            if (toStream) {
                producer.close();
            }
            Date end = new Date();
            long diff = end.getTime() - start.getTime();
            double perSecRate = ((double) progressCount / diff) * 1000;
            if (toStream) {
                for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
                    System.out.println();
                    System.out.println("Partition: " + entry.getKey() + " Offset: " + entry.getValue());
                }
            }
            System.out.println("Time: " + diff + " Loops: " + progressCount);
            System.out.println("Rate (perSec): " + perSecRate);

        }
    }

    public static void main(String[] args) throws Exception {
        int result;
        RecordGenerator cli = new RecordGenerator();
        result = cli.run(args);
        System.exit(result);
    }

}
