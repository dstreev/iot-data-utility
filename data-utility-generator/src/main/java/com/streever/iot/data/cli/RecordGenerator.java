package com.streever.iot.data.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.kafka.producer.KafkaProducerConfig;
import com.streever.iot.kafka.producer.ProducerCreator;
import com.streever.iot.kafka.spec.ProducerSpec;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class RecordGenerator {
    public enum FILE_TYPE {
        JSON, YAML;
    }

    private Options options;
    private Long count = null;
    private String outputFilename = null;
    private String configurationFile = null;
    private String streamConfigurationFile = null;
    private Boolean tsOnFile;
    Integer transactionCommitCount = 5000;
    Integer progressIndicatorCount = 5000;
    private Integer burstCount = null;
    private Integer pauseMax = null;
    private boolean randomBurst = false;
    private boolean randomPause = false;
    private Random random = new Random(new Date().getTime());

    private com.streever.iot.data.utility.generator.RecordGenerator recordGenerator = null;
    private ProducerSpec streamingSpec = null;

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
                .required(false)
                .build();

        Option dOutput = Option.builder("d")
                .argName("directory")
                .desc("Output Directory")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(false)
                .build();

        Option oConfig = Option.builder("cfg")
                .argName("config")
                .desc("Configuration Filename")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(true)
                .build();

        Option sConfig = Option.builder("scfg")
                .argName("streamConfig")
                .desc("Streaming Configuration")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .required(false)
                .build();

        Option oCount = Option.builder("c")
                .argName("count")
                .desc("Record Count")
                .hasArg(true)
                .numberOfArgs(1)
                .type(Long.class)
                .required(false)
                .build();

        Option oTimestamp = Option.builder("t")
                .argName("timestamp")
                .desc("Add Timestamp to Filename")
                .hasArg(false)
                .required(false)
                .build();

        Option oBurstMax = Option.builder("bm")
                .argName("burstMax")
                .desc("Burst Max")
                .type(Integer.class)
                .hasArg(true)
                .required(false)
                .build();

        Option oPause = Option.builder("p")
                .argName("pauseMax")
                .desc("pause max millis")
                .type(Integer.class)
                .hasArg(true)
                .required(false)
                .build();

        Option oRandomBurst = Option.builder("rb")
                .argName("randomizeBurst")
                .desc("randomize burst")
                .type(Boolean.class)
                .hasArg(false)
                .required(false)
                .build();

        Option oRandomPause = Option.builder("rp")
                .argName("randomizePause")
                .desc("randomize pause")
                .type(Boolean.class)
                .hasArg(false)
                .required(false)
                .build();

        options.addOption(oHelp);
        options.addOption(oOutput);
        options.addOption(dOutput);
        options.addOption(oConfig);
        options.addOption(sConfig);
        options.addOption(oCount);
        options.addOption(oTimestamp);
        options.addOption(oBurstMax);
        options.addOption(oPause);
        options.addOption(oRandomBurst);
        options.addOption(oRandomPause);

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

        if (line.hasOption("c")) {
            count = Long.parseLong(line.getOptionValue("c"));
        }

        Map<String, String> outputMap = new TreeMap<String, String>();

        if (line.hasOption("o")) {
            outputFilename = line.getOptionValue("o");
        }
        if (line.hasOption("scfg")) {
            streamConfigurationFile = line.getOptionValue("scfg");
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
            String ext = FilenameUtils.getExtension(configurationFile);

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
            File file = new File(configurationFile);
            String generatorCfg = FileUtils.readFileToString(file, Charset.forName("UTF-8"));

            recordGenerator = mapper.readerFor(com.streever.iot.data.utility.generator.RecordGenerator.class).readValue(generatorCfg);

            if (streamConfigurationFile != null) {
                File streamCfgFile = new File(streamConfigurationFile);
                String streamCfg = FileUtils.readFileToString(streamCfgFile, Charset.forName("UTF-8"));

                streamingSpec = mapper.readerFor(ProducerSpec.class).readValue(streamCfg);
            }

            Map<String, String> output = new HashMap<String, String>();
            // Determine Output modes.
            if (streamingSpec != null) {
                output.put("Stream", streamingSpec.getTopic().getName());
            }
            if (outputFilename != null) {
                output.put("File", outputFilename);
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
        if (outputFilename != null)
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

        BufferedWriter writer = null;

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
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HHmmss");

                String extension = FilenameUtils.getExtension(outputFilename);
                String baseName = FilenameUtils.getBaseName(outputFilename);
                String fullPath = FilenameUtils.getFullPath(outputFilename);
                String now = df.format(new Date());

                outputFilename = fullPath + File.separator + baseName + "_" + now + "." + extension;
            }
            
            if (toFile)
                writer = new BufferedWriter(new FileWriter(outputFilename));

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

                Object key = recordGenerator.getKey();
                Object value = recordGenerator.getValue();

                if (toFile) {
                    writer.append(value.toString());
                    writer.append(recordGenerator.getOutput().getNewLine());
                }

                if (toStream && transactional && progressCount % transactionCommitCount == 0) {
                    producer.commitTransaction();
                    producer.beginTransaction();
                }
                if (toStream) {
                    final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key.toString(), value.toString());

                    RecordMetadata metadata = producer.send(record).get();
                    offsets.put(metadata.partition(), metadata.offset());
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
        } catch (ExecutionException e) {
            e.printStackTrace();
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
