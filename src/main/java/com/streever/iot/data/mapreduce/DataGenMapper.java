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

//import com.streever.iot.data.utility.generator.RecordGenerator;

import com.streever.iot.data.utility.generator.Relationship;
import com.streever.iot.data.utility.generator.Schema;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.CSVFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class DataGenMapper extends Mapper<LongWritable, NullWritable, NullWritable, Text> {
    static private Logger LOG = Logger.getLogger(DataGenMapper.class.getName());

    public static final String DEFAULT_CONFIG_RESOURCE_FILE = "/validation/simple-default.yaml";

    protected Boolean earlyTermination = Boolean.FALSE;

    protected Text textRecord = new Text();
    protected long localCount = 0l;
    protected long localSize = 0l;

    // default format for now.
    protected CSVFormat format = new CSVFormat();

    protected Schema schema;

    protected void setup(Context context) {
        // Get the conf location from the job conf.
        String config = context.getConfiguration().get(DataGenTool.SCHEMA_FILE, DEFAULT_CONFIG_RESOURCE_FILE);

        if (config.equals(DEFAULT_CONFIG_RESOURCE_FILE)) {
            // Use the default validation file.
            try {
                InputStream configInputStream = getClass().getResourceAsStream(DEFAULT_CONFIG_RESOURCE_FILE);
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
                FS1 = FileSystem.get(context.getConfiguration());

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
        schema.validate(context.getConfiguration().get(DataGenTool.DATAGEN_PARTITION, null));
    }

    /*
Write a record, based on the schema, to the proper output path.
 */
    protected long write(Context context, Schema record) throws IOException, InterruptedException {
//        Map<Schema, String> pathMap = record.getPathMap();
        String strRec = format.write(record.getValueMap());
//        if (pathMap != null) {
//            String schemaPath = pathMap.get(record);
//            if (record.getPartitioned()) {
//                schemaPath = schemaPath + "/" + partitionPathPrefix;
//            }
//            context.write(new Text(schemaPath), new Text(strRec));
//        } else {
            context.write(NullWritable.get(), new Text(strRec));
//        }
        // The last generated recordset
        return strRec.length();
    }

    public void map(LongWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
//        Text record = new Text();

        // Use this to quickly cycle through the remain counter,
        // even though we've reach to end because of the termination
        // event in the generator.
        if (!earlyTermination) {
            // TODO: Fix
            try {
                schema.next();
                // Need an Output spec (CSV, Json, etc, maybe ORC, Parquet, Seq)
//                Object k = recordGenerator.getKey();
//                Object v = schema.getValueMap();
//                record.set(v.toString());
//                context.write(NullWritable.get(), record);


                // 0 for counts
                // 1 for size
//                long runStatus[] = new long[2];
//                long localCount = count;
//                long localSize = size;
//                try {
//                    openOutput();
//                    long loop = 0;
                // Run the loop until the counts/sizes are met.  If the count/size has been set to -1, then run indefinitely.
//                    do {
//                        schema.next();
                long lsize = write(context, schema);

//                        runStatus[1] += lsize;
//                        if (localSize != -1)
//                            localSize -= lsize;
//                        runStatus[0]++;
                long[] rStatus = writeRelationships(context, schema.getRelationships());
//                        runStatus[0] += rStatus[0];
//                        runStatus[1] += rStatus[1];
//                        if (localCount != -1)
//                            localCount--;
//                        if (localSize != -1)
//                            localSize -= rStatus[1];
//                        if (loop % progressIndicatorCount == 0) {
//                            System.out.printf("[ %d, %d, %d ]%n", loop, runStatus[0], runStatus[1]);
//                        }
//                        loop++;
//                    } while ((localCount > 0 || localCount == -1) && (localSize > 0 || localSize == -1));
            } catch (TerminateException te) {
//                System.out.println("Terminate Exception Raised after " + (runStatus[0]) + " records");
            } catch (IOException ioe) {
                System.err.println("Issue Writing to file");
                ioe.printStackTrace();
            } finally {
//                    try {
//                        closeOutput();
//                        System.out.printf("[ %d, %d ]%n", runStatus[0], runStatus[1]);
//                    } catch (IOException e) {
//                        // TODO: Handle ioexception
//                        e.printStackTrace();
//                    }
            }
//                return runStatus;


//            } catch (TerminateException te) {
//                earlyTermination = Boolean.TRUE;
//            }
        }
    }

    /*
Process the hierarchy of the schema.
 */
    protected long[] writeRelationships(Context context, Map<String, Relationship> relationships) throws IOException, InterruptedException {
        // status[0] for counts
        // status[1] for size
        long[] status = {0, 0};
        if (relationships != null) {
            Set<String> relationshipKeys = relationships.keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = relationships.get(key);
                Schema rRecord = relationship.getRecord();
                try {
                    int range = relationship.getCardinality().getMax() - relationship.getCardinality().getMin();
                    if (range <= 0) {
                        // Assume 1-1 relationship.
                        rRecord.next();
                        write(context, rRecord);
                        status[0]++;
                        // Recurse into hierarchy
                        long[] rStatus;
                        rStatus = writeRelationships(context, rRecord.getRelationships());
                        status[0] += rStatus[0];
                        status[1] += rStatus[1];
                    } else {
                        // Assume min to max relationship.
                        int rNum = ThreadLocalRandom.current().nextInt(relationship.getCardinality().getMin(), relationship.getCardinality().getMax() + 1);
                        for (int i = relationship.getCardinality().getMin(); i < rNum; i++) {
                            rRecord.next();
                            status[1] += write(context, rRecord);
                            status[0]++;
                            // Recurse into hierarchy
                            long[] rStatus;
                            rStatus = writeRelationships(context, rRecord.getRelationships());
                            status[0] += rStatus[0];
                            status[1] += rStatus[1];
                        }
                    }
                } catch (TerminateException | IOException te) {

                }
            }
        }
        return status;
    }

    /*
            if (!initialized) {
            throw new RuntimeException("Builder was not initialized. Call init() before run().");
        }
        // 0 for counts
        // 1 for size
        long runStatus[] = new long[2];
        long localCount = count;
        long localSize = size;
        try {
            openOutput();
            long loop = 0;
            // Run the loop until the counts/sizes are met.  If the count/size has been set to -1, then run indefinitely.
            do {
                getSchema().next();
                long lsize = write(getSchema());
                runStatus[1] += lsize;
                if (localSize != -1)
                    localSize -= lsize;
                runStatus[0]++;
                long[] rStatus = writeRelationships(getSchema().getRelationships());
                runStatus[0] += rStatus[0];
                runStatus[1] += rStatus[1];
                if (localCount != -1)
                    localCount--;
                if (localSize != -1)
                    localSize -= rStatus[1];
                if (loop % progressIndicatorCount == 0) {
                    System.out.printf("[ %d, %d, %d ]%n", loop, runStatus[0], runStatus[1]);
                }
                loop++;
            } while ((localCount > 0 || localCount == -1) && (localSize > 0 || localSize == -1));
        } catch (TerminateException te) {
            System.out.println("Terminate Exception Raised after " + (runStatus[0]) + " records");
        } catch (IOException ioe) {
            System.err.println("Issue Writing to file");
            ioe.printStackTrace();
        } finally {
            try {
                closeOutput();
                System.out.printf("[ %d, %d ]%n", runStatus[0], runStatus[1]);
            } catch (IOException e) {
                // TODO: Handle ioexception
                e.printStackTrace();
            }
        }
        return runStatus;

     */

}
