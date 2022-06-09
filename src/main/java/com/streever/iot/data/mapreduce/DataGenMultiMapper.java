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
import com.streever.iot.data.utility.generator.fields.FieldProperties;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.CSVFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.streever.iot.data.mapreduce.DataGenTool.DEFAULT_MAPPERS;

public class DataGenMultiMapper extends Mapper<LongWritable, NullWritable, Text, Text> {
    static private Logger LOG = Logger.getLogger(DataGenMultiMapper.class.getName());

    //    public static final String SCHEMA_FILE = "schema.file";
    public static final String DEFAULT_MULTI_CONFIG_RESOURCE_FILE = "/validation/multi-default.yaml";
    public static final String PARTITION_PATH_PREFIX = "partition.path.prefix";

    private long counter = 0l;
    private long childrenCounter = 0l;

    protected Boolean earlyTermination = Boolean.FALSE;
    protected String partitionPathPrefix = null;

    // default format for now.
    protected CSVFormat format = new CSVFormat();

    protected Schema schema;

    protected void setup(Context context) {
        // Get the conf location from the job conf.
        String config = context.getConfiguration().get(DataGenTool.SCHEMA_FILE, DEFAULT_MULTI_CONFIG_RESOURCE_FILE);
        partitionPathPrefix = context.getConfiguration().get(PARTITION_PATH_PREFIX, "not-set");

        if (config.equals(DEFAULT_MULTI_CONFIG_RESOURCE_FILE)) {
            // Use the default validation file.
            try {
                InputStream configInputStream = getClass().getResourceAsStream(DEFAULT_MULTI_CONFIG_RESOURCE_FILE);
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
        schema.validate();
    }

    /*
Write a record, based on the schema, to the proper output path.
 */
    protected void write(Context context, String pathPrefix, Schema record) throws IOException, InterruptedException {
//        String strRec = format.write(record.getRecordMap());
        String strRec = format.write(record.getValueMap());
        LOG.debug("Writing record: " + record.getId() + ":" + strRec);
        // TODO: Need some partitioning element here to drive more than 1 reducer per relationship
        context.write(new Text(pathPrefix), new Text(strRec));
        writeRelationships(context, record.getRelationships());
    }

    /*
    Process the hierarchy of the schema.
*/
    protected void writeRelationships(Context context, Map<String, Relationship> relationships) throws IOException, InterruptedException {
        int mapperCount = context.getConfiguration().getInt(MRJobConfig.NUM_MAPS, DEFAULT_MAPPERS);
//        LOG.info("Mapper Count (in multi rel): " + mapperCount);
        for (Map.Entry<String, Relationship> entry : relationships.entrySet()) {
            Relationship relationship = entry.getValue();
            Schema schema = relationship.getRecord();
            int range = relationship.getCardinality().getRange(); //getMax() - relationship.getCardinality().getMin();
            double filepartBase = Math.log((double)range) * mapperCount;
            int filepartBaseInt = new Double(filepartBase).intValue();
            int filepartPrefix = schema.getKeyHash() % filepartBaseInt;
            String pathPrefix = schema.getTitle() + "/" + String.format("%1$5s", filepartPrefix).replace(' ', '0');
            LOG.debug(String.format("Base: %1f: BaseInt: %2d KeyHash: %3d Prefix: %4s", filepartBase, filepartBaseInt, schema.getKeyHash(), pathPrefix));
            if (range <= 0) {
                try {
                    childrenCounter++;
                    schema.next();
                    LOG.debug("(1)Writing relationship: " + schema.getId());
                    write(context, pathPrefix, schema);
                } catch (TerminateException e) {
                    // Early Termination;
                }
            } else {
                int rNum = ThreadLocalRandom.current().nextInt(range);
                for (int i = 0; i <= rNum; i++) {
                    try {
                        childrenCounter++;
                        schema.next();
                        LOG.debug("(n)Writing relationship: " + schema.getId());
                        write(context, pathPrefix, schema);
                    } catch (TerminateException e) {
                        break;
                    }
                }
            }
        }
    }


    public void map(LongWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
//        Text record = new Text();

        // Use this to quickly cycle through the remain counter,
        // even though we've reach to end because of the termination
        // event in the generator.
        if (!earlyTermination) {
            // TODO: Fix
            try {
                counter++;
                schema.next();
                int filepartPrefix = schema.getKeyHash() % context.getConfiguration().getInt(MRJobConfig.NUM_MAPS, DEFAULT_MAPPERS);
                String pathPrefix = schema.getTitle() + "/" + String.format("%1$5s", filepartPrefix).replace(' ', '0');
                write(context, pathPrefix, schema);
                if (counter % 1000 == 0) {
                    LOG.info(String.format("Master count: %1d Child count: %2d", counter, childrenCounter));
                }
            } catch (TerminateException te) {
//                System.out.println("Terminate Exception Raised after " + (runStatus[0]) + " records");
            } catch (IOException ioe) {
                System.err.println("Issue Writing to file");
                ioe.printStackTrace();
            } finally {

            }
        }
    }

}
