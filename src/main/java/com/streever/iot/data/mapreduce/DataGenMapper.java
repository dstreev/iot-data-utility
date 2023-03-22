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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streever.iot.data.utility.generator.Domain;
import com.streever.iot.data.utility.generator.DomainBuilder;
import com.streever.iot.data.utility.generator.Relationship;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.Format;
import com.streever.iot.data.utility.generator.output.FormatFactory;
import com.streever.iot.data.utility.generator.output.JSONFormat;
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

import static com.streever.iot.data.mapreduce.DataGenTool.FORMAT_TYPE;

public class DataGenMapper extends Mapper<LongWritable, NullWritable, NullWritable, Text> {
    static private Logger LOG = Logger.getLogger(DataGenMapper.class.getName());

    public static final String DEFAULT_CONFIG_RESOURCE_FILE = "/validation/simple-default.yaml";

    protected Boolean earlyTermination = Boolean.FALSE;

    // default format for now.
    protected Format format = new JSONFormat();

    protected DomainBuilder domainBuilder;

    protected void setup(Context context) {
        // Get the conf location from the job conf.
        String config = context.getConfiguration().get(DataGenTool.DOMAIN_FILE, DEFAULT_CONFIG_RESOURCE_FILE);
        Domain domain = null;
        // Get Anchor.schema from config.
        String anchorSchema = context.getConfiguration().get(DataGenTool.ANCHOR_SCHEMA);
        format = FormatFactory.getFormatImplementation(context.getConfiguration().get(FORMAT_TYPE, "JSON"));
        // TODO: Get Tokens from cli.
        Map<String, Object> tokens = null;

        if (config.equals(DEFAULT_CONFIG_RESOURCE_FILE)) {
            // Use the default validation file.
            try {
                InputStream configInputStream = getClass().getResourceAsStream(DEFAULT_CONFIG_RESOURCE_FILE);
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
                // Read the Config from the path.
                FileSystem FS1 = null;
                FSDataInputStream dfsConfigInputStream = null;
                try {
                    FS1 = FileSystem.get(context.getConfiguration());

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
        domainBuilder = new DomainBuilder(domain, anchorSchema);

    }

    /*
    Write a record, based on the schema, to the proper output path.
    */
    protected long write(Context context, ObjectNode node) throws IOException, InterruptedException {
        String strRec = format.format(node);
//        System.out.println(strRec);
        context.write(NullWritable.get(), new Text(strRec));
        return strRec.length();
    }

    public void map(LongWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        // Use this to quickly cycle through the remain counter,
        // even though we've reach to end because of the termination
        // event in the generator.
        if (!earlyTermination) {
            try {
                ObjectNode node = domainBuilder.getRecord();
                long lsize = write(context, node);
            } catch (TerminateException te) {
                earlyTermination = Boolean.TRUE;
            } catch (IOException ioe) {
                System.err.println("Issue Writing to file");
                ioe.printStackTrace();
            }
        }
    }

}
