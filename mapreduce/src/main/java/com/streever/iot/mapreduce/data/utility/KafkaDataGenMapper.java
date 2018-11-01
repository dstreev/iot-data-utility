/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streever.iot.mapreduce.data.utility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.streever.iot.data.utility.generator.RecordGenerator;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KafkaDataGenMapper extends DataGenMapper<LongWritable, NullWritable, Object, Object> {


    public void map(LongWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        Text record = new Text();
        try {
            // Use this to quickly cycle through the remain counter,
            // even though we've reach to end because of the termination
            // event in the generator.
            if (!earlyTermination) {
                record.set(recordGenerator.next());
                context.write(NullWritable.get(), record);
            }
        } catch (TerminateException te) {
            earlyTermination = Boolean.TRUE;
        }
    }
}
