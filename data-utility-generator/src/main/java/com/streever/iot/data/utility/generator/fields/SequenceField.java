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

package com.streever.iot.data.utility.generator.fields;

import com.fasterxml.jackson.databind.JsonNode;

import java.text.DecimalFormat;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SequenceField extends AbstractFieldType implements FieldType<Number> {

    private enum TYPE {
        INT,LONG;
    }

    private Number start = new AtomicInteger(0);
    private DecimalFormat decimalFormat = new DecimalFormat("#.##");
    private TYPE type = TYPE.INT;

    public SequenceField(JsonNode node) {
        super(node);
        if (node.has("type")) {
            String typeStr = node.get("type").asText();
            type = TYPE.valueOf(typeStr.toUpperCase());
        }
        if (node.has("start")) {
            switch (type) {
                case INT:
                    start = new AtomicInteger(node.get("start").intValue());
                    break;
                case LONG:
                    start = new AtomicLong(node.get("start").longValue());
                    break;
            }
        }
    }

    protected Number newValue() {
        switch (type) {
            case INT:
                return ((AtomicInteger)start).incrementAndGet();
            case LONG:
                return ((AtomicLong)start).incrementAndGet();
            default:
                return ((AtomicInteger)start).incrementAndGet();
        }
    }

    public Number getValue() {
        return newValue();
    }

    @Override
    public Number getPoolValue() {
        return newValue();
    }

}
