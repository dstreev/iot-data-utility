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

package com.streever.iot.data.utility.generator.fields;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;

@JsonIgnoreProperties({"minIp", "maxIp", "ipDiff"})
public class IPV4AddressField extends FieldBase<String> {

    private Range<String> range = new Range("10.0.0.1", "10.0.0.255");
    private Pool<String> pool;
    private Long minIp = ipToLong(range.getMin());
    private Long maxIp = ipToLong(range.getMax());
    private Long ipDiff = maxIp - minIp;

    public Range<String> getRange() {
        return range;
    }

    public void setRange(Range<String> range) {
        this.range = range;
        minIp = ipToLong(range.getMin());
        maxIp = ipToLong(range.getMax());
        ipDiff = maxIp - minIp;
    }

    public Pool<String> getPool() {
        return pool;
    }

    public void setPool(Pool<String> pool) {
        this.pool = pool;
    }

    private void buildPool() {
        if (pool.getInitialized() != Boolean.TRUE) {
            for (int i = 0; i < pool.getSize(); i++) {
                pool.getItems().add(newValue());
            }
            pool.setInitialized(Boolean.TRUE);
        }
    }

    protected String newValue() {
        double multiplierD = randomizer.nextDouble();
        long ipLong = (Long) minIp + Math.round((Long) ipDiff * multiplierD);
        return longToIp(ipLong);
    }

//    public String getPoolValue() {
//        int ran = (int) Math.round((pool.getItems().size() - 1) * randomizer.nextFloat());
//        return pool.getItems().get(ran);
//    }

    @Override
    public String getNext() {
        String rtn = null;
        if (pool == null) {
            rtn = newValue();
        } else {
            if (pool != null && pool.getInitialized() == Boolean.FALSE) {
                buildPool();
            }
            if (pool != null) {
                rtn = pool.getItem();
            }
        }
        return rtn;
    }

    public static long ipToLong(String ipAddress) {
        long result = 0;
        String[] atoms = ipAddress.split("\\.");

        for (int i = 3; i >= 0; i--) {
            result |= (Long.parseLong(atoms[3 - i]) << (i * 8));
        }

        return result & 0xFFFFFFFF;
    }

    public static String longToIp(long ip) {
        StringBuilder sb = new StringBuilder(15);

        for (int i = 0; i < 4; i++) {
            sb.insert(0, Long.toString(ip & 0xff));

            if (i < 3) {
                sb.insert(0, '.');
            }

            ip >>= 8;
        }

        return sb.toString();
    }


//    private Long max = 100000l;
//    private Long diff;
//    private int poolSize = 100;
//    private boolean hasPool = false;
//    Random random = null;

/*
    public IPV4AddressField(JsonNode node) {
        super(node);
        if (node.has("min")) {
            min = node.get("min").longValue();

        if (node.has("minIp")) {
            min = ipToLong(node.get("minIp").asText());
        }
        if (node.has("max")) {
            max = node.get("max").longValue();
        }
        if (node.has("maxIp")) {
            max = ipToLong(node.get("maxIp").asText());
        }
        if (node.has("pool")) {
            // size is required
            poolSize = node.get("pool").get("size").asInt();
            fillPool();
        }
    }

    private Number getDiff() {
        if (diff == null) {
            diff = max - min;
        }
        return diff;
    }

    private void fillPool() {
        hasPool = true;
        pool = new String[poolSize];
        for (int i=0;i < poolSize;i++) {
            pool[i] = newValue();
        }
    }

    protected String newValue() {
        if (random == null)
            random = new Range();
        double multiplierD = random.nextDouble();
        long ipLong =  (Long)min + Math.round((Long)getDiff() * multiplierD);
        return longToIp(ipLong);
    }

    public String getPoolValue() {
        if (random == null)
            random = new Range();
        int ran = (int)Math.round((poolSize-1) * random.nextFloat());
        return pool[ran];
    }

    public String getValue() {
        if (hasPool) {
            return getPoolValue();
        } else {
            return newValue();
        }
    }

    public static long ipToLong(String ipAddress) {
        long result = 0;
        String[] atoms = ipAddress.split("\\.");

        for (int i = 3; i >= 0; i--) {
            result |= (Long.parseLong(atoms[3 - i]) << (i * 8));
        }

        return result & 0xFFFFFFFF;
    }

    public static String longToIp(long ip) {
        StringBuilder sb = new StringBuilder(15);

        for (int i = 0; i < 4; i++) {
            sb.insert(0, Long.toString(ip & 0xff));

            if (i < 3) {
                sb.insert(0, '.');
            }

            ip >>= 8;
        }

        return sb.toString();
    }
*/
}
