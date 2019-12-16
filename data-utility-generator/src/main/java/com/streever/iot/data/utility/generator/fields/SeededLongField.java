package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;
import org.apache.commons.codec.digest.DigestUtils;

public class SeededLongField extends FieldBase<String> {
    private Range<Long> range = new Range(0, Long.MAX_VALUE);
    private Pool<Long> pool;
    private String seed = "ABCD";
    private Boolean hash = Boolean.FALSE;
    private String messageDigest = null;
    private DigestUtils dg = null;

    public Range<Long> getRange() {
        return range;
    }

    public void setRange(Range<Long> range) {
        this.range = range;
    }

    public Pool<Long> getPool() {
        return pool;
    }

    public void setPool(Pool<Long> pool) {
        this.pool = pool;
    }

    protected Long getDiff() {
        Long rtn = range.getMax() - range.getMin();
        return rtn;
    }

    public String getSeed() {
        return seed;
    }

    public void setSeed(String seed) {
        this.seed = seed;
    }

    public Boolean getHash() {
        return hash;
    }

    public void setHash(Boolean hash) {
        this.hash = hash;
    }

    @Override
    public String getNext() {
        double multiplierD = randomizer.nextDouble();
        Long value = (Long)range.getMin() + Math.round((Long)getDiff() * multiplierD);
        String rtn = seed + value;

        if (hash) {
            if (dg == null) {
                if (messageDigest != null) {
                    try {
                        dg = new DigestUtils(messageDigest);
                    } catch (IllegalArgumentException iae) {
                        System.err.println("Issue with MessageDigest: " + iae.getMessage());
                        dg = new DigestUtils("MD5");
                    }
                } else {
                    dg = new DigestUtils("MD5");
                }
            }
            rtn = dg.digestAsHex(rtn).toUpperCase();
        }

        return rtn;
    }
}
