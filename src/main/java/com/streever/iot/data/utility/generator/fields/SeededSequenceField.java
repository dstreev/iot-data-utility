package com.streever.iot.data.utility.generator.fields;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.concurrent.atomic.AtomicLong;

public class SeededSequenceField extends FieldBase<String> {
    private AtomicLong start = new AtomicLong(0l);
    private String seed = "ABCD";
    private Boolean hash = Boolean.FALSE;
    private String messageDigest = null;
    private DigestUtils dg = null;

    @Override
    public FieldType getFieldType() {
        return FieldType.STRING;
    }

    public AtomicLong getStart() {
        return start;
    }

    public void setStart(AtomicLong start) {
        this.start = start;
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
        Long nextLong = (Long)start.incrementAndGet();
        String rtn = seed + Long.toString(nextLong);
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
        setLast(rtn);
        return rtn;
    }
}
