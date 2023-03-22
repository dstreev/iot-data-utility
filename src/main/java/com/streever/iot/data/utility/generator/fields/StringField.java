package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.security.DigestException;

public class StringField extends FieldBase<String> {
    private Pool<String> pool;
    private String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private Range<Integer> range = new Range<Integer>(10, 10);
    private boolean hash = false;
    private String messageDigest = "MD5";
    @JsonIgnore
    private DigestUtils dg = null;

    @Override
    public FieldType getFieldType() {
        return FieldType.STRING;
    }

    public Pool<String> getPool() {
        return pool;
    }

    public void setPool(Pool<String> pool) {
        this.pool = pool;
    }

    protected Integer getDiff() {
        Integer rtn = null;
        if (range != null) {
            rtn = Math.abs(range.getMax() - range.getMin());
        } else {
            rtn = 0;
        }
        return rtn;
    }

    public Range<Integer> getRange() {
        return range;
    }

    public void setRange(Range<Integer> range) {
        this.range = range;
    }

    public boolean isHash() {
        return hash;
    }

    public void setHash(boolean hash) {
        this.hash = hash;
    }

    public DigestUtils getDg() {
        if (dg == null) {
            dg = new DigestUtils(getMessageDigest());
        }
        return dg;
    }

    public String getMessageDigest() {
        return messageDigest;
    }

    public void setMessageDigest(String messageDigest) {
        this.messageDigest = messageDigest;
    }

    public String getCharacters() {
        return characters;
    }

    public void setCharacters(String characters) {
        this.characters = characters;
    }

    // Get a random Length from the Range.
    protected Integer getCharacterLength() {
        if (range != null) {
            if (range.getMin() < range.getMax()) {
                Long value = new RandomDataGenerator().nextLong(range.getMin(), range.getMax());
                return value.intValue();
            } else {
                return range.getMin();
            }
        } else {
            return 10;
        }
    }

    protected void buildPool() {
        if (pool != null) {
            for (int i = 0; i < pool.getSize(); i++) {
                pool.getItems().add(RandomStringUtils.random(getCharacterLength(), getCharacters()));
            }
            pool.setInitialized(true);
        }
    }

    @Override
    public String getNext() {
        String rtn = null;
        if (pool == null) {
            rtn = RandomStringUtils.random(getCharacterLength(), getCharacters());
        } else {
            if (pool != null && pool.getInitialized() == Boolean.FALSE) {
                buildPool();
            }
            if (pool != null) {
                rtn = pool.getItem();
            }
        }
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
