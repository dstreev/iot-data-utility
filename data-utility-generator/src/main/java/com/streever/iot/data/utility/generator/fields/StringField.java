package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;

public class StringField extends FieldBase<String> {
    private Pool<String> pool;
    private String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private Range<Integer> range = new Range<Integer>(10, 10);

    public Pool<String> getPool() {
        return pool;
    }

    public void setPool(Pool<String> pool) {
        this.pool = pool;
    }

    public Range<Integer> getRange() {
        return range;
    }

    public void setRange(Range<Integer> range) {
        this.range = range;
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
            if (range.getMin() > range.getMax()) {
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
        return rtn;
    }
}
