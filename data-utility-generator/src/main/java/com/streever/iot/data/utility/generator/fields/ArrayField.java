package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.ArrayList;

public abstract class ArrayField<T> extends FieldBase<ArrayList<T>> {
    private Pool<T> pool;
    private String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private Range<Long> range = new Range<Long>(10l, 10l);

    public Pool<T> getPool() {
        return pool;
    }

    public void setPool(Pool<T> pool) {
        this.pool = pool;
    }

    public Range<Long> getRange() {
        return range;
    }

    public void setRange(Range<Long> range) {
        this.range = range;
    }

    public String getCharacters() {
        return characters;
    }

    public void setCharacters(String characters) {
        this.characters = characters;
    }

    // Get a random Length from the Range.
    protected Long getCharacterLength() {
        if (range != null) {
            if (range.getMin() < range.getMax()) {
                Long value = new RandomDataGenerator().nextLong(range.getMin(), range.getMax());
                return value.longValue();
            } else {
                return range.getMin();
            }
        } else {
            return 10l;
        }
    }

    protected void buildPool() {
        if (pool != null) {
            for (int i = 0; i < pool.getSize(); i++) {
                pool.getItems().add(get());
            }
            pool.setInitialized(true);
        }
    }

    protected abstract T get();

    @Override
    public ArrayList<T> getNext() {
        ArrayList<T> rtn = new ArrayList<T>();
        for (int i=0;i<5;i++) {
            if (pool == null) {
                rtn.add(get());
            } else {
                if (pool != null && pool.getInitialized() == Boolean.FALSE) {
                    buildPool();
                }
                if (pool != null) {
                    rtn.add(pool.getItem());
                }
            }
        }
        return rtn;
    }
}
