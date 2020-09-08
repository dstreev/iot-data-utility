package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;

public class IntegerField extends FieldBase<Integer> {
    private Range<Integer> range = new Range(0, Integer.MAX_VALUE);
    private Pool<Integer> pool;

    public Range<Integer> getRange() {
        return range;
    }

    public void setRange(Range<Integer> range) {
        this.range = range;
    }

    public Pool<Integer> getPool() {
        return pool;
    }

    public void setPool(Pool<Integer> pool) {
        this.pool = pool;
    }

    protected Integer getDiff() {
        Integer rtn = range.getMax() - range.getMin();
        return rtn;
    }

    @Override
    public boolean isNumber() {
        return true;
    }

    @Override
    public Integer getNext() {
        Integer rtn = (Integer) range.getMin() + randomizer.nextInt((Integer) getDiff());
        setLast(rtn);
        return rtn;
    }
}
