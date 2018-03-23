package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;

public class LongField extends FieldBase<Long> {
    private Range<Long> range = new Range(0, Long.MAX_VALUE);
    private Pool<Long> pool;

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
        Long rtn = Math.abs(range.getMax()) - Math.abs(range.getMin());
        return rtn;
    }

    @Override
    public Long getNext() {
        double multiplierD = randomizer.nextDouble();
        return (Long)range.getMin() + Math.round((Long)getDiff() * multiplierD);
    }
}
