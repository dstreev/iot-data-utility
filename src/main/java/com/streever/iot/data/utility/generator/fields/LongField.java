package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;

public class LongField extends FieldBase<Long> {
    private Range<Long> range = new Range(0, Long.MAX_VALUE);
    private Pool<Long> pool;

    @Override
    public FieldType getFieldType() {
        return FieldType.LONG;
    }

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

    @Override
    public boolean isNumber() {
        return true;
    }

    @Override
    public Long getNext() {
        double multiplierD = randomizer.nextDouble();
        Long rtn = (Long) range.getMin() + Math.round((Long) getDiff() * multiplierD);
        setLast(rtn);
        return rtn;
    }
}
