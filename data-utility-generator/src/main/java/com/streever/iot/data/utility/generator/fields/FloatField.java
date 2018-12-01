package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;

import java.text.DecimalFormat;

public class FloatField extends FieldBase<Float> {

    private Range<Float> range = new Range(0, Integer.MAX_VALUE);
    private DecimalFormat format = new DecimalFormat("#.##");
    private Pool<Float> pool;

    public Range<Float> getRange() {
        return range;
    }

    public void setRange(Range<Float> range) {
        this.range = range;
    }

    public Pool<Float> getPool() {
        return pool;
    }

    public void setPool(Pool<Float> pool) {
        this.pool = pool;
    }

    public DecimalFormat getFormat() {
        return format;
    }

    public void setFormat(DecimalFormat format) {
        this.format = format;
    }

    protected Float getDiff() {
//        Float rtn = Math.abs(range.getMax()) - Math.abs(range.getMin());
        Float rtn = range.getMax() - range.getMin();
        return rtn;
    }

    @Override
    public Float getNext() {
        float multiplierF = randomizer.nextFloat();
        Float valF = (Float) range.getMin() + ((Float) getDiff() * multiplierF);
        return Float.valueOf(format.format(valF));
    }
}
