package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;

import java.text.DecimalFormat;

public class DoubleField extends FieldBase<Double> {

    private Range<Double> range = new Range(0, Integer.MAX_VALUE);
    private DecimalFormat format = new DecimalFormat("#.##");
    private Pool<Double> pool;

    public Range<Double> getRange() {
        return range;
    }

    public void setRange(Range<Double> range) {
        this.range = range;
    }

    public Pool<Double> getPool() {
        return pool;
    }

    public void setPool(Pool<Double> pool) {
        this.pool = pool;
    }

    public DecimalFormat getFormat() {
        return format;
    }

    public void setFormat(DecimalFormat format) {
        this.format = format;
    }

    protected Double getDiff() {
        Double rtn = range.getMax() - range.getMin();
        return rtn;
    }

    @Override
    public boolean isNumber() {
        return true;
    }

    @Override
    public Double getNext() {
        double multiplierD2 = randomizer.nextDouble();
        Double valD = (Double)range.getMin() + ((Double)getDiff() * multiplierD2);
        return Double.valueOf(format.format(valD));
    }
}
