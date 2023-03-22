package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.text.DecimalFormat;

public class FloatField extends FieldBase<Float> {

    private Range<Float> range = new Range(0, Integer.MAX_VALUE);
    private String format = "#.##";
    @JsonIgnore
    private DecimalFormat decimalFormat = new DecimalFormat("#.##");
    private Pool<Float> pool;
    private Float last;

    @Override
    public FieldType getFieldType() {
        return FieldType.FLOAT;
    }

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

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
        decimalFormat = new DecimalFormat(this.format);
    }

    public DecimalFormat getDecimalFormat() {
        return decimalFormat;
    }

    public void setDecimalFormat(DecimalFormat decimalFormat) {
        this.decimalFormat = decimalFormat;
    }

    public Float getLast() {
        return last;
    }

    public void setLast(Float last) {
        this.last = last;
    }

    protected Float getDiff() {
//        Float rtn = Math.abs(range.getMax()) - Math.abs(range.getMin());
        Float rtn = range.getMax() - range.getMin();
        return rtn;
    }

    @Override
    public boolean isNumber() {
        return true;
    }

    @Override
    public Float getNext() {
        float multiplierF = randomizer.nextFloat();
        Float valF = (Float) range.getMin() + ((Float) getDiff() * multiplierF);
        setLast(Float.valueOf(decimalFormat.format(valF)));
        return getLast();
    }
}
