package com.streever.iot.data.utility.generator.fields;

public class ArrayLongField extends ArrayField<Long> {

    protected Long getDiff() {
        Long rtn = Math.abs(getRange().getMax()) - Math.abs(getRange().getMin());
        return rtn;
    }

    @Override
    protected Long get() {
        double multiplierD = randomizer.nextDouble();
        return (Long)getRange().getMin() + Math.round((Long)getDiff() * multiplierD);
    }
}
