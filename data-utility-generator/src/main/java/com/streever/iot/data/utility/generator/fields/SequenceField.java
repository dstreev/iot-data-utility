package com.streever.iot.data.utility.generator.fields;

import java.util.concurrent.atomic.AtomicLong;

public class SequenceField extends FieldBase<Long> {
    private AtomicLong start = new AtomicLong(0l);

    public AtomicLong getStart() {
        return start;
    }

    public void setStart(AtomicLong start) {
        this.start = start;
    }

    @Override
    public Long getNext() {
        return (Long)start.incrementAndGet();

    }
}
