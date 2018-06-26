package com.streever.iot.data.utility.generator.fields;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streever.iot.data.utility.generator.fields.support.Range;
import com.streever.iot.data.utility.generator.fields.support.StartStopState;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@JsonIgnoreProperties({"df", "startStop", "lastIssued"})
public class DateField extends FieldBase<String> {
    private Range<Timestamp> range;
    private Long diff = 1000l;
    private Long startStopSpan = 100000l;
    private String format = "yyyy-MM-dd HH:mm:ss";
    private DateFormat df = new SimpleDateFormat(format);
    private Boolean increment = Boolean.FALSE;
    private Boolean current = Boolean.TRUE;
    //private Boolean startStop = Boolean.FALSE;
    private Long lastIssued;

    public Range<Timestamp> getRange() {
        return range;
    }

    public void setRange(Range<Timestamp> range) {
        this.range = range;
        if (!increment) {
            this.diff = range.getMax().getTime() - range.getMin().getTime();
        }
        // When Range is set, don't need current.
        this.current = Boolean.FALSE;
    }

    public Boolean getCurrent() {
        return current;
    }

    public void setCurrent(Boolean current) {
        this.current = current;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
        df = new SimpleDateFormat(this.format);
    }

    public Long getDiff() {
        return diff;
    }

    public void setDiff(Long diff) {
        this.diff = diff;
    }

    public Boolean getIncrement() {
        return increment;
    }

    public void setIncrement(Boolean increment) {
        this.increment = increment;
    }

    @Override
    public String getNext() {
        if (current) {
            return df.format(new Date());
        } else if (increment) {
            if (range != null && lastIssued == null) {
                lastIssued = range.getMin().getTime();
            } else {
                double multiplierD = randomizer.nextDouble();
                long incrementL = Math.round((Long) diff * multiplierD);
//                System.out.println("Increment: " + incrementL + "Multiplier: " + multiplierD);
                lastIssued = lastIssued + incrementL;
            }
            return df.format(new Date(lastIssued));
        } else {
            Long dateValue = null;
            if (getStartStopState().equals(StartStopState.START)) {
                double multiplierD = randomizer.nextDouble();
                dateValue = range.getMin().getTime() + Math.round((Long) diff * multiplierD);
                lastIssued = dateValue;
            } else if (getStartStopState().equals(StartStopState.STOP)) {
                double multiplierD = randomizer.nextDouble();
                dateValue = lastIssued + Math.round((Long) startStopSpan * multiplierD);
            } else {
                double multiplierD = randomizer.nextDouble();
                dateValue = range.getMin().getTime() + Math.round((Long) diff * multiplierD);
                lastIssued = null;
            }
            return df.format(new Date(dateValue));
        }
    }
}
