package com.streever.iot.data.utility.generator.fields;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streever.iot.data.utility.generator.fields.support.LateArriving;
import com.streever.iot.data.utility.generator.fields.support.Range;
import com.streever.iot.data.utility.generator.fields.support.StartStopState;
import com.streever.iot.data.utility.generator.fields.support.TimeInterval;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@JsonIgnoreProperties({"df", "startStop", "lastIssued"})
public class DateField extends FieldBase<String> implements ControlField {
    public enum As {
        STRING, LONG;
    }

    private Range<Timestamp> range;
    private Long diff = 1000l;
    private Long startStopSpan = 100000l;
    private String format = "yyyy-MM-dd HH:mm:ss";
    private DateFormat df = new SimpleDateFormat(format);
    private Boolean increment = Boolean.FALSE;
    private Boolean current = Boolean.TRUE;

    private LateArriving lateArriving = null;

    //private Boolean startStop = Boolean.FALSE;
    private Long lastIssued;
    private As as = As.STRING;

    public As getAs() {
        return as;
    }

    public void setAs(As as) {
        this.as = as;
    }

    // Used to control the termination of creating records when this is the control field.
    private Long lastValue;
    private Boolean controlField = Boolean.FALSE;

    public Range<Timestamp> getRange() {
        return range;
    }

    public void setRange(Range<Timestamp> range) {
        this.range = range;
        if (range != null) {
            if (!increment) {
                this.diff = range.getMax().getTime() - range.getMin().getTime();
            }
            // When Range is set, don't need current.
            this.current = Boolean.FALSE;
        } else {
            this.current = Boolean.TRUE;
        }
    }

    public void setControlField(Boolean controlField) {
        this.controlField = controlField;
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

    public LateArriving getLateArriving() {
        return lateArriving;
    }

    public void setLateArriving(LateArriving lateArriving) {
        this.lateArriving = lateArriving;
    }

    @Override
    public String getNext() {
        Date rtn = null;
        if (current) {
            Date now = new Date();
            if (lateArriving != null) {
                rtn = lateArriving.getValue(now);
            } else {
                lastValue = now.getTime();
                rtn = now;
            }
        } else if (increment) {
            if (range != null && lastIssued == null) {
                lastIssued = range.getMin().getTime();
            } else {
                double multiplierD = randomizer.nextDouble();
                long incrementL = Math.round((Long) diff * multiplierD);
//                System.out.println("Increment: " + incrementL + "Multiplier: " + multiplierD);
                lastIssued = lastIssued + incrementL;
            }
            lastValue = lastIssued;
            if (lateArriving != null) {
                rtn = lateArriving.getValue(new Date(lastIssued));
            } else {
                rtn = new Date(lastIssued);
            }
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
            lastValue = dateValue;
            rtn = new Date(dateValue);
        }
        if (getAs() == As.STRING) {
            return df.format(rtn);
        } else {
            return Long.toString(rtn.getTime());
        }
    }

    @Override
    public boolean terminate() {
        // If the last value issued exceeds the max range value, then terminate.
        if (getRange() != null && getRange().getMax() != null && this.isControlField()) {
            if (getRange().getMax().getTime() <= lastValue)
                return true;
        }
        return false;
    }

    @Override
    public boolean isControlField() {
        return controlField;
    }
}
