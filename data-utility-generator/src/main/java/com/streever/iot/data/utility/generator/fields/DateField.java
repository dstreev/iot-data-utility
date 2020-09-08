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
public class DateField extends FieldBase<Object> implements ControlField {
    private Range<Timestamp> range;
    private Long diff = 1000l;
    private Long startStopSpan = 100000l;
    private String format = "yyyy-MM-dd HH:mm:ss";
    private DateFormat df = new SimpleDateFormat(format);
    private Boolean increment = Boolean.FALSE;
    private Boolean current = Boolean.TRUE;

    public String[] getStates() {
        String[] states = {"start", "stop"};
        return states;
    }

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

    public Long getStartStopSpan() {
        return startStopSpan;
    }

    public void setStartStopSpan(Long startStopSpan) {
        this.startStopSpan = startStopSpan;
    }

    @Override
    public boolean isNumber() {
        if (getAs() == As.STRING) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public Object getNext() {
        Date working = null;
        if (!isMaintainState()) {
            if (current) {
                Date now = new Date();
                if (lateArriving != null) {
                    working = lateArriving.getValue(now);
                } else {
                    lastValue = now.getTime();
                    working = now;
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
                    working = lateArriving.getValue(new Date(lastIssued));
                } else {
                    working = new Date(lastIssued);
                }
            } else {
                System.out.println("What?");
            }
        } else {
            stateValues.clear();
            Long startValue, stopValue = null;
            double multiplierD = randomizer.nextDouble();
            startValue = range.getMin().getTime() + Math.round((Long) diff * multiplierD);
            stateValues.put("start", startValue);
            multiplierD = randomizer.nextDouble();
            stopValue = startValue + Math.round((Long) startStopSpan * multiplierD);
            stateValues.put("stop", stopValue);
            working = new Date(startValue);
        }
        Object rtn = null;
        if (getAs() == As.STRING) {
            rtn = df.format(working);
        } else {
            rtn = working.getTime() / 1000;
        }
        setLast(rtn);
        return rtn;
    }

    public Object getNextStateValue(String state) {
        if (stateValues.containsKey(state)) {
            if (getAs() == As.STRING) {
                return df.format(new Date((long) stateValues.get(state)));
            } else {
                return (long) stateValues.get(state) / 1000;
            }
        } else {
            // TODO: Do more messaging here.
            throw new RuntimeException("Bad state declaration: " + state);
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
