package com.streever.iot.data.utility.generator.fields.support;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;

import static com.streever.iot.data.utility.generator.fields.support.TimeInterval.DAY;
import static com.streever.iot.data.utility.generator.fields.support.TimeInterval.MINUTE;

public class LateArriving {

    private static Random random = new Random(new Date().getTime());

    /*
    A factor of the Late Arriving Interval Type. IE: If Late Arriving IntervalType is DAY and LapsePeriod is 10, then
    potential late arriving values can go back 10 days for 'next' increment/current value.
     */
    private Integer lateArrivingLapsePeriod = 0;

    private TimeInterval lateArrivingIntervalType = DAY;

    /*
    Valid values between 0-1
     */
    private Float lateArrivingPercentage = 0.0f;


    public Integer getLateArrivingLapsePeriod() {
        return lateArrivingLapsePeriod;
    }

    public void setLateArrivingLapsePeriod(Integer lateArrivingLapsePeriod) {
        this.lateArrivingLapsePeriod = lateArrivingLapsePeriod;
    }

    public TimeInterval getLateArrivingIntervalType() {
        return lateArrivingIntervalType;
    }

    public void setLateArrivingIntervalType(TimeInterval lateArrivingIntervalType) {
        this.lateArrivingIntervalType = lateArrivingIntervalType;
    }

    public Float getLateArrivingPercentage() {
        return lateArrivingPercentage;
    }

    public void setLateArrivingPercentage(Float lateArrivingPercentage) {
        this.lateArrivingPercentage = lateArrivingPercentage;
    }

    public Date getValue(Date checkDate) {
        Float check;
        check = random.nextFloat();
        if (check <= lateArrivingPercentage) {
            // Now create an last Range.
            Float lapseBasis = random.nextFloat();
            Integer differential = Math.round(lateArrivingLapsePeriod * lapseBasis) * -1;
            Calendar working = Calendar.getInstance();
            working.setTime(checkDate);
            switch (lateArrivingIntervalType) {
                case MINUTE:
                    working.add(Calendar.MINUTE, differential);
                    break;
                case HOUR:
                    working.add(Calendar.HOUR, differential);
                    // Randomize the Minute.
                    working.set(Calendar.MINUTE, random.nextInt(60));
                    break;
                case DAY:
                    working.add(Calendar.DAY_OF_YEAR, differential);
                    // Randomize the HOUR.
                    working.set(Calendar.HOUR, random.nextInt(24));
                    // Randomize the Minute.
                    working.set(Calendar.MINUTE, random.nextInt(60));
                    break;
                case MONTH:
                    working.add(Calendar.MONTH, differential);
                    // Randomize the DAY.
                    working.set(Calendar.DAY_OF_MONTH, random.nextInt(30));
                    // Randomize the HOUR.
                    working.set(Calendar.HOUR, random.nextInt(24));
                    // Randomize the Minute.
                    working.set(Calendar.MINUTE, random.nextInt(60));
                    break;
                case YEAR:
                    working.add(Calendar.YEAR, differential);
                    // Randomize the MONTH.
                    working.set(Calendar.MONTH, random.nextInt(12));
                    // Randomize the DAY.
                    working.set(Calendar.DAY_OF_MONTH, random.nextInt(30));
                    // Randomize the HOUR.
                    working.set(Calendar.HOUR, random.nextInt(24));
                    // Randomize the Minute.
                    working.set(Calendar.MINUTE, random.nextInt(60));
                    break;
            }
            return working.getTime();
        } else {
            return checkDate;
        }
    }

    @Override
    public String toString() {
        return "LateArriving{" +
                "lateArrivingLapsePeriod=" + lateArrivingLapsePeriod +
                ", lateArrivingIntervalType=" + lateArrivingIntervalType +
                ", lateArrivingPercentage=" + lateArrivingPercentage +
                '}';
    }
}
