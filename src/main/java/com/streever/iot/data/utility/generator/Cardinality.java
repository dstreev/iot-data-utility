package com.streever.iot.data.utility.generator;

public class Cardinality {

    /* The minimum number of records to create in an iteration */
    private int min = 1;
    /* The maximum number of records to create in an iteration */
    private int max = 1;
    /*
    For values greater than 1, the title will append a "_x" to the title as the entity name.
     */
    private Integer repeat = 1;

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public Integer getRepeat() {
        return repeat;
    }

    public void setRepeat(Integer repeat) {
        this.repeat = repeat;
        // Limit this.
        if (this.repeat > 1000) {
            this.repeat = 1000;
        }
    }

    public int getRange() {
        return getMax() - getMin();
    }

}
