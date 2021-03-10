package com.streever.iot.data.utility.generator;

import java.util.List;

public class Cardinality {

    /* The minimum number of records to create in an iteration */
    private int min = 1;
    /* The maximum number of records to create in an iteration */
    private int max = 1;

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
}
