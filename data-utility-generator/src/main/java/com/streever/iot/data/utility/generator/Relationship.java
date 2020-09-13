package com.streever.iot.data.utility.generator;

public class Relationship {
    private Cardinality cardinality = new Cardinality(); // Will default to a 1-1 relationship.
    private Schema record;

    public Cardinality getCardinality() {
        return cardinality;
    }

    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    public Schema getRecord() {
        return record;
    }

    public void setRecord(Schema record) {
        this.record = record;
    }
}
