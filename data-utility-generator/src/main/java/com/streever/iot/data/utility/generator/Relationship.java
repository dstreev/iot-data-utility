package com.streever.iot.data.utility.generator;

public class Relationship {
    private Cardinality cardinality = new Cardinality(); // Will default to a 1-1 relationship.
    private Record record;

    public Cardinality getCardinality() {
        return cardinality;
    }

    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }
}
