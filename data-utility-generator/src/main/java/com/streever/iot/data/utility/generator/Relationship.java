package com.streever.iot.data.utility.generator;

//@JsonIgnoreProperties({ "parent" })
public class Relationship {
//    Cardinality
//    Reference Key Field(s)
//    Record
    private Cardinality cardinality;
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
