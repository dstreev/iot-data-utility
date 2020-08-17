package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streever.iot.data.utility.generator.output.Output;

@JsonIgnoreProperties({"parent"})
public class Child {
    // Use this to build the output spec for the child. IE: output filename
    private String name;
    // The parent record.  Needed to pull down keys to establish link in cardinality.
    private Record parent;
    // Output will inherit parent properties unless otherwise specified.
    private Output output;
    private Cardinality cardinality;
    private int cardinalityRange = 1;
    private Record record;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Record getParent() {
        return parent;
    }

    public void setParent(Record parent) {
        this.parent = parent;
        for (Child child: record.getChildren()) {
            child.setParent(this.record);
        }
    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    public Cardinality getCardinality() {
        return cardinality;
    }

    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    public int getCardinalityRange() {
        return cardinalityRange;
    }

    public void setCardinalityRange(int cardinalityRange) {
        this.cardinalityRange = cardinalityRange;
    }

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }
}
