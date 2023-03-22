package com.streever.iot.data.utility.generator;

import org.codehaus.jackson.annotate.JsonIgnore;

public class Relationship {
    @JsonIgnore
    private Domain domain;
    private String from;
    private String to;

    @JsonIgnore
    private Schema fromSchema;
    @JsonIgnore
    private Schema toSchema;
    private Cardinality cardinality = null; //new Cardinality(); // Will default to a 1-1 relationship.
//    private Schema record;
//    private RelationshipType toType;

    public Domain getDomain() {
        return domain;
    }

    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

//    public RelationshipType getToType() {
//        return toType;
//    }
//
//    public void setToType(RelationshipType toType) {
//        this.toType = toType;
//    }

    public Schema getFromSchema() {
        return fromSchema;
    }

    public void setFromSchema(Schema fromSchema) {
        this.fromSchema = fromSchema;
    }

    public Schema getToSchema() {
        return toSchema;
    }

    public void setToSchema(Schema toSchema) {
        this.toSchema = toSchema;
    }

    public Cardinality getCardinality() {
        return cardinality;
    }

    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
//        if (record != null)
//            record.setCardinality(this.cardinality);
    }

//    public Schema getRecord() {
//        return record;
//    }
//
//    public void setRecord(Schema record) {
//        this.record = record;
//        if (cardinality != null)
//            this.record.setCardinality(cardinality);
//    }
}
