package com.streever.iot.data.utility.generator.fields;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.streever.iot.data.utility.generator.fields.support.StartStopState;

import java.util.Date;
import java.util.Random;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = StringField.class, name = "string"),
        @JsonSubTypes.Type(value = IPV4AddressField.class, name = "ipv4"),
        @JsonSubTypes.Type(value = GeoLocationField.class, name = "location"),
        @JsonSubTypes.Type(value = FixedField.class, name = "fixed"),
        @JsonSubTypes.Type(value = SequenceField.class, name = "sequence"),
        @JsonSubTypes.Type(value = DateField.class, name = "date"),
        @JsonSubTypes.Type(value = IntegerField.class, name = "int"),
        @JsonSubTypes.Type(value = LongField.class, name = "long"),
        @JsonSubTypes.Type(value = FloatField.class, name = "float"),
        @JsonSubTypes.Type(value = DoubleField.class, name = "double"),
        @JsonSubTypes.Type(value = ArrayLongField.class, name = "array.long"),
        @JsonSubTypes.Type(value = ArrayStringField.class, name = "array.string")
})
@JsonIgnoreProperties({ "randomizer", "order", "startStopState" })
public abstract class FieldBase<T> implements Comparable<FieldBase> {
    private Integer order;
    private String name;
    private Boolean random = Boolean.TRUE;
    protected Random randomizer = new Random(new Date().getTime());
    private StartStopState startStopState = StartStopState.NA;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public StartStopState getStartStopState() {
        return startStopState;
    }

    public void setStartStopState(StartStopState startStopState) {
        this.startStopState = startStopState;
    }

    public Boolean getRandom() {
        return random;
    }

    public void setRandom(Boolean random) {
        this.random = random;
    }

    public Integer getOrder() {
        return order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }

    public abstract T getNext();

    @Override
    public int compareTo(FieldBase o) {
        if (this.order == o.order)
            return 0;
        if (this.order > o.order)
            return 1;
        else
            return -1;
    }
}
