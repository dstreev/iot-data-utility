package com.streever.iot.data.utility.generator.fields;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.streever.iot.data.utility.generator.Relationship;
import com.streever.iot.data.utility.generator.fields.support.StartStopState;

import java.util.Date;
import java.util.Random;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = StringField.class, name = "string"),
        @JsonSubTypes.Type(value = RegExField.class, name = "regex"),
        @JsonSubTypes.Type(value = IPV4AddressField.class, name = "ipv4"),
        @JsonSubTypes.Type(value = GeoLocationField.class, name = "location"),
        @JsonSubTypes.Type(value = FixedField.class, name = "fixed"),
        @JsonSubTypes.Type(value = SequenceField.class, name = "sequence"),
        @JsonSubTypes.Type(value = SeededSequenceField.class, name = "seeded.sequence"),
        @JsonSubTypes.Type(value = SeededLongField.class, name = "seeded.long"),
        @JsonSubTypes.Type(value = DateField.class, name = "date"),
        @JsonSubTypes.Type(value = IntegerField.class, name = "int"),
        @JsonSubTypes.Type(value = LongField.class, name = "long"),
        @JsonSubTypes.Type(value = FloatField.class, name = "float"),
        @JsonSubTypes.Type(value = DoubleField.class, name = "double"),
        @JsonSubTypes.Type(value = UUIDField.class, name = "uuid"),
        @JsonSubTypes.Type(value = ReferenceStringField.class, name = "reference.string"),
        @JsonSubTypes.Type(value = ArrayLongField.class, name = "array.long"),
        @JsonSubTypes.Type(value = ArrayStringField.class, name = "array.string"),
        @JsonSubTypes.Type(value = Relationship.class, name = "relationship")
})
@JsonIgnoreProperties({ "randomizer", "order", "startStopState", "key", "last" })
public abstract class FieldBase<T> implements Comparable<FieldBase> {
    private Integer order;
    private String name;
    private boolean key = false;
    private Integer repeat = 1;
    private String desc;
//    private boolean number = false;
    private Boolean random = Boolean.TRUE;
    protected Random randomizer = new Random(new Date().getTime());
    private StartStopState startStopState = StartStopState.NA;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isKey() {
        return key;
    }

    public void setKey(boolean key) {
        this.key = key;
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

    public Integer getRepeat() {
        return repeat;
    }

    public void setRepeat(Integer repeat) {
        if (repeat > 1000) {
            throw new RuntimeException("Repeat Value can't exceed 1000. FieldBase: " + this.name);
        }
        this.repeat = repeat;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Integer getOrder() {
        return order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }

    public boolean isNumber() {
        return false;
    }

    public abstract T getNext();

    /*
    Used as a light container for properties of the Field that are
    specific to the resulting value instance.
     */
    public FieldProperties getFieldProperties(String repeat) {
        FieldProperties fp = new FieldProperties();
        if (repeat != null) {
            fp.setName(this.getName() + "_" + repeat);
        } else {
            fp.setName(this.getName());
        }
        fp.setNumber(this.isNumber());
        return fp;
    }

    @Override
    public int compareTo(FieldBase o) {
        if (this.order == o.order)
            return 0;
        if (this.order > o.order)
            return 1;
        else
            return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldBase<?> fieldBase = (FieldBase<?>) o;

        return name.equals(fieldBase.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
