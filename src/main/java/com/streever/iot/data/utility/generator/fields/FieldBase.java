package com.streever.iot.data.utility.generator.fields;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.streever.iot.data.utility.generator.Relationship;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

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
@JsonIgnoreProperties({ "randomizer", "order", "startStopState", "key", "last", "ext", "maintainState" })
public abstract class FieldBase<T> implements Comparable<FieldBase> {
//    private Integer order;
    private String name;
    private boolean maintainState = false;
    protected Map<String, Object> stateValues = new TreeMap<String, Object>();
    private boolean key = false;
    private Integer repeat = 1;
    private String desc;
    private boolean number = false;
    private Boolean random = Boolean.TRUE;
    protected Random randomizer = new Random(new Date().getTime());
//    private StartStopState startStopState = StartStopState.NA;

    private Object last;

    public void setLast(Object last) {
        this.last = last;
    }

    public Object getLast() {
        return last;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isMaintainState() {
        return maintainState;
    }

    public void setMaintainState(boolean maintainState) {
        this.maintainState = maintainState;
    }

    public String[] getStates() {
        return null;
    }

    public Object getNextStateValue(String state) {
        return null;
    }

    public boolean isKey() {
        return key;
    }

    public void setKey(boolean key) {
        this.key = key;
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

    public boolean isNumber() {
        return number;
    }

    public boolean validate() {
        return Boolean.TRUE;
    }

    public abstract T getNext() throws TerminateException;

    public FieldProperties getFieldProperties() {
        FieldProperties fp = new FieldProperties(this);
        return fp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldBase<?> fieldBase = (FieldBase<?>) o;

        return name != null ? name.equals(fieldBase.name) : fieldBase.name == null;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public int compareTo(FieldBase o) {
        return this.getName().compareTo(o.getName());
    }

    @Override
    public String toString() {
        return "FieldBase{" +
                "name='" + name + '\'' +
                ", maintainState=" + maintainState +
                ", key=" + key +
                ", repeat=" + repeat +
                ", desc='" + desc + '\'' +
                ", random=" + random +
//                ", randomizer=" + randomizer +
                '}';
    }
}
