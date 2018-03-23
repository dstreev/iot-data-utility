package com.streever.iot.data.utility.generator.fields.support;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties({ "initialized" })
public class Pool<T> {
    private Long size;
    private Boolean initialized = Boolean.FALSE;
    private List<T> items = new ArrayList<T>();

    public Boolean getInitialized() {
        return initialized;
    }

    public void setInitialized(Boolean initialized) {
        this.initialized = initialized;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
        this.initialized = Boolean.TRUE;
    }

    public T getItem() {
        T rtn;
        double multiplier = Math.random();
        rtn = items.get((int) (getItems().size() * multiplier));
        return rtn;
    }
}
